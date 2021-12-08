package workerpool

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

/*
** What to we want:
** - The ability for workerpool instances to connect as a quorum
** - Set a parallelisation limit for the entire quorum
** - Distribute workload between instances if queues are uneven
**
** Use cases:
** - Workload parallelisation with autoscaling to evaluate most effective parallelisation
** - Rate limiting to avoid hammering a downstream component. In case of Quorum rate limiting, allow horizontal
** scaling while preserving downstream component global rate limit
** - Workload distribution in case of heaving computation tasks
**
** Parallelisation limit
**
** Global parallelisation limit means each instances will have a new limit to QUORUM_MAX_WORKER divided by
** the number of instances, updated whenever an instances join or leave quorum
**
** Workload distribution
**
** Whenever workload queue is above a certain threshold (i.e 3 * MAX_WORKER),
** the instances with work to share will broadcast a todolist to the quorum by block of N (like MAX_WORKER/10).
** If another instance choose to accept the todolist, it will broadcast an acceptance message and wait to be
** aknowledge by the majority of instances in the quorum to start working.
** Each response will be sent to the instance who created the todolist.
**
** Message to exchange:
** - CONNECT : instances joining quorum (uuid, dsn)
** - DISCONNECT : instances left quorum (uuid)
** - TASK_AVAILABLE: a new block of task is available
** - TASK_ACCEPTANCE: an instance accept the task
** - TASK_ACCEPTANCE_ACK: instance aknowledge given instance doing the task
** - TASK_RESULT: task result message
 */
type ConnectMessage struct {
	Name string
	DSN  string
}

// Quorum configuration struct
type Quorum struct {
	Status           Status
	Name             string
	DSN              string
	Bind             string
	DSNs             []string
	MaxWorker        int
	SharingThreshold int

	instances map[string]*Instance
	m         sync.Mutex
}

// Instance describe other instances in the quorum
type Instance struct {
	DSN       string
	Name      string
	ReadConn  net.Conn
	WriteConn net.Conn
}

// WithQuorum instructs new workerpool to activate quorum mode and connects to given quorum instances
// if instanceName is empty, an UUID will be generated
func WithQuorum(instanceName string, bind string, public string, quorumDSNs []string, quorumMaxWorker int, taskSharingThreshold int) OptFunc {
	fn := func(wp *WorkerPool) {
		if public == "" {
			public = bind
		}
		q := &Quorum{
			Name:             instanceName,
			Bind:             bind,
			DSN:              public,
			DSNs:             quorumDSNs,
			MaxWorker:        quorumMaxWorker,
			SharingThreshold: taskSharingThreshold,
			instances:        make(map[string]*Instance),
		}

		wp.quorum = q
	}
	return fn
}

// InstanceCount returns the number of connected instances in quorum
func (q *Quorum) InstanceCount() int {
	q.m.Lock()
	defer q.m.Unlock()
	c := len(q.instances) + 1
	return c
}

// Start starts:
// - routine trying to connect to all other members of quorum
// - routine listening for new connection from other quorum instances
func (q *Quorum) Start() error {
	q.Status = Running

	go q.connectionRoutine()

	ln, err := net.Listen("tcp", q.Bind)
	if err != nil {
		return fmt.Errorf("cannot listen: %s", err)
	}

	go func() {
		for q.Status == Running {
			conn, err := ln.Accept()
			if err != nil {
				// handle error
			}
			if q.Status != Running {
				conn.Close()
				return
			}
			go q.instanceRoutine(conn)
		}
	}()

	return nil
}

// Stop all routines and close connections to other instances in quorum
func (q *Quorum) Stop() error {
	q.Status = Stopped
	for _, inst := range q.instances {
		inst.ReadConn.Close()
		inst.WriteConn.Close()
	}
	return nil
}

func (q *Quorum) connectionRoutine() {
	ok := false

	for _, dsn := range q.DSNs {
		i := &Instance{
			DSN: dsn,
		}
		q.addInstance(i)
	}

	var count int
	for {
		if ok {
			time.Sleep(100 * time.Millisecond)
		}
		count = 0

		for _, inst := range q.instances {
			if inst.WriteConn == nil {
				err := q.Connect(inst)
				if err != nil {
					q.debug("cannot connect to %s: %s", inst.DSN, err)
				}
				if inst.ReadConn != nil {
					q.debug("Connected to %+v", inst)
				}
			}

			if inst.WriteConn != nil && inst.ReadConn != nil {
				count++
			}
		}
		if count == len(q.instances) {
			ok = true
		} else {
			ok = false
		}
	}
}

func (q *Quorum) listenRoutine() {
}

// instanceRoutine accepts new conn and read CONNECT message from new instance
func (q *Quorum) instanceRoutine(conn net.Conn) {
	var inst *Instance

	dec := gob.NewDecoder(conn)

	cmsg := ConnectMessage{}
	err := dec.Decode(&cmsg)
	if err != nil {
		log.Errorf("Cannot decode connect message: %s", err)
	}
	q.debug("Received CONNECT: %+v", cmsg)

	found := false
	for i := range q.instances {
		if q.instances[i].DSN == cmsg.DSN {
			found = true
			inst = q.instances[i]
			inst.ReadConn = conn
			inst.Name = cmsg.Name
			if inst.WriteConn != nil {
				q.debug("Connected to %+v", inst)
			}
		}
	}
	if !found {
		inst = &Instance{
			Name:     cmsg.Name,
			ReadConn: conn,
			DSN:      cmsg.DSN,
		}
		q.addInstance(inst)
	}

	enc := gob.NewEncoder(conn)
	err = enc.Encode(&ConnectMessage{
		Name: q.Name,
		DSN:  q.DSN,
	})

	var msg interface{}
	for q.Status == Running {
		err := dec.Decode(&msg)
		if err != nil && err == io.EOF {
			q.debug("Instance %+v disconnected", inst)
			inst.ReadConn.Close()
			inst.WriteConn.Close()
			q.removeInstance(inst)
			return
		}
		if err != nil {
			q.debug("Decode error: %s", err)
		}
	}
}

func (q *Quorum) Connect(inst *Instance) error {
	var err error

	inst.WriteConn, err = net.Dial("tcp", inst.DSN)
	if err != nil {
		return err
	}

	cmsg := ConnectMessage{
		Name: q.Name,
		DSN:  q.DSN,
	}

	enc := gob.NewEncoder(inst.WriteConn)

	err = enc.Encode(cmsg)
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(inst.WriteConn)
	err = dec.Decode(&cmsg)
	if err != nil {
		return err
	}
	inst.Name = cmsg.Name
	//	q.debug("Connected to %+v", inst)
	return nil
}

func (q *Quorum) debug(format string, vars ...interface{}) {
	d := fmt.Sprintf(format, vars...)
	fmt.Printf("[DEBUG <%s>] %s\n", q.Name, d)
}

func (q *Quorum) removeInstance(inst *Instance) {
	q.m.Lock()
	defer q.m.Unlock()
	delete(q.instances, inst.DSN)
}

func (q *Quorum) addInstance(inst *Instance) {
	q.m.Lock()
	defer q.m.Unlock()
	q.instances[inst.DSN] = inst
}
