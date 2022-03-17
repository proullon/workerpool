package workerpool

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
** If tasks are not taken by any instance of quorum after AVAILABILITY_TIMEOUT, tasks will be fed back into local
** WorkerPool queue.
**
** Message to exchange:
** - CONNECT : instances joining quorum (uuid, dsn)
** - DISCONNECT : instances left quorum (uuid)
** - TASK_AVAILABLE: a new block of task is available
** - TASK_ACCEPTANCE: an instance accept the task
** - TASK_ACCEPTANCE_ACK: instance aknowledge given instance doing the task
** - TASK_ACCEPTANCE_REJECT: instance reject instance proposition (already ack another)
** - TASK_RESULT: task result message
** - VELOCITY_VALUES: share local velocity values
 */
type ConnectMessage struct {
	Name string
	DSN  string
}

type TaskAvailable struct {
	UUID    string
	Emitter string
	Emitted time.Time
	Tasks   []Payload
}

type TaskAcceptance struct {
	UUID     string
	Emitter  string
	Accepter string
	//Count    int
}

type TaskAcceptanceAck struct {
	UUID         string
	Emitter      string
	Accepter     string
	Acknowledger string
}

type TaskAcceptanceReject struct {
	UUID     string
	Emitter  string
	Accepter string
}

type TaskResult struct {
	UUID      string
	Emitter   string
	Worker    string
	Responses []Response
}

type VelocityValues struct {
	Emitter          string
	CurrentPercentil int
	CurrentVelocity  float64
	CurrentAverage   float64
	Velocity         map[int]Velocity
}

type Global struct {
	Cmd string
	TaskAvailable
	TaskAcceptance
	TaskAcceptanceAck
	TaskAcceptanceReject
	TaskResult
	VelocityValues
}

// Quorum configuration struct
type Quorum struct {
	Status           Status
	Name             string
	DSN              string
	Bind             string
	DSNs             []string
	MaxWorker        int
	LocalMaxWorker   int
	SharingThreshold int

	availableTasks      map[string]TaskAvailable
	availabilityTimeout time.Duration
	//pendingAckTasks     *list.List
	pendingAckTasks   map[string]TaskAvailable
	acceptedTasks     map[string]*TaskAcceptance
	awaitingResponses map[string]TaskAvailable
	executionTimeout  time.Duration
	wp                *WorkerPool

	listener  net.Listener
	instances map[string]*Instance
	m         sync.Mutex
}

func (q *Quorum) internalStatus() {
	//q.m.Lock()
	//defer q.m.Unlock()

	/*
		var c = 1
		for _, inst := range q.instances {
			if inst.WriteConn != nil && inst.ReadConn != nil {
				c++
			}
		}
			log.Infof("[STATUS <%s>] Quorum:%d, Available:%d, PendingAck:%d, Accepted:%d, AwaitingResults:%d", q.Name,
				c,
				len(q.availableTasks),
				len(q.pendingAckTasks),
				len(q.acceptedTasks),
				len(q.awaitingResponses),
			)
	*/
}

// Instance describe other instances in the quorum
type Instance struct {
	DSN       string
	Name      string
	ReadConn  net.Conn
	WriteConn net.Conn
	Encoder   *gob.Encoder

	Velocity map[int]Velocity
}

// WithQuorum instructs new workerpool to activate quorum mode and connects to given quorum instances
// if instanceName is empty, an UUID will be generated
func WithQuorum(instanceName string, bind string, public string, quorumDSNs []string, quorumMaxWorker int, taskSharingThreshold int) OptFunc {
	fn := func(wp *WorkerPool) {
		if public == "" {
			public = bind
		}
		q := &Quorum{
			Name:                instanceName,
			Bind:                bind,
			DSN:                 public,
			DSNs:                quorumDSNs,
			MaxWorker:           quorumMaxWorker,
			SharingThreshold:    taskSharingThreshold,
			instances:           make(map[string]*Instance),
			availableTasks:      make(map[string]TaskAvailable),
			availabilityTimeout: 300 * time.Millisecond,
			pendingAckTasks:     make(map[string]TaskAvailable),
			acceptedTasks:       make(map[string]*TaskAcceptance),
			awaitingResponses:   make(map[string]TaskAvailable),
			executionTimeout:    3 * time.Second,
		}

		wp.quorum = q
	}
	return fn
}

func (q *Quorum) GlobalVelocityValues() (map[int]Velocity, map[string]*Instance) {

	all := make(map[string]*Instance)

	local := q.wp.VelocityValues()
	i := &Instance{Name: "local", Velocity: local}
	all["local"] = i

	combined := q.wp.VelocityValues()

	q.m.Lock()
	for k, i := range q.instances {
		if i.Connected() == false {
			continue
		}
		all[k] = i
		for parallelisation, velocity := range i.Velocity {
			cv := combined[parallelisation]
			cv.Ops += velocity.Ops
			cv.Avg += velocity.Avg
			combined[parallelisation] = cv
		}
	}
	q.m.Unlock()

	for parallelisation, velocity := range combined {
		velocity.Avg /= float64(len(all))
		combined[parallelisation] = velocity
	}

	return combined, all
}

// InstanceCount returns the number of connected instances in quorum
func (q *Quorum) InstanceCount() int {
	q.m.Lock()
	defer q.m.Unlock()
	c := len(q.instances) + 1
	return c
}

func (q *Quorum) MaxInstanceWorker() int {
	q.m.Lock()
	defer q.m.Unlock()
	max := q.MaxWorker / (len(q.instances) + 1)
	if q.LocalMaxWorker < max {
		return q.LocalMaxWorker
	}
	return max
}

// Start starts:
// - routine trying to connect to all other members of quorum
// - routine listening for new connection from other quorum instances
func (q *Quorum) Start(localMax int, wp *WorkerPool) error {
	q.m.Lock()
	q.Status = Running
	q.LocalMaxWorker = localMax
	q.wp = wp
	q.m.Unlock()

	go q.connectionRoutine()
	go q.tasksRoutine()

	ln, err := net.Listen("tcp", q.Bind)
	if err != nil {
		return fmt.Errorf("cannot listen: %s", err)
	}
	q.m.Lock()
	q.listener = ln
	q.m.Unlock()

	go func() {
		for q.status() == Running {
			conn, err := ln.Accept()
			if err != nil && err == net.ErrClosed {
				return
			}
			if q.status() != Running {
				if conn != nil {
					conn.Close()
				}
				return
			}
			if err != nil {
				q.debug("cannot accept new conn: %s", err)
			}
			go q.instanceRoutine(conn)
		}
	}()

	return nil
}

// Stop all routines and close connections to other instances in quorum
func (q *Quorum) Stop() error {
	q.m.Lock()
	defer q.m.Unlock()
	q.Status = Stopped
	q.listener.Close()
	for _, inst := range q.instances {
		if inst.ReadConn != nil {
			inst.ReadConn.Close()
		}
		if inst.WriteConn != nil {
			inst.WriteConn.Close()
		}
	}
	return nil
}

func (q *Quorum) tasksRoutine() {
	for {
		// Check available tasks timeout
		q.cleanupTimedOutAvailableTasks()
		// Check pending acknowledgement tasks timeout
		q.cleanupTimedOutPendingTasks()
		// Check tasks awaiting responses
		q.cleanupTimedOutAwaitingTasks()

		// rest
		time.Sleep(10 * time.Millisecond)
	}
}

func (q *Quorum) cleanupTimedOutAvailableTasks() {
	q.m.Lock()

	//	log.Infof("%d availableTasks", len(q.availableTasks))

	for k, ta := range q.availableTasks {
		//log.Infof("Checking %+v", e.Value)
		if time.Since(ta.Emitted) > q.availabilityTimeout {
			q.m.Unlock()
			log.Infof("[INFO <%s>] Availability timeout. Feeding back %s (%s, %s) %d tasks", q.Name, ta.UUID, ta.Emitter, ta.Emitted, len(ta.Tasks))
			// feed back into queue
			q.wp.jobmu.Lock()
			for _, p := range ta.Tasks {
				q.wp.jobq.PushBack(p.Body)
			}
			q.wp.jobmu.Unlock()

			q.m.Lock()
			delete(q.availableTasks, k)
			q.m.Unlock()
			q.internalStatus()
			return
		}
	}

	q.m.Unlock()
}

func (q *Quorum) cleanupTimedOutPendingTasks() {
	q.m.Lock()
	defer q.m.Unlock()

	//log.Infof("%d pending tasks", len(q.pendingAckTasks))

	for k, ta := range q.pendingAckTasks {
		if time.Since(ta.Emitted) > q.availabilityTimeout {
			delete(q.pendingAckTasks, k)
			log.Infof("[INFO <%s>] Acknowledgment timeout. Removed pending %s (%s, %s) %d tasks", q.Name, ta.UUID, ta.Emitter, ta.Emitted, len(ta.Tasks))
			q.internalStatus()
			return
		}
	}
}

func (q *Quorum) cleanupTimedOutAwaitingTasks() {
	q.m.Lock()
	defer q.m.Unlock()

	//log.Infof("%d awaiting responses tasks", len(q.awaitingResponses))

	for k, ta := range q.awaitingResponses {
		//log.Infof("Checking %+v", e.Value)
		if time.Since(ta.Emitted) > q.executionTimeout {
			//log.Infof("Feeding back %s (%s, %s) %d tasks", ta.UUID, ta.Emitter, ta.Emitted, len(ta.Tasks))
			log.Infof("[INFO <%s>] Execution timeout. Feeding back %s (%s, %s) %d tasks", q.Name, ta.UUID, ta.Emitter, ta.Emitted, len(ta.Tasks))
			// feed back into queue
			q.wp.jobmu.Lock()
			for _, p := range ta.Tasks {
				q.wp.jobq.PushBack(p.Body)
			}
			q.wp.jobmu.Unlock()

			delete(q.awaitingResponses, k)
			q.internalStatus()
			return
		}
	}
}

// ShareTasks will offer taskchunk to quorum, wait if it's accepted and aknowledged
// -> YES: wait for response
// -> NO: put back into local queue
func (q *Quorum) ShareTasks(tasks []Payload, wp *WorkerPool) {

	ta := TaskAvailable{
		UUID:    uuid.NewString(),
		Emitter: q.DSN,
		Emitted: time.Now(),
		Tasks:   tasks,
	}

	// set tasks in available list, so we can put them back in queue if no one want them
	q.m.Lock()
	q.availableTasks[ta.UUID] = ta
	q.internalStatus()
	q.m.Unlock()

	// send TaskAvailable message to all instance in quorum
	q.Broadcast(Global{TaskAvailable: ta, Cmd: "TaskAvailable"})
	/*
		for _, inst := range q.instances {
			if inst.WriteConn == nil {
				continue
			}
			inst.Write(Global{TaskAvailable: ta, Cmd: "TaskAvailable"})
		}
	*/
	q.debug("Broadcasted %s (%d tasks)", ta.UUID, len(tasks))
}

func (q *Quorum) SharedTasks() int {
	var l int
	q.m.Lock()
	l = len(q.availableTasks)
	q.m.Unlock()
	return l
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
			time.Sleep(1000 * time.Millisecond)
		}
		count = 0

		q.m.Lock()
		insts := make(map[string]*Instance)
		for k, v := range q.instances {
			insts[k] = v
		}
		q.m.Unlock()

		for _, inst := range insts {
			if inst.WriteConn == nil {
				err := q.Connect(inst)
				if err != nil {
					//q.debug("cannot connect to %s: %s", inst.DSN, err)
					time.Sleep(100 * time.Millisecond)
				}
				if q.hasReadConn(inst) {
					q.debug("Connected to %s (%s)", q.hasName(inst), q.hasDSN(inst))
				}
			}

			if inst.WriteConn != nil && q.hasReadConn(inst) {
				count++
			}
		}
		if count == q.instanceCount() {
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
		conn.Close()
		return
	}
	//q.debug("Received CONNECT: %+v", cmsg)

	found := false
	for i := range q.instances {
		if q.instances[i].DSN == cmsg.DSN {
			found = true
			q.m.Lock()
			inst = q.instances[i]
			inst.ReadConn = conn
			inst.Name = cmsg.Name
			q.m.Unlock()
			if q.hasWriteConn(inst) {
				q.debug("Connected to %s (%s)}", cmsg.Name, cmsg.DSN)
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

	var msg Global
	for q.status() == Running {
		err := dec.Decode(&msg)
		if q.status() != Running {
			return
		}
		if err != nil && (err == io.EOF || err == net.ErrClosed) {
			q.debug("Instance %+v disconnected", inst)
			inst.ReadConn.Close()
			inst.WriteConn.Close()
			q.removeInstance(inst)
			return
		}
		if err != nil && err.Error() != "gob: duplicate type received" {
			q.debug("Decode error: %s", err)
			if strings.Contains(err.Error(), "use of closed network connection") {
				inst.ReadConn.Close()
				inst.WriteConn.Close()
				q.removeInstance(inst)
				return
			}
			continue
		}

		if err != nil {
			q.debug("Decode error real: %s", err)
			continue
		}

		switch msg.Cmd {
		case "TaskAvailable":
			ta := msg.TaskAvailable
			if ta.UUID == "" {
				log.Errorf("WTF ????? TaskAvailable :%+v", msg)
				continue
			}
			q.handleTasksAvailable(&ta)
		case "TaskAcceptance":
			ta := msg.TaskAcceptance
			if ta.UUID == "" {
				log.Errorf("WTF ????? TaskAcceptance :%+v", msg)
				continue
			}
			q.handleTaskAcceptance(&ta)
		case "TaskAcceptanceAck":
			tack := msg.TaskAcceptanceAck
			if tack.UUID == "" {
				log.Errorf("WTF ????? TaskAcceptanceAck :%+v", msg)
				continue
			}
			q.handleTaskAcceptanceAck(&tack)
		case "TaskAcceptanceReject":
			tr := msg.TaskAcceptanceReject
			q.handleTaskAcceptanceReject(&tr)
		case "TaskResult":
			tr := msg.TaskResult
			if tr.UUID == "" {
				log.Errorf("WTF ????? TaskResult :%+v", msg)
				continue
			}
			q.handleTaskResult(&tr)
		default:
			q.debug("Received unknown message: %+v", msg)
		}
	}
}

// handleTasksAvailable will check with local WorkerPool if tasks are worth accepting
// - Low local queue
// - User defined callback say OK
//
// Then push tasks to pending waiting for emitter validation
func (q *Quorum) handleTasksAvailable(ta *TaskAvailable) {
	//q.debug("Received %d tasks available from %s", len(ta.Tasks), ta.Emitter)

	// to avoid all instances in quorum trying to accept the same tasks, add random sleep time
	//rand.Seed(time.Now().UnixNano())
	//n := rand.Intn(100) // n will be between 0 and 10
	//	fmt.Printf("Sleeping %d seconds...\n", n)
	//	time.Sleep(time.Duration(n) * time.Millisecond)
	// Randomly refuse to accept task
	//if n > 100/(q.InstancesConnected()-1) {
	/*
		ic := q.InstancesConnected()
		if ic > 2 && n > 100/((q.InstancesConnected()-2)/2) {
			log.Infof("%d > %d => Randomly refusing", n, 100/((q.InstancesConnected()-2)/2))
			return
		}
	*/

	if ta.Emitter == q.DSN {
		log.Errorf("WAIT WHY AM I RECEIVING THIS %+v", ta)
		return
	}

	// let local workerpool decide if we can accept task
	if q.wp.canAcceptTasks(ta.Tasks) == false {
		return
	}

	q.m.Lock()
	// avoid accepting too much tack at one
	if len(q.pendingAckTasks) > 0 {
		q.m.Unlock()
		return
	}

	q.pendingAckTasks[ta.UUID] = *ta
	q.internalStatus()
	q.m.Unlock()
	q.debug("Accepting %d tasks %s from %s, adding to pendingAckTasks", len(ta.Tasks), ta.UUID, ta.Emitter)

	tacc := TaskAcceptance{
		UUID:     ta.UUID,
		Emitter:  ta.Emitter,
		Accepter: q.DSN,
		//Count:    0,
	}

	q.m.Lock()
	/*
		// Broadcast acceptance to all quorum
			for _, inst := range q.instances {
		inst.Write(Global{Cmd: "TaskAcceptance", TaskAcceptance: tacc})
		}
	*/
	emitter, ok := q.instances[ta.Emitter]
	if !ok {
		q.m.Unlock()
		log.Errorf("could not find Emitter (%s) in instance list", ta.Emitter)
		return
	}
	emitter.Write(Global{Cmd: "TaskAcceptance", TaskAcceptance: tacc})
	// Add to local acceptedTasks, cause local won't receive self TaskAcceptance obviously
	q.acceptedTasks[ta.UUID] = &tacc
	q.m.Unlock()
}

// handleTaskAcceptance job is to create concensus on quorum of which tasks are attributed to which instance
func (q *Quorum) handleTaskAcceptance(ta *TaskAcceptance) {
	//q.debug("Received task acceptance (Emitter: %s, Accepter: %s)", ta.Emitter, ta.Accepter)

	q.m.Lock()

	// has someone already accepted ?
	_, ok := q.acceptedTasks[ta.UUID]
	if ok {
		accepter, ok := q.instances[ta.Accepter]
		if !ok {
			q.m.Unlock()
			log.Errorf("could not find Accepter (%s) in instance list", ta.Accepter)
			return
		}
		tr := TaskAcceptanceReject{
			UUID:     ta.UUID,
			Emitter:  ta.Emitter,
			Accepter: ta.Accepter,
		}
		accepter.Write(Global{Cmd: "TaskAcceptanceReject", TaskAcceptanceReject: tr})
		q.m.Unlock()
		q.debug("Task %s has already been accepted, rejecting %s", ta.UUID, ta.Accepter)
		return
	}

	// if not, lock task uuid with current accepter
	q.acceptedTasks[ta.UUID] = ta
	q.internalStatus()
	q.m.Unlock()

	// Oh shit my task have been accepted, removing from available
	// Add them to awaitingResponses
	if ta.Emitter == q.DSN {
		q.debug("%s has accepted my task %s!", ta.Accepter, ta.UUID)
		q.m.Lock()
		t, ok := q.availableTasks[ta.UUID]
		if !ok {
			log.Errorf("[%s] not found in available tasks !?", ta.UUID)
			return
		}
		delete(q.availableTasks, ta.UUID)
		q.awaitingResponses[ta.UUID] = t
		q.internalStatus()
		q.m.Unlock()
		q.debug("Adding %s to awaitingResponses", t.UUID)
	}

	// send ack to accepter
	tack := TaskAcceptanceAck{
		UUID:         ta.UUID,
		Emitter:      ta.Emitter,
		Accepter:     ta.Accepter,
		Acknowledger: q.DSN,
	}

	// Respond to Accepter
	q.m.Lock()
	accepter, ok := q.instances[ta.Accepter]
	if !ok {
		q.m.Unlock()
		log.Errorf("could not find Accepter (%s) in instance list", ta.Accepter)
		return
	}
	q.m.Unlock()

	accepter.Write(Global{Cmd: "TaskAcceptanceAck", TaskAcceptanceAck: tack})

	/*
		q.Broadcast(Global{Cmd: "TaskAcceptanceAck", TaskAcceptanceAck: tack})
	*/
	/*
		// Respond to Emitter and Accepter
		if ta.Accepter != q.DSN {
			q.instances[ta.Accepter].Write(Global{Cmd: "TaskAcceptanceAck", TaskAcceptanceAck: tack})
		}
		if ta.Emitter != q.DSN {
			q.instances[ta.Emitter].Write(Global{Cmd: "TaskAcceptanceAck", TaskAcceptanceAck: tack})
		}
	*/
	/*
		for _, inst := range q.instances {
			inst.Write(Global{Cmd: "TaskAcceptanceAck", TaskAcceptanceAck: tack})
		}
	*/
}

// handleTaskAcceptanceReject when receiving TaskAcceptanceReject,
// meaning Emitter rejected local acceptance and we need to remove from pendingAckTasks
func (q *Quorum) handleTaskAcceptanceReject(tr *TaskAcceptanceReject) {
	q.debug("Received reject from <%s>", tr.Emitter)
	q.m.Lock()
	delete(q.pendingAckTasks, tr.UUID)
	q.m.Unlock()
}

// handleTaskAcceptanceAck will count the number of acceptance acknowledgement until reaching quorum majority
//
// - If no concensus is attained, tasks will be put back into local queue
// - If consensus is reached, Emitter will put tasks in awaitingResponses map and wait until EXECUTION_TIMEOUT
// before putting tasks back into local queue
// - If consensus is reached, Accepter will start executing tasks and preparing TaskResult message
func (q *Quorum) handleTaskAcceptanceAck(tack *TaskAcceptanceAck) {

	/*
			q.m.Lock()

			ta, ok := q.acceptedTasks[tack.UUID]
			if !ok {
				q.m.Unlock()
				q.debug("Task %+v not found but acknowledged", tack)
				return
			}
			q.acceptedTasks[tack.UUID].Count++
			c := ta.Count
			q.m.Unlock()

		q.debug("[%s] %s ack acceptance (Emitter: %s, Accepter: %s) %d/%d", tack.UUID, tack.Acknowledger, tack.Emitter, tack.Accepter, c, q.InstancesConnected())
	*/

	// We actually do not need a majority, only Emitter accepting
	/*
		// So we want a majority.
		// Accepter is OK obviously
		//q.debug("Ack count for %s is %d", tack.UUID, c)
		if c < q.InstancesConnected()/2 {
			return
		}
		//q.debug("TASK VALIDATED AND ACKNOWLEDGED %s( %d / %d)", tack.UUID, c, q.InstanceCount()/2)
	*/

	q.m.Lock()
	//delete(q.acceptedTasks, tack.UUID)
	q.internalStatus()
	task, ok := q.pendingAckTasks[tack.UUID]
	if ok {
		delete(q.pendingAckTasks, tack.UUID)
		q.internalStatus()
	}
	q.m.Unlock()

	// Oh shit it's me, let's get to work
	if tack.Accepter == q.DSN {
		if len(task.Tasks) == 0 {
			log.Errorf("TaskAvailable from pendingAckTasks %s HAS 0 TASKS HERE", tack.UUID)
			return
		}
		q.debug("Executing %d tasks for instance <%s>", len(task.Tasks), tack.Emitter)
		if !ok {
			q.debug("Supposed to exec %s but cannot find it in pendingAckTasks...", tack.UUID)
			return
		}
		//go q.exec(task)
		q.exec(task)
	}

	/*
		if tack.Emitter == q.DSN {
			q.debug("[%s] Task accepted and aknowledged by quorum, removing from availableTasks and moving to awaitingResponses", tack.UUID)
		}
	*/
}

// TODO: Implement and use WorkerPool.ExecBatch
func (q *Quorum) exec(ta TaskAvailable) {
	tr := TaskResult{
		UUID:    ta.UUID,
		Emitter: ta.Emitter,
		Worker:  q.DSN,
	}

	//	begin := time.Now()
	r := make([]Response, len(ta.Tasks))

	/*
		for _, t := range ta.Tasks {
			r := Response{}
			r.Body, r.Err = q.wp.Exec(t.Body)
			tr.Responses = append(tr.Responses, r)
		}
	*/
	for i, t := range ta.Tasks {
		if i < len(ta.Tasks)-1 {
			go func(i int, t Payload, r *[]Response) {
				resp := Response{}
				resp.Body, resp.Err = q.wp.Exec(t.Body)
				(*r)[i] = resp
			}(i, t, &r)
		}
	}

	t := ta.Tasks[len(ta.Tasks)-1]
	resp := Response{}
	resp.Body, resp.Err = q.wp.Exec(t.Body)
	r[len(ta.Tasks)-1] = resp
	tr.Responses = r

	// Send back to Emitter
	q.instances[ta.Emitter].Write(Global{Cmd: "TaskResult", TaskResult: tr})

	//log.Infof("Exec (%d tasks) done in %s", len(ta.Tasks), time.Since(begin))
}

func (q *Quorum) handleTaskResult(tr *TaskResult) {
	q.m.Lock()
	defer q.m.Unlock()

	ta, ok := q.awaitingResponses[tr.UUID]
	if !ok {
		log.Errorf("[ERROR <%s>] Received tasks response %s from <%s> but wasn't waiting for it", q.Name, tr.UUID, tr.Worker)
		return
	}
	delete(q.awaitingResponses, tr.UUID)

	log.Infof("[STATUS <%s>] Got results for task %s %s after emission", q.Name, ta.UUID, time.Since(ta.Emitted))
	for i, r := range tr.Responses {
		p := ta.Tasks[i]
		if p.ResponseChan != nil {
			p.ResponseChan <- r
		} else {
			q.wp.pushResponse(r)
		}
		q.wp.Done()
	}
	q.internalStatus()
}

func (q *Quorum) Connect(inst *Instance) error {

	c, err := net.Dial("tcp", inst.DSN)
	if err != nil {
		return err
	}
	q.m.Lock()
	inst.WriteConn = c
	inst.Encoder = gob.NewEncoder(inst.WriteConn)
	q.m.Unlock()

	cmsg := ConnectMessage{
		Name: q.Name,
		DSN:  q.DSN,
	}

	err = inst.Encoder.Encode(cmsg)
	if err != nil {
		return err
	}

	dec := gob.NewDecoder(inst.WriteConn)
	err = dec.Decode(&cmsg)
	if err != nil {
		return err
	}
	q.m.Lock()
	inst.Name = cmsg.Name
	q.m.Unlock()
	return nil
}

func (q *Quorum) debug(format string, vars ...interface{}) {
	d := fmt.Sprintf(format, vars...)
	q.m.Lock()
	name := q.Name
	q.m.Unlock()
	log.Infof("[DEBUG <%s>] %s\n", name, d)
}

func (q *Quorum) removeInstance(inst *Instance) {
	q.m.Lock()
	defer q.m.Unlock()
	delete(q.instances, inst.DSN)
}

func (q *Quorum) instanceCount() int {
	q.m.Lock()
	c := len(q.instances)
	q.m.Unlock()
	return c
}

func (q *Quorum) addInstance(inst *Instance) {
	q.m.Lock()
	defer q.m.Unlock()
	q.instances[inst.DSN] = inst
}

func (q *Quorum) status() Status {
	q.m.Lock()
	s := q.Status
	q.m.Unlock()
	return s
}

func (q *Quorum) hasReadConn(inst *Instance) bool {
	q.m.Lock()
	b := inst.ReadConn != nil
	q.m.Unlock()
	return b
}

func (q *Quorum) hasWriteConn(inst *Instance) bool {
	q.m.Lock()
	b := inst.WriteConn != nil
	q.m.Unlock()
	return b
}

func (q *Quorum) hasName(inst *Instance) string {
	q.m.Lock()
	n := inst.Name
	q.m.Unlock()
	return n
}

func (q *Quorum) hasDSN(inst *Instance) string {
	q.m.Lock()
	dsn := inst.DSN
	q.m.Unlock()
	return dsn
}

func (q *Quorum) InstancesConnected() int {
	var c int = 1
	q.m.Lock()
	for _, inst := range q.instances {
		if inst.WriteConn != nil && inst.ReadConn != nil {
			c++
		}
	}
	q.m.Unlock()
	return c
}

func (inst *Instance) Connected() bool {
	if inst == nil || inst.WriteConn == nil || inst.ReadConn == nil || inst.Encoder == nil {
		return false
	}
	return true
}

func (inst *Instance) Write(msg interface{}) {
	if inst == nil || inst.WriteConn == nil || inst.ReadConn == nil || inst.Encoder == nil {
		return
	}
	go func() {
		err := inst.Encoder.Encode(msg)
		if err != nil {
			log.Errorf("Cannot share TaskAvailable with %s: %s", inst.DSN, err)
		}
	}()
}

func (q *Quorum) Broadcast(msg interface{}) {
	q.m.Lock()
	defer q.m.Unlock()

	for _, inst := range q.instances {
		if inst.ReadConn == nil || inst.WriteConn == nil {
			continue
		}
		inst.Write(msg)
	}
}
