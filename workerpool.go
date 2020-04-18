package workerpool

import (
	"container/list"
	"sync"
	"time"
)

var (
	// DefaultSizesPercentil defines regular increase ten by ten from 1 to 100.
	DefaultSizesPercentil = []int{1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	// AllSizesPercentil defines all percentils from 1 to 100. it allows WorkerPool to find the perfect sizing fo optimal velocity. Only worth for long running operation
	AllSizesPercentil = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100}
	// LogSizesPercentil defines a logarithmic distribution from 1 to 100. Perfect for job targeting client sensible to load.
	LogSizesPercentil = []int{1, 15, 23, 30, 34, 38, 42, 45, 47, 50, 52, 54, 57, 58, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100}
	// AllInSizesPercentil allows only 100% of workerpool, meaning WorkerPool will always use MaxWorker goroutines
	AllInSizesPercentil = []int{100}
)

// JobFnc defines the job function executed by WorkerPool workers
//
// Note: if both response and error are nil, response will not be stacked
type JobFnc func(payload interface{}) (response interface{}, err error)

// OptFunc defines functionnal parameter to New() function
type OptFunc func(w *WorkerPool)

// Response from workers. Can be read from ReturnChannel
type Response struct {
	Body interface{}
	Err  error
}

type Status string

const (
	Running Status = "running"
	Paused  Status = "paused"
	Stopped Status = "stopped"
)

// WorkerPool is an auto-scaling generic worker pool.
//
// Features include:
// - Automatic scaling in response to effective velocity (op/s)
// - Convenient worker response reading
// - Automatic scale down uppon overload
// - Easy goroutine cleanup
type WorkerPool struct {
	MaxDuration    time.Duration
	MaxWorker      int
	Job            JobFnc
	ReturnChannel  chan Response
	SizePercentil  []int
	EvaluationTime int

	status  Status
	stopped bool

	jobch  chan interface{}
	active int
	wanted int

	velocity  map[int]int
	ops       map[int]int
	sizeindex int

	responses *list.List
	respmu    sync.RWMutex

	sync.WaitGroup
	mu sync.RWMutex
}

func WithMaxWorker(max int) OptFunc {
	fn := func(wp *WorkerPool) {
		wp.MaxWorker = max
	}
	return fn
}

func WithSizePercentil(s []int) OptFunc {
	fn := func(wp *WorkerPool) {
		wp.SizePercentil = s
	}
	return fn
}

// Set evaluation time in second
func WithEvaluationTime(s int) OptFunc {
	fn := func(wp *WorkerPool) {
		wp.EvaluationTime = s
	}
	return fn
}

func WithMaxDuration(d time.Duration) OptFunc {
	fn := func(wp *WorkerPool) {
		wp.MaxDuration = d
	}
	return fn
}

func New(jobfnc JobFnc, opts ...OptFunc) (*WorkerPool, error) {

	wp := &WorkerPool{
		Job:            jobfnc,
		MaxDuration:    3 * time.Second,
		MaxWorker:      500,
		SizePercentil:  DefaultSizesPercentil,
		EvaluationTime: 5,
		ops:            make(map[int]int),
		velocity:       make(map[int]int),
		responses:      list.New(),
		status:         Running,
	}

	// apply options
	for _, fn := range opts {
		fn(wp)
	}

	wp.ReturnChannel = make(chan Response)
	wp.jobch = make(chan interface{}, wp.MaxWorker)

	// spawn the first size worker
	wp.setsize(0)
	// start velocity routine
	go wp.velocityRoutine()
	go wp.responseRoutine()

	return wp, nil
}

// VelocityValues returns map of recorded velocity for each used velocity percentil
func (wp *WorkerPool) VelocityValues() map[int]int {
	c := make(map[int]int)
	wp.mu.RLock()
	for k, v := range wp.velocity {
		c[k] = v
	}
	wp.mu.RUnlock()
	return c
}

func (wp *WorkerPool) CurrentVelocityValues() (int, int) {
	i := wp.index()
	wp.mu.RLock()
	p := wp.SizePercentil[i]
	v := wp.velocity[wp.SizePercentil[i]]
	wp.mu.RUnlock()

	return p, v
}

// Stop WorkerPool. All worker goroutine will exit, but stacked responses can still be consumed
func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.stopped = true
	wp.status = Stopped
	close(wp.jobch)
}

// Pause all workers with killing them
func (wp *WorkerPool) Pause() {
	wp.mu.Lock()
	wp.status = Paused
}

// Resume all workers
func (wp *WorkerPool) Resume() {
	wp.status = Running
	wp.mu.Unlock()
}

// Status return WorkerPool current status
func (wp *WorkerPool) Status() Status {
	wp.mu.RLock()
	st := wp.status
	wp.mu.RUnlock()
	return st
}

// Feed payload to worker
func (wp *WorkerPool) Feed(payload interface{}) {
	wp.jobch <- payload
	wp.Add(1)
}

// AvailableResponses returns the current number of stacked responses. Consume ReturnChannel to read them
func (wp *WorkerPool) AvailableResponses() int {
	wp.respmu.RLock()
	n := wp.responses.Len()
	wp.respmu.RUnlock()
	return n
}

func (wp *WorkerPool) index() int {
	wp.mu.RLock()
	si := wp.sizeindex
	wp.mu.RUnlock()
	return si
}

func (wp *WorkerPool) evaluate(d time.Duration, err error) bool {
	if err != nil {
		return wp.exit()
	}

	if d > wp.MaxDuration*time.Second {
		return wp.exit()
	}

	wp.mu.RLock()
	active := wp.active
	wanted := wp.wanted
	wp.mu.RUnlock()
	if active > wanted {
		return wp.exit()
	}

	return false
}

func (wp *WorkerPool) exit() bool {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	// leave at least 1 worker
	if wp.active == 1 {
		return false
	}

	wp.active--
	return true
}

func (wp *WorkerPool) spawn() {
	wp.active++
	go wp.worker()
}

func (wp *WorkerPool) worker() {
	var err error
	var body interface{}

	for {

		p, ok := <-wp.jobch
		if !ok {
			return
		}

		begin := time.Now()
		body, err = wp.Job(p)
		t := time.Since(begin)
		wp.Done()
		wp.tick()

		if body != nil || err != nil {
			wp.pushResponse(Response{
				Body: body,
				Err:  err,
			})
		}

		if shouldExit := wp.evaluate(t, err); shouldExit {
			return
		}
	}
}

func (wp *WorkerPool) isStopped() bool {
	wp.mu.RLock()
	stopped := wp.stopped
	wp.mu.RUnlock()
	return stopped
}

func (wp *WorkerPool) tick() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	wp.ops[wp.SizePercentil[wp.sizeindex]]++
}

func (wp *WorkerPool) velocityRoutine() {
	for {
		if wp.isStopped() {
			return
		}

		wp.mu.Lock()
		wp.ops[wp.SizePercentil[wp.sizeindex]] = 0
		wp.mu.Unlock()

		time.Sleep(time.Duration(wp.EvaluationTime) * time.Second)

		i := wp.index()

		wp.mu.Lock()
		wp.velocity[wp.SizePercentil[i]] = wp.ops[wp.SizePercentil[i]] / wp.EvaluationTime
		wp.mu.Unlock()

		wp.mu.RLock()
		// always increase from first value
		if i == 0 && i < len(wp.SizePercentil)-1 {
			wp.mu.RUnlock()
			wp.setsize(i + 1)
			continue
		}
		// if velocity is 0, decrease
		if wp.velocity[wp.SizePercentil[i]] == 0 {
			wp.mu.RUnlock()
			wp.setsize(i - 1)
			continue
		}
		// if velocity increased, then increase worker pool size
		if i < len(wp.SizePercentil)-1 && wp.velocity[wp.SizePercentil[i]] > wp.velocity[wp.SizePercentil[i-1]] {
			wp.mu.RUnlock()
			wp.setsize(i + 1)
			continue
		}
		// if velocity decreased then decrease worker pool size
		if wp.sizeindex > 0 && wp.velocity[wp.SizePercentil[i]] < wp.velocity[wp.SizePercentil[i-1]] {
			wp.mu.RUnlock()
			wp.setsize(i - 1)
			continue
		}
		wp.mu.RUnlock()
	}
}

func (wp *WorkerPool) setsize(i int) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	wp.sizeindex = i
	wp.wanted = int(wp.MaxWorker / 100 * wp.SizePercentil[i])
	if wp.wanted == 0 {
		wp.wanted++
	}

	if wp.active < wp.wanted {
		for i := wp.active; i < wp.wanted; i++ {
			wp.spawn()
		}
	}
}

func (wp *WorkerPool) pushResponse(r Response) {
	wp.respmu.Lock()
	wp.responses.PushBack(r)
	wp.respmu.Unlock()
}

func (wp *WorkerPool) responseRoutine() {
	var n int
	for {
		n = wp.AvailableResponses()
		if wp.isStopped() && n == 0 {
			close(wp.ReturnChannel)
			return
		}

		if n == 0 {
			time.Sleep(1 * time.Millisecond)
			continue
		}

		wp.respmu.RLock()
		e := wp.responses.Front()
		wp.respmu.RUnlock()
		r, ok := e.Value.(Response)
		if !ok {
			continue
		}
		wp.ReturnChannel <- r

		wp.respmu.Lock()
		wp.responses.Remove(e)
		wp.respmu.Unlock()
	}
}
