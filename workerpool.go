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
type JobFnc func(payload interface{}) (response interface{}, err error)

// OptFunc defines functionnal parameter to New() function
type OptFunc func(w *WorkerPool)

// Response from workers. Can be read from ReturnChannel
type Response struct {
	Body interface{}
	Err  error
}

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

	stopped bool

	jobch  chan interface{}
	active int
	wanted int

	velocity  map[int]int
	ops       map[int]int
	sizeindex int
	vm        sync.Mutex

	responses *list.List
	rm        sync.Mutex

	sync.WaitGroup
	sync.RWMutex
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

func (wp *WorkerPool) VelocityValues() map[int]int {
	c := make(map[int]int)
	wp.vm.Lock()
	for k, v := range wp.velocity {
		c[k] = v
	}
	wp.vm.Unlock()
	return c
}

func (wp *WorkerPool) CurrentVelocityValues() (int, int) {
	wp.vm.Lock()
	p := wp.SizePercentil[wp.sizeindex]
	v := wp.velocity[wp.SizePercentil[wp.sizeindex]]
	wp.vm.Unlock()

	return p, v
}

func (wp *WorkerPool) Stop() {
	wp.stopped = true
	close(wp.jobch)
}

// Pause all workers with killing them
func (wp *WorkerPool) Pause() {
	wp.Lock()
}

// Resume all workers
func (wp *WorkerPool) Resume() {
	wp.Unlock()
}

// Feed payload to worker
func (wp *WorkerPool) Feed(payload interface{}) {
	wp.jobch <- payload
	wp.Add(1)
}

func (wp *WorkerPool) AvailableResponses() int {
	wp.rm.Lock()
	n := wp.responses.Len()
	wp.rm.Unlock()
	return n
}

func (wp *WorkerPool) evaluate(d time.Duration, err error) bool {
	if err != nil {
		return wp.errexit(err)
	}

	if d > wp.MaxDuration*time.Second {
		return wp.timeexit(d)
	}

	wp.RLock()
	defer wp.RUnlock()
	if wp.active > wp.wanted {
		wp.active--
		return true
	}

	return false
}

func (wp *WorkerPool) errexit(err error) bool {

	wp.Lock()
	defer wp.Unlock()

	// leave at least 1 worker
	if wp.active == 1 {
		return false
	}

	wp.active--
	return true
}

func (wp *WorkerPool) timeexit(t time.Duration) bool {

	wp.Lock()
	defer wp.Unlock()

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

		wp.pushResponse(Response{
			Body: body,
			Err:  err,
		})

		if shouldExit := wp.evaluate(t, err); shouldExit {
			return
		}
	}
}

func (wp *WorkerPool) tick() {
	wp.Lock()
	defer wp.Unlock()

	wp.ops[wp.SizePercentil[wp.sizeindex]]++
}

func (wp *WorkerPool) velocityRoutine() {
	for {
		if wp.stopped {
			return
		}

		wp.Lock()
		wp.ops[wp.SizePercentil[wp.sizeindex]] = 0
		wp.Unlock()

		time.Sleep(time.Duration(wp.EvaluationTime) * time.Second)

		wp.vm.Lock()
		wp.velocity[wp.SizePercentil[wp.sizeindex]] = wp.ops[wp.SizePercentil[wp.sizeindex]] / wp.EvaluationTime
		wp.vm.Unlock()

		// always increase from first value
		if wp.sizeindex == 0 && wp.sizeindex < len(wp.SizePercentil)-1 {
			//			fmt.Printf("VelocityRoutine: increasing because sizeindex=0\n")
			wp.setsize(wp.sizeindex + 1)
			continue
		}
		// if velocity is 0, decrease
		if wp.velocity[wp.SizePercentil[wp.sizeindex]] == 0 {
			wp.setsize(wp.sizeindex - 1)
			continue
		}
		// if velocity increased, then increase worker pool size
		if wp.sizeindex < len(wp.SizePercentil)-1 && wp.velocity[wp.SizePercentil[wp.sizeindex]] > wp.velocity[wp.SizePercentil[wp.sizeindex-1]] {
			//			fmt.Printf("VelocityRoutine: increasing because velocity(%d%%)=%d \n")
			wp.setsize(wp.sizeindex + 1)
			continue
		}
		// if velocity decreased then decrease worker pool size
		if wp.sizeindex > 0 && wp.velocity[wp.SizePercentil[wp.sizeindex]] < wp.velocity[wp.SizePercentil[wp.sizeindex-1]] {
			wp.setsize(wp.sizeindex - 1)
			continue
		}
	}
}

func (wp *WorkerPool) setsize(i int) {
	wp.Lock()
	defer wp.Unlock()

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
	wp.rm.Lock()
	wp.responses.PushBack(r)
	wp.rm.Unlock()
}

func (wp *WorkerPool) responseRoutine() {
	var n int
	for {
		n = wp.AvailableResponses()

		if wp.stopped && n == 0 {
			close(wp.ReturnChannel)
			return
		}

		if n == 0 {
			time.Sleep(1 * time.Millisecond)
			continue
		}

		wp.rm.Lock()
		e := wp.responses.Front()
		wp.rm.Unlock()
		r, ok := e.Value.(Response)
		if !ok {
			continue
		}
		wp.ReturnChannel <- r

		wp.rm.Lock()
		wp.responses.Remove(e)
		wp.rm.Unlock()
	}
}
