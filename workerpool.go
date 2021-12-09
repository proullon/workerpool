package workerpool

import (
	"container/list"
	"runtime"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
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

// Payload contains actual payload push via Feed()
// and metadata like try number
type Payload struct {
	Body         interface{}
	Try          int
	ResponseChan chan Response
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
	// MaxDuration sets the maximum allowed duration to execute a job
	MaxDuration time.Duration
	// MaxWorker defines the maximum goroutine executing Job concurrently
	MaxWorker int
	// Job to be executed by WorkerPool
	Job JobFnc
	// ReturnChannel contains JobFnc returned values
	ReturnChannel chan Response
	// SizePercentil sets the possible percentage of MaxWorker usable by WorkerPool
	SizePercentil []int
	// EvaluationTime represents the duration of velocity collection
	EvaluationTime int
	// Retry will fead again payload N times on failure
	// returning error only on last fail
	Retry int
	// MaxQueue limits the number of items in the queue.
	// 0 is unlimited, using list.List
	MaxQueue int

	status  Status
	stopped bool

	jobmu  sync.Mutex
	jobq   *list.List
	jobch  chan *Payload
	active int
	wanted int

	velocity  map[int]float64
	avg_sum   map[int]float64
	ops       map[int]int
	sizeindex int

	responses *list.List
	respmu    sync.RWMutex

	sync.WaitGroup
	mu sync.RWMutex

	quorum *Quorum
}

// Velocity information for each sizing
// rps describe the number of operation per second
// avg describe the average duration of operation
type Velocity struct {
	Ops float64
	Avg float64
}

// WithRetry instructs new WorkerPool to
// retry payload n times on error.
// Note: error is only sent to ReturnChannel on last retry
// Default is 0
func WithRetry(n int) OptFunc {
	fn := func(wp *WorkerPool) {
		wp.Retry = n
	}
	return fn
}

// WithMaxWorker instructs new WorkerPool to
// limit the maximum number of goroutine to max.
// Default is 500
func WithMaxWorker(max int) OptFunc {
	fn := func(wp *WorkerPool) {
		wp.MaxWorker = max
	}
	return fn
}

// WithSizePercentil creates new WorkerPool
// with given SizePercentil. Default is DefaultSizesPercentil
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

// WithMaxQueue limits queue size, sometimes you may risk OOM kill...
func WithMaxQueue(max int) OptFunc {
	fn := func(wp *WorkerPool) {
		wp.MaxQueue = max
	}
	return fn
}

func New(jobfnc JobFnc, opts ...OptFunc) (*WorkerPool, error) {

	wp := &WorkerPool{
		Job:            jobfnc,
		MaxDuration:    3 * time.Second,
		MaxWorker:      runtime.NumCPU(),
		SizePercentil:  DefaultSizesPercentil,
		EvaluationTime: 5,
		ops:            make(map[int]int),
		velocity:       make(map[int]float64),
		avg_sum:        make(map[int]float64),
		responses:      list.New(),
		jobq:           list.New(),
		status:         Running,
	}

	// apply options
	for _, fn := range opts {
		fn(wp)
	}

	if wp.quorum != nil {
		err := wp.quorum.Start(wp.MaxWorker)
		if err != nil {
			return nil, err
		}
	}

	wp.ReturnChannel = make(chan Response)
	if wp.MaxQueue > 0 {
		wp.jobch = make(chan *Payload, wp.MaxQueue)
	} else {
		wp.jobch = make(chan *Payload, wp.MaxWorker)
	}

	// spawn the first size worker
	wp.setsize(0)
	// start velocity routine
	go wp.velocityRoutine()
	go wp.responseRoutine()
	if wp.MaxQueue == 0 {
		go wp.feedRoutine()
	}

	return wp, nil
}

// Responses returns Response channel containing all Job returned values
func (wp *WorkerPool) Responses() chan Response {
	return wp.ReturnChannel
}

// VelocityValues returns map of recorded velocity for each used velocity percentil
func (wp *WorkerPool) VelocityValues() map[int]Velocity {
	c := make(map[int]Velocity)
	wp.mu.RLock()
	for k, v := range wp.velocity {
		c[k] = Velocity{
			Ops: v,
			Avg: wp.avg_sum[k] / float64(wp.ops[k]),
		}
	}
	wp.mu.RUnlock()
	return c
}

func (wp *WorkerPool) CurrentVelocityValues() (percentil int, velocity float64, average float64) {
	i := wp.index()
	wp.mu.RLock()
	percentil = wp.SizePercentil[i]
	velocity = wp.velocity[wp.SizePercentil[i]]
	average = wp.avg_sum[wp.SizePercentil[i]] / float64(wp.ops[wp.SizePercentil[i]])
	wp.mu.RUnlock()

	return percentil, velocity, average
}

// Stop WorkerPool. All worker goroutine will exit, but stacked responses can still be consumed
func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	wp.stopped = true
	wp.status = Stopped
	if wp.quorum != nil {
		wp.quorum.Stop()
	}
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

// Active returns the number of active goroutines
func (wp *WorkerPool) Active() int {
	wp.mu.RLock()
	a := wp.active
	wp.mu.RUnlock()
	return a
}

// Exec job with direct response and blocking until response is received
// Usefull for ratelimiting execution
func (wp *WorkerPool) Exec(body interface{}) (response interface{}, err error) {
	ch := make(chan Response)

	wp.Add(1)
	payload := &Payload{
		Body:         body,
		Try:          0,
		ResponseChan: ch,
	}

	wp.jobch <- payload

	resp := <-ch
	return resp.Body, resp.Err
}

// Feed payload to worker
func (wp *WorkerPool) Feed(body interface{}) {
	if wp.MaxQueue > 0 {
		wp.feedSync(body)
	} else {
		wp.feedAsync(body)
	}
}

func (wp *WorkerPool) feedAsync(body interface{}) {
	wp.Add(1)

	wp.jobmu.Lock()
	defer wp.jobmu.Unlock()
	wp.jobq.PushBack(body)
}

func (wp *WorkerPool) feedSync(body interface{}) {
	wp.Add(1)
	payload := &Payload{
		Body: body,
		Try:  0,
	}

	wp.jobch <- payload
}

func (wp *WorkerPool) feedRoutine() {
	for {
		if wp.isStopped() {
			close(wp.jobch)
			return
		}

		wp.jobmu.Lock()
		n := wp.jobq.Len()
		wp.jobmu.Unlock()

		if n == 0 {
			time.Sleep(1 * time.Millisecond)
			continue
		}

		wp.jobmu.Lock()
		e := wp.jobq.Front()
		body := e.Value
		wp.jobmu.Unlock()

		payload := &Payload{
			Body: body,
			Try:  0,
		}

		wp.jobch <- payload

		wp.jobmu.Lock()
		wp.jobq.Remove(e)
		wp.jobmu.Unlock()
	}
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
		body, err = wp.Job(p.Body)
		t := time.Since(begin)
		wp.tick(t)

		if body != nil || (err != nil && p.Try == wp.Retry) {
			r := Response{
				Body: body,
				Err:  err,
			}
			if p.ResponseChan != nil {
				p.ResponseChan <- r
			} else {
				wp.pushResponse(r)
			}
		}

		// maybe retry
		if err != nil && p.Try < wp.Retry {
			p.Try++
			wp.retry(p)
		} else {
			wp.Done()
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

func (wp *WorkerPool) tick(d time.Duration) {
	wp.mu.Lock()
	wp.ops[wp.SizePercentil[wp.sizeindex]]++
	wp.avg_sum[wp.SizePercentil[wp.sizeindex]] += float64(d.Milliseconds())
	wp.mu.Unlock()
}

func (wp *WorkerPool) velocityRoutine() {
	for {
		if wp.isStopped() {
			return
		}

		wp.mu.Lock()
		wp.ops[wp.SizePercentil[wp.sizeindex]] = 0
		wp.avg_sum[wp.SizePercentil[wp.sizeindex]] = 0
		wp.mu.Unlock()

		time.Sleep(time.Duration(wp.EvaluationTime) * time.Second)

		i := wp.index()

		if wp.quorum != nil {
			wp.MaxWorker = wp.quorum.MaxInstanceWorker()
			wp.setsize(i)
		}

		wp.mu.Lock()
		wp.velocity[wp.SizePercentil[i]] = float64(wp.ops[wp.SizePercentil[i]]) / float64(wp.EvaluationTime)
		wp.mu.Unlock()

		wp.mu.RLock()
		// always increase from first value
		if i == 0 && i < len(wp.SizePercentil)-1 {
			wp.mu.RUnlock()
			wp.setsize(i + 1)
			continue
		}
		// if velocity is 0, decrease
		if i > 0 && wp.velocity[wp.SizePercentil[i]] == 0 {
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

	wp.sizeindex = i
	wp.wanted = int(wp.MaxWorker * wp.SizePercentil[i] / 100)
	if wp.wanted == 0 {
		wp.wanted++
	}

	log.Debugf("MaxWorker: %d, SizePercentil: %d, Wanted: %d, Active: %d\n", wp.MaxWorker, wp.SizePercentil[i], wp.wanted, wp.active)
	active := wp.active
	wanted := wp.wanted

	defer wp.mu.Unlock()

	if active < wanted {
		for i := active; i < wanted; i++ {
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

func (wp *WorkerPool) retry(payload *Payload) {
	go func() {
		wp.jobch <- payload
	}()
}
