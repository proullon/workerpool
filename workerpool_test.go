package workerpool

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

type Job struct {
}

const e = 5

func (j *Job) execute(p interface{}) (interface{}, error) {
	payload := p.(int)
	f := func(p int) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return p * 2, nil
	}
	return f(payload)
}

func (j *Job) slow(p interface{}) (interface{}, error) {
	time.Sleep(3 * time.Second)
	return nil, nil
}

func (j *Job) unreliable(p interface{}) (interface{}, error) {
	n := rand.Intn(100)
	if n%2 == 0 {
		return nil, fmt.Errorf("fail hehe")
	}
	return nil, nil
}

func TestWorkerPool(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	job := &Job{}

	wp, err := New(job.execute,
		WithMaxWorker(1000),
		WithEvaluationTime(1),
	)

	if err != nil {
		t.Errorf(err.Error())
	}

	for i := 0; i < 100000; i++ {
		wp.Feed(i)
	}

	// wait for completion
	wp.Wait()
	// Wait again, should not deadlock
	wp.Wait()
	// stop workerpool
	wp.Stop()

	// check velocity
	var previous float64
	v := wp.VelocityValues()
	t.Logf("Velocity:\n")
	for i := 1; i <= 100; i++ {
		velocity, ok := v[i]
		if !ok {
			continue
		}
		t.Logf("%d%% -> %f op/s (avg: %.2fms)", i, velocity.Ops, velocity.Avg)
		if velocity.Ops < previous {
			t.Errorf("Expected velocity to increase steadily, got %f with previous %f", velocity.Ops, previous)
		}
		previous = velocity.Ops
		if velocity.Avg < 100-e || velocity.Avg > 100+e {
			t.Errorf("Expected average to be 100ms got %.2fms", velocity.Avg)
		}
	}

	// check current velocity
	percentil, ops, avg := wp.CurrentVelocityValues()
	t.Logf("Current velocity: %d%% -> %f op/s (avg: %.2fms)\n", percentil, ops, avg)
	if percentil != 100 {
		t.Errorf("Expected use of full size, got %d%%", percentil)
	}
	if ops < 9500 {
		t.Errorf("Expected around 10000 op/s with 1000 worker doing 10 op/s each, got %f", ops)
	}

}

func TestResponses(t *testing.T) {
	taskcount := 15

	job := &Job{}
	wp, err := New(job.execute,
		WithMaxWorker(10),
		WithEvaluationTime(1),
	)

	if err != nil {
		t.Errorf(err.Error())
	}

	for i := 0; i < taskcount; i++ {
		wp.Feed(i)
	}

	wp.Wait()

	n := wp.AvailableResponses()
	fmt.Printf("Available responses: %d\n", n)
	if n != taskcount {
		t.Errorf("Expected %d responses, got %d", taskcount, n)
	}

	wp.Stop()

	// read all responses
	var count int
	for r := range wp.ReturnChannel {
		count++
		if r.Err != nil {
			t.Errorf("Expected all errors to be nil")
		}
	}

	if count != taskcount {
		t.Errorf("Expected response count to be %d, got %d", taskcount, count)
	}

	n = wp.AvailableResponses()
	fmt.Printf("Available responses: %d\n", n)
	if n != 0 {
		t.Errorf("Expected 0 responses, got %d", n)
	}
}

func TestAllIn(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	job := &Job{}
	wp, err := New(job.execute,
		WithMaxWorker(1000),
		WithEvaluationTime(1),
		WithSizePercentil(AllInSizesPercentil),
	)

	if err != nil {
		t.Errorf(err.Error())
	}

	for i := 0; i < 100000; i++ {
		wp.Feed(i)
	}

	// wait for completion
	wp.Wait()
	// stop workerpool
	wp.Stop()

	// check current velocity
	percentil, ops, avg := wp.CurrentVelocityValues()
	t.Logf("Current velocity: %d%% -> %f op/s (avg: %.2fms)\n", percentil, ops, avg)
	if percentil != 100 {
		t.Errorf("Expected use of full size, got %d%%", percentil)
	}
	if ops < 9500 {
		t.Errorf("Expected around 10000 op/s with 1000 worker doing 10 op/s each, got %f", ops)
	}
}

func TestSlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	job := &Job{}

	wp, err := New(job.slow,
		WithMaxWorker(100),
		WithEvaluationTime(5),
		WithSizePercentil(AllSizesPercentil),
	)

	if err != nil {
		t.Errorf(err.Error())
	}

	for i := 0; i < 40; i++ {
		wp.Feed(i)
	}

	// wait for completion
	wp.Wait()
	// stop workerpool
	wp.Stop()

	// check velocity
	var previous float64
	v := wp.VelocityValues()
	t.Logf("Velocity:\n")
	for i := 1; i <= 100; i++ {
		velocity, ok := v[i]
		if !ok {
			continue
		}
		t.Logf("%d%% -> %f op/s (avg: %.2fms)", i, velocity.Ops, velocity.Avg)
		if velocity.Ops < previous {
			t.Errorf("Expected velocity to increase steadily, got %f with previous %f", velocity.Ops, previous)
		}
		previous = velocity.Ops
		if velocity.Avg < 3000-e || velocity.Avg > 3000+e {
			t.Errorf("Expected average to be 3000ms got %.2fms", velocity.Avg)
		}
	}

}

/*
** Test retry feature with unreliable job (ie: network)
 */
func TestRetry(t *testing.T) {
	E := 30
	job := &Job{}

	wp, err := New(job.unreliable,
		WithRetry(0),
		WithMaxWorker(100),
		WithEvaluationTime(1),
		WithSizePercentil(LogSizesPercentil),
	)

	if err != nil {
		t.Errorf(err.Error())
	}

	n := 1000
	for i := 0; i < n; i++ {
		wp.Feed(i)
	}

	// wait for completion
	wp.Wait()
	// stop workerpool
	wp.Stop()

	// Without retry unreliable job with 50% failure, we should have n/2 errors
	nerrors := wp.AvailableResponses()
	if nerrors < (n/2)-E || nerrors > (n/2)+E {
		t.Errorf("WithRetry(0): Expected ~%d errors (50%% of %d), got %d", n/2, n, nerrors)
	}

	// now that control test is established, run with retry
	wp, err = New(job.unreliable,
		WithRetry(5),
		WithMaxWorker(100),
		WithEvaluationTime(1),
		WithSizePercentil(LogSizesPercentil),
	)

	if err != nil {
		t.Errorf(err.Error())
	}

	n = 1000
	for i := 0; i < n; i++ {
		wp.Feed(i)
	}

	// wait for completion
	wp.Wait()
	// stop workerpool
	wp.Stop()

	// With 3 possible retry, we should have close to 0 errors
	nerrors = wp.AvailableResponses()
	if nerrors > E {
		t.Errorf("WithRetry(5): Expected ~0 errors, got %d", nerrors)
	}

}

func TestMaxQueue(t *testing.T) {
	job := &Job{}

	wp, err := New(job.execute,
		WithMaxQueue(10),
	)

	if err != nil {
		t.Errorf(err.Error())
	}

	for i := 0; i < 100; i++ {
		wp.Feed(i)
	}

	// wait for completion
	wp.Wait()
	// stop workerpool
	wp.Stop()
}
