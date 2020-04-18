package workerpool

import (
	"fmt"
	"testing"
	"time"
)

type Job struct {
}

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
		t.Logf("%d%% -> %f op/s", i, velocity)
		if velocity < previous {
			t.Errorf("Expected velocity to increase steadily, got %f with previous %f", velocity, previous)
		}
		previous = velocity
	}

	// check current velocity
	percentil, ops := wp.CurrentVelocityValues()
	t.Logf("Current velocity: %d%% -> %f op/s\n", percentil, ops)
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
	percentil, ops := wp.CurrentVelocityValues()
	t.Logf("Current velocity: %d%% -> %f op/s\n", percentil, ops)
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
		t.Logf("%d%% -> %f op/s", i, velocity)
		if velocity < previous {
			t.Errorf("Expected velocity to increase steadily, got %f with previous %f", velocity, previous)
		}
		previous = velocity
	}

}
