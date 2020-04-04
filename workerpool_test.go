package workerpool

import (
	"fmt"
	"testing"
	"time"
)

type Config struct {
}

func testJob(c interface{}, p interface{}) (interface{}, error) {
	config := c.(*Config)
	payload := p.(int)
	f := func(c *Config, p int) (int, error) {
		time.Sleep(100 * time.Millisecond)
		return p * 2, nil
	}
	return f(config, payload)
}

func TestWorkerPool(t *testing.T) {

	wp, err := New(&Config{}, testJob,
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
	var previous int
	v := wp.VelocityValues()
	t.Logf("Velocity:\n")
	for i := 1; i <= 100; i++ {
		velocity, ok := v[i]
		if !ok {
			continue
		}
		t.Logf("%d%% -> %d op/s", i, velocity)
		if velocity < previous {
			t.Errorf("Expected velocity to increase steadily, got %d with previous %d", velocity, previous)
		}
		previous = velocity
	}

	// check current velocity
	percentil, ops := wp.CurrentVelocityValues()
	t.Logf("Current velocity: %d%% -> %d op/s\n", percentil, ops)
	if percentil != 100 {
		t.Errorf("Expected use of full size, got %d%%", percentil)
	}
	if ops != 10000 {
		t.Errorf("Expected 10000 op/s with 1000 worker doing 10 op/s each, got %d", ops)
	}

}

func TestResponses(t *testing.T) {
	wp, err := New(&Config{}, testJob,
		WithMaxWorker(10),
		WithEvaluationTime(1),
	)

	if err != nil {
		t.Errorf(err.Error())
	}

	for i := 0; i < 100; i++ {
		wp.Feed(i)
	}

	wp.Wait()

	n := wp.AvailableResponses()
	fmt.Printf("Available responses: %d\n", n)
	if n != 100 {
		t.Errorf("Expected 100 responses, got %d", n)
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

	if count != 100 {
		t.Errorf("Expected response count to be 100, got %d", count)
	}

	n = wp.AvailableResponses()
	fmt.Printf("Available responses: %d\n", n)
	if n != 0 {
		t.Errorf("Expected 0 responses, got %d", n)
	}
}
