package workerpool

import (
	"testing"
	"time"
)

func TestQuorum(t *testing.T) {

	job := &Job{}

	wp, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(10),
		WithMaxQueue(100000),
		WithEvaluationTime(1),
		WithQuorum("1", "127.0.0.1:8090", "", []string{"127.0.0.1:8091", "127.0.0.1:8092"}, 1000, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp2, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(10),
		WithMaxQueue(100000),
		WithEvaluationTime(1),
		WithQuorum("2", "127.0.0.1:8091", "", []string{"127.0.0.1:8090", "127.0.0.1:8092"}, 1000, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp3, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(10),
		WithMaxQueue(100000),
		WithEvaluationTime(1),
		WithQuorum("3", "127.0.0.1:8092", "", []string{"127.0.0.1:8090", "127.0.0.1:8091"}, 1000, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	for i := 0; i < 100; i++ {
		wp.Feed(i)
	}

	// wait for completion
	wp.Wait()

	if c := wp.quorum.InstanceCount(); c != 3 {
		t.Errorf("Expected wp1 to have 3 instances in quorum, got %d", c)
	}
	if c := wp2.quorum.InstanceCount(); c != 3 {
		t.Errorf("Expected wp2 to have 3 instances in quorum, got %d", c)
	}
	if c := wp3.quorum.InstanceCount(); c != 3 {
		t.Errorf("Expected wp3 to have 3 instances in quorum, got %d", c)
	}

	wp4, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(10),
		WithMaxQueue(100000),
		WithEvaluationTime(1),
		WithQuorum("4", "127.0.0.1:8094", "", []string{"127.0.0.1:8090", "127.0.0.1:8091", "127.0.0.1:8092"}, 1000, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	for i := 0; i < 100; i++ {
		wp4.Feed(i)
	}

	wp4.Wait()

	if c := wp.quorum.InstanceCount(); c != 4 {
		t.Errorf("Expected wp1 to have 4 instances in quorum, got %d", c)
	}
	if c := wp2.quorum.InstanceCount(); c != 4 {
		t.Errorf("Expected wp2 to have 4 instances in quorum, got %d", c)
	}
	if c := wp3.quorum.InstanceCount(); c != 4 {
		t.Errorf("Expected wp3 to have 4 instances in quorum, got %d", c)
	}
	if c := wp4.quorum.InstanceCount(); c != 4 {
		t.Errorf("Expected wp4 to have 4 instances in quorum, got %d", c)
	}

	// stop workerpool
	wp.Stop()

	time.Sleep(500 * time.Millisecond)

	if c := wp2.quorum.InstanceCount(); c != 3 {
		t.Errorf("Expected wp2 to have 3 instances in quorum, got %d", c)
	}
	if c := wp3.quorum.InstanceCount(); c != 3 {
		t.Errorf("Expected wp3 to have 3 instances in quorum, got %d", c)
	}
	if c := wp4.quorum.InstanceCount(); c != 3 {
		t.Errorf("Expected wp4 to have 3 instances in quorum, got %d", c)
	}

	wp2.Stop()
	wp3.Stop()
	wp4.Stop()
}

func TestQuorumSharedRateLimit(t *testing.T) {
	job := &Job{}
	localMax := 20
	globalMax := 30

	wp, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(100000),
		WithEvaluationTime(1),
		WithQuorum("1", "127.0.0.1:8090", "", []string{"127.0.0.1:8091", "127.0.0.1:8092"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp2, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(100000),
		WithEvaluationTime(1),
		WithQuorum("2", "127.0.0.1:8091", "", []string{"127.0.0.1:8090", "127.0.0.1:8092"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp3, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(100000),
		WithEvaluationTime(1),
		WithQuorum("3", "127.0.0.1:8092", "", []string{"127.0.0.1:8090", "127.0.0.1:8091"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	for i := 0; i < 500; i++ {
		wp.Feed(i)
		wp2.Feed(i)
		wp3.Feed(i)
	}

	// wait for completion
	wp.Wait()

	if c := wp.quorum.MaxInstanceWorker(); c != globalMax/3 {
		t.Errorf("Expected wp1 to have MaxInstanceWorker to %d, got %d", globalMax/3, c)
	}
	if c := wp2.quorum.MaxInstanceWorker(); c != globalMax/3 {
		t.Errorf("Expected wp2 to have MaxInstanceWorker to %d, got %d", globalMax/3, c)
	}
	if c := wp3.quorum.MaxInstanceWorker(); c != globalMax/3 {
		t.Errorf("Expected wp3 to have MaxInstanceWorker to %d, got %d", globalMax/3, c)
	}

	if a := wp.Active(); a != globalMax/3 {
		t.Errorf("Expected wp1 to have %d active goroutines, got %d", globalMax/3, a)
	}
	if a := wp2.Active(); a != globalMax/3 {
		t.Errorf("Expected wp2 to have %d active goroutines, got %d", globalMax/3, a)
	}
	if a := wp3.Active(); a != globalMax/3 {
		t.Errorf("Expected wp3 to have %d active goroutines, got %d", globalMax/3, a)
	}

	wp2.Stop()
	wp3.Stop()

	for i := 0; i < 500; i++ {
		wp.Feed(i)
	}
	wp.Wait()

	if c := wp.quorum.MaxInstanceWorker(); c != localMax {
		t.Errorf("Expected wp1 to have MaxInstanceWorker to %d, got %d", localMax, c)
	}
	if a := wp.Active(); a != localMax {
		t.Errorf("Expected wp1 to have %d active goroutines, got %d", localMax, a)
	}

	wp.Stop()
}
