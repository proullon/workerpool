package workerpool

import (
	"math"
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

func TestQuorumSimple(t *testing.T) {

	job := &Job{}

	wp, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(5),
		WithMaxQueue(100000),
		WithEvaluationTime(1),
		WithQuorum("1", "127.0.0.1:8090", "", []string{"127.0.0.1:8091", "127.0.0.1:8092"}, 1000, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp2, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(5),
		WithMaxQueue(100000),
		WithEvaluationTime(1),
		WithQuorum("2", "127.0.0.1:8091", "", []string{"127.0.0.1:8090", "127.0.0.1:8092"}, 1000, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp3, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(5),
		WithMaxQueue(100000),
		WithEvaluationTime(1),
		WithQuorum("3", "127.0.0.1:8092", "", []string{"127.0.0.1:8090", "127.0.0.1:8091"}, 1000, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	time.Sleep(2 * time.Second)

	wp.Stop()
	wp2.Stop()
	wp3.Stop()
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

func TestSoloQuorum(t *testing.T) {
	job := &Job{}
	localMax := 5
	globalMax := 30
	maxQueue := 10 * 1000
	eps := 3.0

	wp, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(1),
		WithQuorum("1", "127.0.0.1:8090", "", []string{"127.0.0.1:8091", "127.0.0.1:8092", "127.0.0.1:8094"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	for i := 0; i < 50; i++ {
		wp.Feed(i)
	}

	// wait for completion
	wp.Wait()

	_, v, a := wp.CurrentVelocityValues()
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp.quorum.Name, a)
	}
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity should not be 0", wp.quorum.Name)
	}

	wp.Stop()
}

func TestQuorumSharedTasksSolo(t *testing.T) {
	job := &Job{}
	localMax := 1
	globalMax := 30
	maxQueue := 2 * 1000
	eps := 3.0

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	wp, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(5),
		WithQuorum("1", "127.0.0.1:8090", "", []string{"127.0.0.1:8091", "127.0.0.1:8092", "127.0.0.1:8094", "127.0.0.1:8095"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	begin := time.Now()
	for i := 0; i < maxQueue/10; i++ {
		wp.Feed(i)
	}
	// wait for completion
	wp.Wait()

	// All 3 should have worked
	// should be at least twice as fast
	if time.Since(begin) > time.Duration(100*maxQueue/10/(localMax)+500)*time.Millisecond {
		t.Errorf("Should have taken less than %s, got %s", time.Duration(100*maxQueue/10/(localMax))*time.Millisecond, time.Since(begin))
	}

	_, v, a := wp.CurrentVelocityValues()
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity (%f) should not be nil", wp.quorum.Name, v)
	}
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp.quorum.Name, a)
	}

	// Test GlobalVelocityValues method
	gv, iv := wp.GlobalVelocityValues()
	if gv == nil || iv == nil {
		t.Errorf("global velocity values should not be nil")
		return
	}
	if len(iv) != 1 {
		t.Errorf("expected 1 instance in quorum velocity, got %d", len(iv))
	}

	wp.Stop()
}

func TestQuorumSharedTasksDuo(t *testing.T) {
	job := &Job{}
	localMax := 1
	globalMax := 30
	maxQueue := 2 * 1000
	eps := 3.0

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	wp, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(5),
		WithQuorum("1", "127.0.0.1:8090", "", []string{"127.0.0.1:8091", "127.0.0.1:8092", "127.0.0.1:8094", "127.0.0.1:8095"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp2, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(5),
		WithQuorum("2", "127.0.0.1:8091", "", []string{"127.0.0.1:8090", "127.0.0.1:8092", "127.0.0.1:8094", "127.0.0.1:8095"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	time.Sleep(1000 * time.Millisecond)
	begin := time.Now()
	for i := 0; i < maxQueue/10; i++ {
		wp.Feed(i)
	}
	// wait for completion
	wp.Wait()

	// All 2 should have worked
	// should be at least twice as fast
	if time.Since(begin) > time.Duration(100*maxQueue/10/(localMax*2)+500)*time.Millisecond {
		t.Errorf("Should have taken less than %s, got %s", time.Duration(100*maxQueue/10/(localMax*2))*time.Millisecond, time.Since(begin))
	}

	p, v, a := wp.CurrentVelocityValues()
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity (%f) for %d%% should not be nil", wp.quorum.Name, v, p)
	}
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp.quorum.Name, a)
	}
	p, v, a = wp2.CurrentVelocityValues()
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity (%f) for %d%% should not be nil", wp2.quorum.Name, v, p)
	}
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp2.quorum.Name, a)
	}

	// Test GlobalVelocityValues method
	gv, iv := wp.GlobalVelocityValues()
	if gv == nil || iv == nil {
		t.Errorf("global velocity values should not be nil")
		return
	}
	if len(iv) != 2 {
		t.Errorf("expected 2 instance in quorum velocity, got %d", len(iv))
	}

	wp.Stop()
	wp2.Stop()
}

func TestQuorumSharedTasksTrio(t *testing.T) {
	job := &Job{}
	localMax := 1
	globalMax := 30
	maxQueue := 2 * 1000
	eps := 3.0

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	wp, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(5),
		WithQuorum("1", "127.0.0.1:8090", "", []string{"127.0.0.1:8091", "127.0.0.1:8092", "127.0.0.1:8093", "127.0.0.1:8094"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp2, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(5),
		WithQuorum("2", "127.0.0.1:8091", "", []string{"127.0.0.1:8090", "127.0.0.1:8092", "127.0.0.1:8093", "127.0.0.1:8094"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp3, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(5),
		WithQuorum("3", "127.0.0.1:8092", "", []string{"127.0.0.1:8090", "127.0.0.1:8091", "127.0.0.1:8093", "127.0.0.1:8094"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	time.Sleep(1000 * time.Millisecond)
	begin := time.Now()
	for i := 0; i < maxQueue/10; i++ {
		wp.Feed(i)
	}
	// wait for completion
	wp.Wait()

	// All 3 should have worked
	// should be at least twice as fast
	if time.Since(begin)-1*time.Second > time.Duration(10*maxQueue/(localMax*3))*time.Millisecond {
		t.Errorf("Should have taken less than %s, got %s", time.Duration(10*maxQueue/(localMax*3))*time.Millisecond, time.Since(begin))
	}

	_, v, a := wp.CurrentVelocityValues()
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity should not be nil", wp.quorum.Name)
	}
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp.quorum.Name, a)
	}
	_, v, a = wp2.CurrentVelocityValues()
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity should not be nil", wp2.quorum.Name)
	}
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp2.quorum.Name, a)
	}
	_, v, a = wp3.CurrentVelocityValues()
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity should not be nil", wp3.quorum.Name)
	}
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp3.quorum.Name, a)
	}

	// Test GlobalVelocityValues method
	gv, iv := wp.GlobalVelocityValues()
	if gv == nil || iv == nil {
		t.Errorf("global velocity values should not be nil")
		return
	}

	wp.Stop()
	wp2.Stop()
	wp3.Stop()
}

func TestQuorumSharedTasksQuatuor(t *testing.T) {
	job := &Job{}
	localMax := 2
	globalMax := 30
	maxQueue := 10 * 1000
	eps := 3.0

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	wp, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(5),
		WithQuorum("1", "127.0.0.1:8090", "", []string{"127.0.0.1:8091", "127.0.0.1:8092", "127.0.0.1:8093", "127.0.0.1:8094"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp2, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(5),
		WithQuorum("2", "127.0.0.1:8091", "", []string{"127.0.0.1:8090", "127.0.0.1:8092", "127.0.0.1:8093", "127.0.0.1:8094"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	wp3, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(5),
		WithQuorum("3", "127.0.0.1:8092", "", []string{"127.0.0.1:8090", "127.0.0.1:8091", "127.0.0.1:8093", "127.0.0.1:8094"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}
	wp4, err := New(job.execute,
		WithSizePercentil(AllInSizesPercentil),
		WithMaxWorker(localMax),
		WithMaxQueue(maxQueue),
		WithEvaluationTime(5),
		WithQuorum("4", "127.0.0.1:8093", "", []string{"127.0.0.1:8090", "127.0.0.1:8091", "127.0.0.1:8092", "127.0.0.1:8093"}, globalMax, 100),
	)
	if err != nil {
		t.Errorf(err.Error())
	}

	time.Sleep(1000 * time.Millisecond)
	begin := time.Now()
	for i := 0; i < maxQueue/10; i++ {
		wp.Feed(i)
	}
	// wait for completion
	wp.Wait()

	// All 4 should have worked
	// should be at least 3 times as fast as fast
	if time.Since(begin) > time.Duration(10*maxQueue/(localMax*3))*time.Millisecond {
		t.Errorf("Should have taken less than %s, got %s", time.Duration(10*maxQueue/(localMax*2))*time.Millisecond, time.Since(begin))
	}

	_, v, a := wp.CurrentVelocityValues()
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity should not be nil", wp.quorum.Name)
	}
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp.quorum.Name, a)
	}
	_, v, a = wp2.CurrentVelocityValues()
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity should not be nil", wp2.quorum.Name)
	}
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp2.quorum.Name, a)
	}
	_, v, a = wp3.CurrentVelocityValues()
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity should not be nil", wp3.quorum.Name)
	}
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp3.quorum.Name, a)
	}
	_, v, a = wp4.CurrentVelocityValues()
	if math.Abs(v) < eps {
		t.Errorf("Workerpool %s velocity should not be nil", wp4.quorum.Name)
	}
	if math.Abs(a-100) > eps {
		t.Errorf("Workerpool %s average should be 100, got %.2f", wp4.quorum.Name, a)
	}

	// Test GlobalVelocityValues method
	gv, iv := wp.GlobalVelocityValues()
	if gv == nil || iv == nil {
		t.Errorf("global velocity values should not be nil")
		return
	}

	wp.Stop()
	wp2.Stop()
	wp3.Stop()
	wp4.Stop()
}
