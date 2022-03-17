package main

import (
	"fmt"
	"time"

	"github.com/proullon/workerpool"
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

func main() {

	job := &Job{}
	localMax := 5
	globalMax := 30
	maxQueue := 10 * 1000

	wp, err := workerpool.New(job.execute,
		workerpool.WithSizePercentil(workerpool.AllInSizesPercentil),
		workerpool.WithMaxWorker(localMax),
		workerpool.WithMaxQueue(maxQueue),
		workerpool.WithEvaluationTime(1),
		workerpool.WithQuorum("4", "127.0.0.1:8094", "", []string{"127.0.0.1:8090", "127.0.0.1:8091", "127.0.0.1:8092", "127.0.0.1:8095"}, globalMax, 100),
	)
	if err != nil {
		fmt.Errorf("Cannot setup workerpool: %s\n", err)
		return
	}

	wp2, err := workerpool.New(job.execute,
		workerpool.WithSizePercentil(workerpool.AllInSizesPercentil),
		workerpool.WithMaxWorker(localMax),
		workerpool.WithMaxQueue(maxQueue),
		workerpool.WithEvaluationTime(1),
		workerpool.WithQuorum("5", "127.0.0.1:8095", "", []string{"127.0.0.1:8090", "127.0.0.1:8091", "127.0.0.1:8092", "127.0.0.1:8094"}, globalMax, 100),
	)
	if err != nil {
		fmt.Errorf("Cannot setup workerpool: %s\n", err)
		return
	}

	_ = wp
	_ = wp2
	for {
		time.Sleep(1 * time.Minute)
	}
}
