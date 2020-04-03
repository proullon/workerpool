# workerpool

Auto scaling generic worker pool. WorkerPool will adapt the number of goroutine to find the optimal velocity by recording operation per second for each specified pool percentil.

## Example

```go
func work(config *Config) error {
	wp, err := workerpool.New(config, genjobfnc)
	if err != nil {
	  return err
  }

	for i := 0; i < 100000; i++ {
		wp.Feed(i)
	}

	// wait for completion
	wp.Wait()

  return nil
}

func genjobfnc(c interface{}, p interface{}) (interface{}, error) {
  config := c.(*Config)
  payload := p.(int)
  return jobfnc(config, payload)
}

func jobfnc(c *Config, p int) (*Response, error) {
  // do stuff
  return resp, nil
}
```

## Options

```go
func work(config *Config) {
  wp, err := workerpool.New(config, jobfnc,
    workerpool.WithMaxWorker(1000),
    workerpool.WithSizePercentil(workerpool.LogSizesPercentil),
    workerpool.EvaluationTime(1),
    workerpool.MaxDuration(3 * time.Second),
  )
}
```

`MaxWorker` defines the maximum parallelisation possible, `SizePercentil` defines all the pool size possible by reference to `MaxWorker`.

With 1000 max worker and default size percentil array, possible values are:
  * 1% -> 10 workers
  * 10% -> 100 workers
  * 20% -> 200 workers
  * 30% -> 300 workers
  * 40% -> 400 workers
  * 50% -> 500 workers
  * 60% -> 600 workers
  * 70% -> 700 workers
  * 80% -> 800 workers
  * 90% -> 900 workers
  * 100% -> 1000 workers

WorkerPool will measure the velocity to find the most effective number of workers to achieve the best performance, increasing or decreasing the number of worker depending of the recorded velocity.

This means increasing the number of worker too find the highest op/s possible. It also means reducing the number of worker if the job takes longer at some point for some reason (network traffic, database load, etc) then increasing again as soon as possible. `EvaluationTime` defines the sampling period and the duration between WorkerPool size change. `MaxDuration` parameter ensures `WorkerPool` won't overload the client resource.

