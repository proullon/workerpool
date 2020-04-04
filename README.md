# workerpool

Auto scaling generic worker pool. WorkerPool will adapt the number of goroutine to find the optimal velocity by recording operation per second for each specified pool percentil.

## Example

```go
func work(config *Config) error {
  wp, err := workerpool.New(config, jobfnc)
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

func jobfnc(c interface{}, p interface{}) (interface{}, error) {
  config := c.(*Config)
  payload := p.(int)
  f := func(c *Config, p int) (int, error) {
    // do stuff
    var id int
    err := config.db.Exec(`INSERT into example (value, event_date) ($1, NOW()) RETURNING id`, p).Scan(&id)
    if err != nil {
      return nil, err
    }
    return id, nil
  }
  return f(config, payload)
}
```

Example behavior with network and database operation:
```
Velocity:
1% : 10op/s
2% : 21op/s
3% : 31op/s
4% : 43op/s
5% : 50op/s
6% : 55op/s
7% : 66op/s
8% : 80op/s
9% : 84op/s
10% : 85op/s
11% : 92op/s
12% : 95op/s
13% : 101op/s
14% : 103op/s
15% : 108op/s
16% : 141op/s
17% : 163op/s
18% : 143op/s
19% : 145op/s
20% : 145op/s
21% : 119op/s
Current velocity: 17% -> 163 op/s
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

## Automatic scaling mode

Scaling is affected by `MaxWorker`, `MaxDuration` and scaling pattern array `SizePercentil`. WorkerPool computes possible number ofworkers using `SizePercentil` of `MaxWorker`.

`MaxDuration` defines the maximum duration wanted to execute a job. If a job takes longer than `MaxDuration`, worker will exit, affecting down current velocity.

One can defines specific `SizePercentil` pattern using `WithSizePercentil` functionnal option to `New`.
Predefined pattern are:
  * DefaultSizesPercentil: Regular increase ten by ten from 1 to 100. Allows fast scaling.
  * AllSizesPercentil: All percentils from 1 to 100. it allows WorkerPool to find the perfect sizing fo optimal velocity. Only worth for long running operation.
  * LogSizesPercentil: Logarithmic distribution from 1 to 100. Perfect for job targeting client sensible to load.
  * AllInSizesPercentil: Only 100% of workerpool, meaning WorkerPool will always use MaxWorker goroutines.

## Response channel

Worker response are stored in an internal list. This allows user to read responses whenever ready without impacting worker, they will not lock.

Call to `Stop()` will close `ReturnChannel` once all stored responses have been read.

```go
func Work() {
  wp, _ := New(nil, testJob, workerpool.WithMaxWorker(10), workerpool.WithEvaluationTime(1))

  for i := 0; i < 100; i++ {
    wp.Feed(i)
  }

  wp.Wait()

  n := wp.AvailableResponses() // n = 100

  wp.Stop()
	
  // read all responses
  var count int
  for r := range wp.ReturnChannel {
    count++
    if r.Err != nil {
      panic("Expected all errors to be nil")
    }
  }

  // count = 100
  n = wp.AvailableResponses() // n = 0
}
```


