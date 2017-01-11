# Goless - More jobs, for less!
Goless is a Go client for the [Qless job queueing system](https://github.com/seomoz/qless).

## Build and run the worker

```bash
cd cmd/goless/
go build
./goless
```

## Schedule a job

```bash
cd cmd/scheduler
go build
./scheduler
```

# Terminate a job

```bash
cd cmd/stopjob
go build
./stopjob <job-uuid>
```
