# Goless - More jobs, for less!

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
