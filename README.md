# Goless - More jobs, for less!
Goless is a Go client for [Qless](https://github.com/seomoz/qless).

## Start the redis server

```bash
redis-server --port 6380 &
```

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

## Terminate a job

```bash
cd cmd/stopjob
go build
./stopjob <job-uuid>
```
