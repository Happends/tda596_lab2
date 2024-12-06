

## To run worker

- change w.id in MakeWorker() in worker.go to "ip_address:port"
- change "ip:port" in coordinatorSock() in rpc.go


## To run Coordinator

- change "ip:port" in coordinatorSock() in rpc.go





## Docker building

```cd TDA596_lab2/6.5840/src/mr```

### Coordinator

```docker build --no-cache -f coordinatordocker/Dockerfile .```

### Worker

```docker build --no-cache -f workerdocker/Dockerfile .```

