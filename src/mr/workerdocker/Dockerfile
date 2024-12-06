FROM golang:1.20-alpine AS build

RUN apk update
RUN apk add bash
RUN apk add git
RUN apk add gcc
RUN apk add musl-dev

WORKDIR /app 

RUN git clone git://g.csail.mit.edu/6.5840-golabs-2023 6.5840

WORKDIR /app/6.5840/src/mr

COPY coordinator.go .
COPY rpc.go .
COPY worker.go .

RUN echo $(ls -1 .)

WORKDIR /app/6.5840/src/main

RUN go build -buildmode=plugin ../mrapps/wc.go

CMD ["go", "run", "-race", "mrworker.go", "wc.so"]

