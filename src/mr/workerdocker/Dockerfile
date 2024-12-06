FROM golang:1.20-alpine AS build

ARG server_ip
ARG server_port

RUN apk update
RUN apk add bash
RUN apk add git

WORKDIR /app 

RUN git clone git@github.com:Happends/tda596_lab2.git

WORKDIR /app/tda596_lab2/src/main

RUN rm mr-out*

RUN build -buildmode=plugin -race ../mrapps/wc.go

CMD ["go", "run", "-race", "mrworker.go", "wc.so"]

