FROM golang:1.22.1 AS builder


WORKDIR /app


COPY go.mod go.sum ./
RUN go mod download


COPY /outputs ./outputs

COPY ./main.go .
COPY ./service.cert .
COPY ./service.key .
COPY ./ca.pem .

CMD ["go", "run", "."]