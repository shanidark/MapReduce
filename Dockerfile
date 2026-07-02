FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o mapreduce .

FROM alpine:3.20
COPY --from=builder /build/mapreduce /usr/local/bin/mapreduce
WORKDIR /workdir
ENTRYPOINT ["/usr/local/bin/mapreduce"]
