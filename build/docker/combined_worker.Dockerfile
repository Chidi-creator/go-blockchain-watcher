FROM golang:1.20 as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o combined_worker ./cmd/workers/combined

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/combined_worker .

# Set default concurrency values
ENV EVM_CONCURRENCY=5
ENV CHANGENOW_CONCURRENCY=2

CMD ["./combined_worker"] 