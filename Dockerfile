# Build stage
FROM golang:1.26.1-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o cache-server ./cmd/server/main.go

# Run stage
FROM alpine:latest
RUN apk --no-cache add curl # add curl for convenient container debugging / health checking
WORKDIR /root/
COPY --from=builder /app/cache-server .
EXPOSE 8080
CMD ["./cache-server"]
