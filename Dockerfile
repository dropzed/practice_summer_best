FROM golang:1.22-alpine AS builder

WORKDIR /app

# Копирование описания зависимостей
COPY go.mod ./

# Копирование исходного кода
COPY pkg/ ./pkg/
COPY cmd/ ./cmd/

# Сборка бинарных файлов
RUN go build -o manager cmd/manager/main.go
RUN go build -o broker cmd/broker/main.go
RUN go build -o demo cmd/demo/main.go

# Финальный образ
FROM alpine:latest

WORKDIR /app

# Установка bash и curl для тестирования
RUN apk add --no-cache bash curl

COPY --from=builder /app/manager /app/manager
COPY --from=builder /app/broker /app/broker
COPY --from=builder /app/demo /app/demo

EXPOSE 8080 8081 8082
