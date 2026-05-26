# Dockerfile для эмулятора счётчиков
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Копируем файлы модулей
COPY go.mod ./
RUN go mod download

# Копируем исходный код
COPY . .

# Собираем приложение
RUN go build -o meter-emulator .

# Финальный образ
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

# Копируем бинарник из builder
COPY --from=builder /app/meter-emulator .

# Открываем порт
EXPOSE 8080

# Запускаем эмулятор
CMD ["./meter-emulator"]