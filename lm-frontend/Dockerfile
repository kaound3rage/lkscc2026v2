# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o lm-frontend .

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/lm-frontend .
COPY templates/ templates/

EXPOSE 3000

ENV PORT=3000

ENTRYPOINT ["./lm-frontend"]