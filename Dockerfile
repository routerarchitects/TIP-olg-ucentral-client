# ==============================================================================
# Multi-stage Dockerfile for ucentral-client
# ==============================================================================

# Stage 1: Build static Go binary
FROM golang:alpine AS builder

WORKDIR /src

ENV GOTOOLCHAIN=auto

# Download Go dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Compile statically-linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/ucentral-client ./cmd/ucentral-client

# Stage 2: Minimal Alpine runtime image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/ucentral-client /app/ucentral-client

ENTRYPOINT ["/app/ucentral-client"]
