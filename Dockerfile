FROM golang:1.23 AS builder

WORKDIR /app
COPY . .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=windows go build -o etl_app .

FROM alpine:latest

WORKDIR /transform_service
COPY --from=builder /transform_service/etl_app .

EXPOSE 8080
CMD ["./etl_app"]