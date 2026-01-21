FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /kinesis ./cmd/kinesis

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /kinesis /kinesis
EXPOSE 8080
ENTRYPOINT ["/kinesis"]
CMD ["api"]
