FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/lb ./cmd/lb && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/backend ./cmd/backend

FROM alpine:3.23
RUN apk add --no-cache ca-certificates
COPY --from=build /out/lb /out/backend /usr/local/bin/
USER 10001:10001
EXPOSE 8080 9090
ENTRYPOINT ["lb"]
