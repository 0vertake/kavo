# Build and run kavod. Nodes are symmetric, so this one image is the whole
# cluster — what a node does is decided entirely by its flags.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /kavod ./cmd/kavod

# Alpine rather than scratch: a dev cluster is worth having a shell in, and the
# healthcheck needs something to make a request with.
FROM alpine:3.22
RUN adduser -D -u 10001 kavo && mkdir /data && chown kavo /data
COPY --from=build /kavod /usr/local/bin/kavod
USER kavo
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["kavod"]
