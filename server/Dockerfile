FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY main.go ./
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -o /server . && CGO_ENABLED=0 go build -o /worker ./cmd/worker

FROM alpine:3.20
COPY --from=build /server /server
COPY --from=build /worker /worker
EXPOSE 8080 9464
CMD ["/server"]
