FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY main.go ./
RUN CGO_ENABLED=0 go build -o /server .

FROM alpine:3.20
COPY --from=build /server /server
EXPOSE 8080 9464
CMD ["/server"]
