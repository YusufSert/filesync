FROM golang:1.23 AS build-stage

WORKDIR /usr/src/app

COPY go.mod go.sum ./
RUN go mod download

COPY ./ ./


RUN CGO_ENABLED=0 GOOS=linux go build -v -o ./app ./cmd/linuxservice

# Run the tests in the container
# FROM build-stage AS run-test-stage
#RUN go test -v ./..


# Deploy the applitcation binary into a lean image
FROM gcr.io/distroless/base-debian11 AS build-release-stage

WORKDIR /

COPY --from=build-stage /usr/src/app/app /app

EXPOSE 8080

USER nonroot:nonroot

# when ever you run the container this will execute
ENTRYPOINT ["/app"]
#path starts from root, config.yml == /config.yml no ./config.yml
CMD ["-config.path=config.yml"]


