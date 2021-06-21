FROM golang:alpine as build
RUN apk --no-cache add build-base git
ADD . /app
WORKDIR /app
RUN make build

FROM golang:alpine

COPY --from=build /app/brimming /bin/brimming

ENTRYPOINT  [ "/bin/brimming" ]