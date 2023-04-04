FROM golang:latest as build
ADD . /brimming
WORKDIR /brimming
RUN make build

FROM scratch

COPY --from=build /brimming/brimming /bin/brimming

ENTRYPOINT  [ "/bin/brimming" ]