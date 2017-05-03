FROM elek/librdkafka
RUN apk add --update --no-cache go
RUN mkdir -p /opt/go/src
ENV GOPATH=/opt/go
ADD . /opt/go/src/app
WORKDIR /opt/go/src/app
RUN go get
RUN go install
ENTRYPOINT ["/opt/go/bin/app"]
