FROM alpine:latest
# Install base packages, set timezone
RUN apk update && apk add curl iproute2 drill bash tree tzdata
ENV TZ Asia/Shanghai
# when use https, resolve Go’s x509 error
ADD conf/ca-certificates.crt /etc/ssl/certs/
ADD app /
ADD conf /conf
EXPOSE 3000
CMD ["/app"]
