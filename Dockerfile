
FROM alpine:latest
ADD conf/ca-certificates.crt /etc/ssl/certs/  # when use https, resolve Go’s x509 error
ADD app /
ADD conf /conf
EXPOSE 3000
CMD ["/app"]
