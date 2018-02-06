
FROM alpine:latest
# when use https, resolve Go’s x509 error
ADD conf/ca-certificates.crt /etc/ssl/certs/
ADD app /
ADD conf /conf
EXPOSE 3000
CMD ["/app"]
