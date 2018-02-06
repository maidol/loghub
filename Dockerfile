
FROM alpine:latest
ADD app /
ADD conf /conf
EXPOSE 3000
CMD ["/app"]
