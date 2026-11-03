# Garage (S3-compatible object store) plus a shell, so the entrypoint can bootstrap
# the single-node layout, access key and buckets on first start.
FROM dxflrs/garage:v2.1.0 AS garage

FROM alpine:3.20
COPY --from=garage /garage /usr/local/bin/garage
COPY garage.toml /etc/garage.toml
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
