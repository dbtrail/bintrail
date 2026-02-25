FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        sysbench \
        default-mysql-client \
        && rm -rf /var/lib/apt/lists/*

COPY traffic.sh /traffic.sh
RUN chmod +x /traffic.sh

CMD ["/traffic.sh"]
