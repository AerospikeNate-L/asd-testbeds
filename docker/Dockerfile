################################################################################
# Aerospike Server Development Runtime Image
# Ubuntu 22.04 with runtime dependencies for EE + tools
################################################################################

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Runtime dependencies for EE binary
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    zlib1g \
    libldap-2.5-0 \
    libcurl4 \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install aerospike-tools
RUN wget -q https://download.aerospike.com/artifacts/aerospike-tools/9.2.1/aerospike-tools_9.2.1_ubuntu22.04_x86_64.tgz -O /tmp/tools.tgz \
    && tar -xzf /tmp/tools.tgz -C /tmp \
    && dpkg -i /tmp/aerospike-tools_9.2.1_ubuntu22.04_x86_64/aerospike-tools_9.2.1-ubuntu22.04_amd64.deb \
    && rm -rf /tmp/aerospike-tools* /tmp/tools.tgz

# Debugging tools (optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdb \
    strace \
    vim \
    less \
    curl \
    iproute2 \
    net-tools \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create required directories
RUN mkdir -p /var/lib/aerospike \
    /var/log/aerospike \
    /var/run/aerospike \
    /etc/aerospike \
    /opt/aerospike/usr/udf/lua \
    /opt/aerospike/sys/udf/lua \
    /opt/aerospike/smd \
    /opt/aerospike/data \
    /opt/aerospike/xdr

WORKDIR /

EXPOSE 3000 3001 3002 3003 3004

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["asd"]
