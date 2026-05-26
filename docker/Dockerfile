################################################################################
# Aerospike Server Development Runtime Image
#
# Base matches a typical EE dev host (Debian 13 / trixie): bind-mounted `asd`
# must see the same glibc, OpenLDAP symbol versions, and libstdc++ as the build.
# Aerospike tools: use newest distro-specific bundle available from upstream
# (debian12 today); `apt-get install -f` pulls compatible runtime libs on trixie.
################################################################################

FROM debian:trixie-slim

ENV DEBIAN_FRONTEND=noninteractive

# Runtime dependencies for EE binary (host-mounted asd)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    wget \
    libssl3t64 \
    zlib1g \
    libldap2 \
    libcurl4t64 \
    && rm -rf /var/lib/apt/lists/*

# Aerospike tools (no trixie-specific bundle published for 9.2.1; debian12 is closest).
ARG AEROSPIKE_TOOLS_VERSION=9.2.1
ARG AEROSPIKE_TOOLS_DIST=debian12
ARG AEROSPIKE_TOOLS_ARCH=x86_64
RUN set -eux; \
    tools_tgz="aerospike-tools_${AEROSPIKE_TOOLS_VERSION}_${AEROSPIKE_TOOLS_DIST}_${AEROSPIKE_TOOLS_ARCH}.tgz"; \
    wget -q "https://download.aerospike.com/artifacts/aerospike-tools/${AEROSPIKE_TOOLS_VERSION}/${tools_tgz}" -O /tmp/tools.tgz; \
    tar -xzf /tmp/tools.tgz -C /tmp; \
    deb_arch=amd64; \
    if [ "${AEROSPIKE_TOOLS_ARCH}" = aarch64 ]; then deb_arch=arm64; fi; \
    dpkg -i "/tmp/aerospike-tools_${AEROSPIKE_TOOLS_VERSION}_${AEROSPIKE_TOOLS_DIST}_${AEROSPIKE_TOOLS_ARCH}/aerospike-tools_${AEROSPIKE_TOOLS_VERSION}-${AEROSPIKE_TOOLS_DIST}_${deb_arch}.deb" || true; \
    apt-get update; \
    apt-get install -y -f --no-install-recommends; \
    rm -rf /var/lib/apt/lists/* /tmp/aerospike-tools* /tmp/tools.tgz

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
