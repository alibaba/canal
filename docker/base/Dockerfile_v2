FROM canal/osbase:v1

MAINTAINER agapple (jianghang115@gmail.com)

env NODE_EPORTER_LINK="https://github.com/prometheus/node_exporter/releases/download/v0.18.1/node_exporter-0.18.1.linux-arm64.tar.gz"

RUN \
    wget "$NODE_EPORTER_LINK" -O /tmp/node_exporter.tar.gz && \
    true

CMD ["/bin/bash"]