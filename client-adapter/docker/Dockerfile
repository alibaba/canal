# 本脚本参考slpcat的docker镜像生成脚本
# 链接：https://github.com/slpcat/docker-images/tree/master/bigdata/canal/debian10-canal-adapter-jdk8
FROM slpcat/openjdk:8

MAINTAINER 若虚 <slpcat@qq.com>

# install canal-adapter
COPY canal.adapter-*.tar.gz /tmp/

RUN \
    mkdir -p /opt/canal-adapter && \
    tar -zxf /tmp/canal.adapter-*.tar.gz -C /opt/canal-adapter && \
    rm -r /tmp/canal.adapter-*.tar.gz

COPY startup.sh /opt/canal-adapter/bin/startup.sh

# 8081 exporter
EXPOSE 8081

WORKDIR /opt/canal-adapter

CMD ["sh", "-c", "./bin/startup.sh"]