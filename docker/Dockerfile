FROM canal/osbase:v1

MAINTAINER agapple (jianghang115@gmail.com)

# install canal
COPY image/ /tmp/docker/
COPY canal.deployer-*.tar.gz /home/admin/
COPY jdk-8-linux-x64.rpm /tmp/

RUN \
    cp -R /tmp/docker/alidata /alidata && \
    chmod +x /alidata/bin/* && \
    mkdir -p /home/admin && \
    cp -R /tmp/docker/admin/* /home/admin/  && \
    /bin/cp -f alidata/bin/lark-wait /usr/bin/lark-wait && \

    mkdir -p /home/admin/canal-server && \
    tar -xzvf /home/admin/canal.deployer-*.tar.gz -C /home/admin/canal-server && \
    /bin/rm -f /home/admin/canal.deployer-*.tar.gz && \

    mkdir -p home/admin/canal-server/logs  && \
    chmod +x /home/admin/*.sh  && \
    chmod +x /home/admin/bin/*.sh  && \
    chown admin: -R /home/admin && \
    yum clean all && \
    true

# 2222 sys , 8080 web , 8000 debug , 11111 canal
EXPOSE 2222 11111 8000 8080

WORKDIR /home/admin

ENTRYPOINT [ "/alidata/bin/main.sh" ]
CMD [ "/home/admin/app.sh" ]
