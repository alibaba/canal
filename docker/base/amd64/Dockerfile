# amd64
FROM centos:centos7.9.2009@sha256:dead07b4d8ed7e29e98de0f4504d87e8880d4347859d839686a31da35a3b532f

ARG jdk_rpm
ARG platform_env

MAINTAINER agapple (jianghang115@gmail.com)
# env DOWNLOAD_AMD_LINK="https://download.oracle.com/otn/java/jdk/8u361-b09/0ae14417abb444ebb02b9815e2103550/jdk-8u361-linux-x64.rpm"
# env DOWNLOAD_ARM_LINK="https://download.oracle.com/otn/java/jdk/8u361-b09/0ae14417abb444ebb02b9815e2103550/jdk-8u361-linux-aarch64.rpm"
env NODE_EPORTER_LINK="https://github.com/prometheus/node_exporter/releases/download/v1.6.1/node_exporter-1.6.1.linux-amd64.tar.gz"

# install system
# update yum config, fix "centos6.x yum install failure && Determining fastest mirrors slow" problems
COPY yum/ /tmp/
RUN \
    /bin/cp /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.bak && \
    /bin/cp -f /tmp/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo && \
    /bin/cp /etc/yum/pluginconf.d/fastestmirror.conf /etc/yum/pluginconf.d/fastestmirror.conf.bak && \
    awk '{ if($0=="enabled=1"){print "enabled=0";} else{print $0;} }' /etc/yum/pluginconf.d/fastestmirror.conf.bak > /etc/yum/pluginconf.d/fastestmirror.conf && \
    /bin/cp /etc/yum.conf /etc/yum.conf.bak && \
    echo 'minrate=1' >> /etc/yum.conf && echo 'timeout=600' >> /etc/yum.conf && \
    yum clean all && yum makecache

RUN \
    /usr/bin/ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo 'root:Hello1234' | chpasswd && \
    groupadd -r admin && useradd -g admin admin && \
    yum install -y man && \
    yum install -y dstat && \
    yum install -y unzip && \
    yum install -y nc && \
    yum install -y openssh-server && \
    yum install -y tar && \
    yum install -y which && \
    yum install -y wget && \
    yum install -y perl && \
    yum install -y file && \
    ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key && \
    ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key && \
    sed -ri 's/session    required     pam_loginuid.so/#session    required     pam_loginuid.so/g' /etc/pam.d/sshd && \
    sed -i -e 's/^#Port 22$/Port 2222/' /etc/ssh/sshd_config && \
    mkdir -p /root/.ssh && chown root.root /root && chmod 700 /root/.ssh && \
    yum install -y cronie && \
    sed -i '/session required pam_loginuid.so/d' /etc/pam.d/crond && \
    true

COPY ${jdk_rpm} /tmp/
RUN \
    touch /var/lib/rpm/* && \
    #if [ "$BUILDPLATFORM" == "linux/amd64" ] ; then wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=xxx; oraclelicense=accept-securebackup-cookie" "$DOWNLOAD_AMD_LINK" -O /tmp/jdk-8-linux.rpm  ; fi && \
    #if [ "$BUILDPLATFORM" == "linux/arm64" ] ; then wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=xxx; oraclelicense=accept-securebackup-cookie" "$DOWNLOAD_ARM_LINK" -O /tmp/jdk-8-linux.rpm  ; fi && \
    yum -y install /tmp/jdk-*.rpm && \
    /bin/rm -f /tmp/jdk-*.rpm && \
    echo "export JAVA_HOME=/usr/java/latest" >> /etc/profile && \
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile && \
    wget "$NODE_EPORTER_LINK" -O /tmp/node_exporter.tar.gz && \
    yum clean all && \
    true

CMD ["/bin/bash"]
