FROM centos:centos6.10

MAINTAINER agapple (jianghang115@gmail.com)

env DOWNLOAD_LINK="http://download.oracle.com/otn-pub/java/jdk/8u181-b13/96a7b8442fe848ef90c96a2fad6ed6d1/jdk-8u181-linux-x64.rpm"
# install system

RUN \
    /bin/cp -f /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
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

RUN \
    touch /var/lib/rpm/* && \ 
    wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=xxx; oraclelicense=accept-securebackup-cookie" "$DOWNLOAD_LINK" -O /tmp/jdk-8-linux-x64.rpm && \
    yum -y install /tmp/jdk-8-linux-x64.rpm && \
    /bin/rm -f /tmp/jdk-8-linux-x64.rpm && \
    echo "export JAVA_HOME=/usr/java/latest" >> /etc/profile && \
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile && \
    yum clean all && \
    true

CMD ["/bin/bash"]
