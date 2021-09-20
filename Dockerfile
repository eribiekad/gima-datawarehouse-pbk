FROM python:3.7-slim
MAINTAINER Joost Venema <joost.venema@kadaster.nl>

# Setup the Kadaster proxy configuration
RUN echo "Acquire::http::Proxy \"http://www-proxy.cs.kadaster.nl:8082\";" > /etc/apt/apt.conf
RUN echo "Acquire::https::Proxy \"http://www-proxy.cs.kadaster.nl:8082\";" > /etc/apt/apt.conf
RUN echo "Defaults env_keep=\"http_proxy\"" > /etc/sudoers
RUN echo "Defaults env_keep=\"https_proxy\"" > /etc/sudoers
ENV http_proxy http://ssl-proxy.cs.kadaster.nl:8080
ENV https_proxy http://ssl-proxy.cs.kadaster.nl:8080
ENV ftp_proxy http://www-proxy.cs.kadaster.nl:8082
ENV no_proxy kadaster.nl

# set correct timezone
ENV TZ Europe/Amsterdam

# Update APT repository
RUN apt-get -y update \
    && apt-get install -y libaio1 libaio-dev gcc alien ca-certificates wget

# Add Kadaster CA-cert to certs-store
COPY Docker/capgemini-ca.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates

# RPM to DEB and install
WORKDIR    /opt/oracle
RUN wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm \
    && wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-devel-linuxx64.rpm \
    && alien -d *.rpm && dpkg -i *.deb

# Add Files
WORKDIR /
COPY  pbk/requirements.txt /pbk/requirements.txt

# Install packages
RUN pip install -i https://ota-portal.so.kadaster.nl/artifactory/api/pypi/python-registry/simple --trusted-host ota-portal.so.kadaster.nl -r /hpp/requirements.txt

COPY  pbk/ /pbk/

WORKDIR /pbk
CMD ["pbk_loader.py",  "execute" ]