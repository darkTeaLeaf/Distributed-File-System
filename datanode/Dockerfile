FROM ubuntu:18.04

EXPOSE 21

COPY ./entrypoint.sh /
ADD ftp_server.py /

RUN apt-get update
RUN apt install -y iproute2
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip3 install pyftpdlib
RUN pip3 install requests

RUN chmod +x /entrypoint.sh

RUN mkdir -p /home/ubuntu/storage


ENTRYPOINT ["/entrypoint.sh"]