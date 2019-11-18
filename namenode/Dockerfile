FROM ubuntu:18.04

EXPOSE 80

COPY ./entrypoint.sh /
ADD fs_tree.py /
ADD ftp_client.py /
ADD http_handler.py /
ADD namenode.py /

RUN apt-get update
RUN apt install -y iproute2
RUN apt-get install -y python3

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
