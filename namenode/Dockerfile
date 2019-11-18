FROM ubuntu:18.04

EXPOSE 80

ADD fs_tree.py /
ADD ftp_client.py /
ADD http_handler.py /
ADD namenode.py /

RUN apt-get update
RUN apt-get install -y python3

CMD ["python3", "-u", "namenode.py"]
