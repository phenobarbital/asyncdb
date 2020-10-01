#
# RethinkDB Dockerfile
#
FROM rethinkdb:2.4
RUN apt update
# Install python pip
RUN apt install -y python3-pip
# Install python driver for rethinkdb
RUN pip3 install rethinkdb
# Expose ports.
#   - 8080: web UI
#   - 28015: process
#   - 29015: cluster
EXPOSE 8080
EXPOSE 28015
EXPOSE 29015
