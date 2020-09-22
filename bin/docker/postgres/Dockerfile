FROM postgres:11-alpine
RUN mkdir /logs; exit 0
RUN   chown postgres:postgres /logs -fR; exit 0
# TODO: build postgres environment
ADD ./init /docker-entrypoint-initdb.d/
