FROM rabbitmq
FROM clojure

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY project.clj /usr/src/app/
RUN lein deps

COPY . /usr/src/app

ENV RABBITMQ-HOST=rabbitmq-server
ENV RABBITMQ-USERNAME="guest"
ENV RABBITMQ-PASSWORD="guest"
ENV RABBITMQ-PORT=5672
ENV RABBITMQ-VHOST="/"

ENTRYPOINT ["lein", "run"]
