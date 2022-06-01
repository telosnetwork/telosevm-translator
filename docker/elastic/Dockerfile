FROM docker.elastic.co/elasticsearch/elasticsearch:8.2.1

ENV ES_JAVA_OPTS "-Xms2g -Xmx2g"
ENV ELASTIC_USERNAME "elastic"
ENV ELASTIC_PASSWORD "password"

COPY elasticsearch.yml /usr/share/elasticsearch/config/

RUN printf "password" | ./bin/elasticsearch-keystore add -f -x "bootstrap.password"

EXPOSE 9200 
