FROM elasticsearch:8.11.3

ENV ES_JAVA_OPTS "-Xms2g -Xmx2g -Des.xpack.security.enabled=false"

COPY elasticsearch.yml /usr/share/elasticsearch/config/

EXPOSE 9200
