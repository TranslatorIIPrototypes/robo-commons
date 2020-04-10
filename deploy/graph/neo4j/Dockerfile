FROM neo4j:4.0
LABEL maintainer="patrick@covar.com"

ENV APOC_URI https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/4.0.0.6/apoc-4.0.0.6-all.jar
ENV ROBOKOP_URI https://github.com/NCATS-Gamma/robokop-neo4j-plugin/releases/download/v2.0.0/robokop-2.0.0.jar
ENV GA_URL https://s3-eu-west-1.amazonaws.com/com.neo4j.graphalgorithms.dist/neo4j-graph-algorithms-3.5.14.0-standalone.zip

RUN mkdir /plugins

RUN wget $APOC_URI \
    && mv apoc-4.0.0.6-all.jar /plugins

RUN wget $ROBOKOP_URI \
    && mv robokop-2.0.0.jar /plugins

RUN wget $GA_URL \
    && mv neo4j-graph-algorithms-3.5.14.0-standalone.zip /plugins

EXPOSE 7474 7473 7687

CMD ["neo4j"]