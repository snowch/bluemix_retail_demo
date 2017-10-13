FROM docker.elastic.co/logstash/logstash:5.6.3
RUN rm -f /usr/share/logstash/pipeline/logstash.conf

ADD pipeline/ pipeline/
ADD config/ config/

RUN mkdir patch/
ADD patch/ patch/

RUN sed -i.bak -e '293,302d;' vendor/bundle/jruby/1.9/gems/logstash-input-kafka-5.1.11/lib/logstash/inputs/kafka.rb && \
    sed -i.bak '292r patch/kafka.rb' vendor/bundle/jruby/1.9/gems/logstash-input-kafka-5.1.11/lib/logstash/inputs/kafka.rb && \
    rm ./vendor/bundle/jruby/1.9/gems/logstash-input-kafka-5.1.11/vendor/jar-dependencies/runtime-jars/kafka-clients-0.10.0.1.jar && \
    curl -s -o ./vendor/bundle/jruby/1.9/gems/logstash-input-kafka-5.1.11/vendor/jar-dependencies/runtime-jars/kafka-clients-0.11.0.1.jar http://central.maven.org/maven2/org/apache/kafka/kafka-clients/0.11.0.1/kafka-clients-0.11.0.1.jar

CMD ["-f", "/usr/share/logstash/pipeline/logstash_transactions.conf"]
