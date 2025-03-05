package com.hidarisoft.reactivekafkaplayground.producers;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    public static void main(String[] args) {
        Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.25:9092,192.168.3.25:9094,192.168.3.25:9096",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        SenderOptions<String, String> options = SenderOptions.<String, String>create(producerConfig);

        Flux<SenderRecord<String, String, String>> flux = Flux.interval(Duration.ofMillis(100))
                .take(100)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(producerRecord -> SenderRecord.create(producerRecord, producerRecord.key()));

        KafkaSender.create(options)
                .send(flux)
                .doOnNext(stringSenderResult -> log.info("correlation id: {}", stringSenderResult.correlationMetadata()))
                .subscribe();
    }
}
