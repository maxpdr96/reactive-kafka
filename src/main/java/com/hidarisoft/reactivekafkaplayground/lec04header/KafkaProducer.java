package com.hidarisoft.reactivekafkaplayground.lec04header;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

/*
   Goal: Producer 1000000 eventos e contando qt tempo
    */

public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    public static void main(String[] args) {
        Map<String, Object> producerConfig = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.25:9092,192.168.3.25:9094,192.168.3.25:9096",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        SenderOptions<String, String> options = SenderOptions.<String, String>create(producerConfig);

        Flux<SenderRecord<String, String, String>> flux = Flux.range(1, 10)
                .map(KafkaProducer::createSenderRecord);


        senderCreated(options, flux, log);
    }

    public static void senderCreated(SenderOptions<String, String> options, Flux<SenderRecord<String, String, String>> flux, Logger log) {
        KafkaSender<String, String> sender = KafkaSender.create(options);

        var start = System.currentTimeMillis();
        sender.send(flux)
                .doOnNext(stringSenderResult -> log.info("correlation id: {}", stringSenderResult.correlationMetadata()))
                .doOnComplete(() -> {
                    log.info("Total time: {}", System.currentTimeMillis() - start);
                    sender.close();
                })
                .subscribe();
    }

    private static SenderRecord<String, String, String> createSenderRecord(Integer i) {
        var headers = new RecordHeaders();
        headers.add("client-id", "some-client".getBytes());
        headers.add("tracing-id", "123".getBytes());
        var pr = new ProducerRecord<>("order-events", null, i.toString(), "order-" + i, headers);
        return SenderRecord.create(pr, pr.key());
    }
}
