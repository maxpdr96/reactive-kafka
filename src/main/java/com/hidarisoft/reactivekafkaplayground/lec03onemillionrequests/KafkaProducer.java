package com.hidarisoft.reactivekafkaplayground.lec03onemillionrequests;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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

        SenderOptions<String, String> options = SenderOptions.<String, String>create(producerConfig)
                .maxInFlight(10_000);

        Flux<SenderRecord<String, String, String>> flux = Flux.range(1, 1_000_000)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(producerRecord -> SenderRecord.create(producerRecord, producerRecord.key()));


        com.hidarisoft.reactivekafkaplayground.lec04header.KafkaProducer.senderCreated(options, flux, log);
    }
}
