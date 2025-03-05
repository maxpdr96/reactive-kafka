package com.hidarisoft.reactivekafkaplayground.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Map;
import java.util.regex.Pattern;

    /*
    Goal: Consumir multiplos topicos, no caso usando order-* regex
     */


public class MultipleTopicConsumer {

    private static final Logger log = LoggerFactory.getLogger(MultipleTopicConsumer.class);

    public static void main(String[] args) {

        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.25:9092,192.168.3.25:9094,192.168.3.25:9096",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "inventory-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        ReceiverOptions<Object, Object> options = ReceiverOptions.create(consumerConfig)
                .subscription(Pattern.compile("order.*"));

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("Topic: {}, key: {}, value: {}",r.topic(), r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();

    }
}
