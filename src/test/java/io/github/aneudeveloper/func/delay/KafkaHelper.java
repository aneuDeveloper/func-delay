package io.github.aneudeveloper.func.delay;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.github.aneudeveloper.func.delay.streams.DelayToWaitProcessor;

public class KafkaHelper {
    public List<ConsumerRecord<String, String>> getMessages(String topic, String consumerGroup) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        List<ConsumerRecord<String, String>> returnVal = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            ConsumerRecords<String, String> records = consumer
                    .poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                returnVal.add(record);
            }
        }

        consumer.close();
        return returnVal;
    }

    public void produceMessage(ZonedDateTime executeAt, String executeTopic, String key, String message)
            throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.01:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Reliability & performance
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        byte[] payload = message.getBytes(StandardCharsets.UTF_8);

        ProducerRecord<String, byte[]> record = new ProducerRecord<>("DELAY", key, payload);

        record.headers().add(new RecordHeader(
                DelayService.SOURCE_TOPIC, executeTopic.getBytes(StandardCharsets.UTF_8)));

        record.headers().add(new RecordHeader(DelayService.EXECUTE_AT,
                executeAt.format(DelayToWaitProcessor.TIME_STAMP_FORMATTER).getBytes(StandardCharsets.UTF_8)));

        producer.send(record).get();

        producer.flush();
        producer.close();
    }

}
