/**
* Copyright 2022 aneu
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the * "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
* 
*/
package io.github.aneudeveloper.func.delay.wait;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.delay.TopicSelector;

public class WaitManager {
    private static final Logger LOG = LoggerFactory.getLogger(WaitManager.class);
    private TopicSelector topicSelector;
    private List<WaitThread> waitTimeHandlers = new ArrayList<>();
    private Properties commonConsumerProperties;
    private String consumerGroupPrefix;
    private KafkaProducer<String, Long> kafkaProducer;

    public WaitManager(Properties properties, TopicSelector topicSelector) {
        this.topicSelector = topicSelector;
        this.consumerGroupPrefix = String
                .valueOf(properties.getOrDefault("retry.wait.consumer-group-prefix", "RetryWaitGroup"));
        String bootstapServer = String.valueOf(properties.getOrDefault("bootstrap.servers", "127.0.0.1:9092"));
        this.commonConsumerProperties = new Properties();
        this.commonConsumerProperties.put("bootstrap.servers", bootstapServer);
        this.commonConsumerProperties.put("key.deserializer", StringDeserializer.class);
        this.commonConsumerProperties.put("value.deserializer", LongDeserializer.class);
        this.commonConsumerProperties.put("enable.auto.commit", false);
        this.commonConsumerProperties.put("auto.offset.reset", "earliest");
        this.commonConsumerProperties.put("isolation.level", "read_committed");
        this.commonConsumerProperties.put("allow.auto.create.topics", false);

        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", bootstapServer);
        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer", LongSerializer.class.getName());
        producerProperties.setProperty("enable.idempotence", "true");
        producerProperties.setProperty("acks", "all");
        producerProperties.setProperty("retries", "10");
        producerProperties.setProperty("compression.type", "snappy");

        String transactioId = String
                .valueOf(properties.getOrDefault("retry.producer.transactionalid",
                        "delay-transaction-id-" + UUID.randomUUID().toString()));
        producerProperties.setProperty("transactional.id", transactioId);

        for (Map.Entry<Object, Object> entry : producerProperties.entrySet()) {
            LOG.info("Creating producer with {}={}", String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        this.kafkaProducer = new KafkaProducer<String, Long>(producerProperties);
        this.kafkaProducer.initTransactions();
    }

    public Map<TopicSelector.WaitTopic, Date> getLastPolls() {
        HashMap<TopicSelector.WaitTopic, Date> result = new HashMap<>();
        this.waitTimeHandlers.forEach(waitTimeHandler -> result.put(waitTimeHandler.getHandler().getWaitTopic(),
                waitTimeHandler.getHandler().getLastPollDate()));
        return result;
    }

    public void start() {
        List<TopicSelector.WaitTopic> waitIntervals = this.topicSelector.getWaitTopics();
        for (TopicSelector.WaitTopic waitInterval : waitIntervals) {
            WaitTimeHandler waitTimeHandler = new WaitTimeHandler(this.kafkaProducer, waitInterval, this.topicSelector,
                    this.commonConsumerProperties,
                    this.consumerGroupPrefix);
            WaitThread thread = new WaitThread(waitTimeHandler);
            this.waitTimeHandlers.add(thread);
            thread.start();
        }
    }

    public void close() {
        if (this.waitTimeHandlers != null) {
            this.waitTimeHandlers.forEach(thread -> thread.interrupt());
        }
        if (this.kafkaProducer != null) {
            LOG.info("Closing kafkaProducer");
            this.kafkaProducer.close();
        }
    }

    private static class WaitThread extends Thread {
        private WaitTimeHandler handler;

        public WaitThread(WaitTimeHandler waitTimeHandler) {
            super(waitTimeHandler);
            this.handler = waitTimeHandler;
        }

        public WaitTimeHandler getHandler() {
            return this.handler;
        }
    }
}
