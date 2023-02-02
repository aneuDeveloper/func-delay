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
package com.github.func.delay.wait;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.func.delay.TopicSelector;

public class WaitTimeHandler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(WaitTimeHandler.class);
    private TopicSelector.WaitTopic waitInterval;
    private KafkaProducer<String, Long> kafkaProducer;
    private KafkaConsumer<String, Long> consumer;
    private TopicSelector topicSelector;
    private CountDownLatch countDownLatch;
    private String consumerGroup;
    private Date lastPollDate;

    public WaitTimeHandler(Properties aCommonKafkaProducerProperties, String transactioIdPrefix,
            TopicSelector.WaitTopic waitInterval, TopicSelector topicSelector, Properties aCommonConsumerProperties,
            String consumerGroupPrefix) {
        this.waitInterval = waitInterval;
        this.topicSelector = topicSelector;
        this.consumerGroup = consumerGroupPrefix + waitInterval.getTimeDefinition();
        Properties consumerProperties = new Properties();
        consumerProperties.putAll((Map<?, ?>) aCommonConsumerProperties);
        consumerProperties.put("group.id", this.consumerGroup);
        int waitTimeMillisForTopic = waitInterval.getWaitTime().intValue();
        int maxToWaitMillis = waitTimeMillisForTopic + 300000;
        if (waitTimeMillisForTopic >= 10800000) {
            maxToWaitMillis = waitTimeMillisForTopic + 900000;
        } else if (waitTimeMillisForTopic >= 43200000) {
            maxToWaitMillis = waitTimeMillisForTopic + 1800000;
        }
        consumerProperties.put("max.poll.interval.ms", maxToWaitMillis);
        consumerProperties.put("connections.max.idle.ms", maxToWaitMillis);
        consumerProperties.put("request.timeout.ms", maxToWaitMillis);
        consumerProperties.put("fetch.min.bytes", Integer.MAX_VALUE);
        consumerProperties.put("fetch.max.wait.ms", waitTimeMillisForTopic);
        this.countDownLatch = new CountDownLatch(1);
        this.consumer = new KafkaConsumer<String, Long>(consumerProperties);
        this.consumer.subscribe(Arrays.asList(waitInterval.getTopicName()));
        Properties producerProperties = new Properties();
        producerProperties.putAll((Map<?, ?>) aCommonKafkaProducerProperties);
        producerProperties.put("transactional.id", transactioIdPrefix + waitInterval.getTimeDefinition());
        producerProperties.put("connections.max.idle.ms", maxToWaitMillis);
        this.kafkaProducer = new KafkaProducer<String, Long>(producerProperties);
        this.initTransaction();
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     */
    @Override
    public void run() {
        LOG.info("Starting consumer for waitHandler with timedefinition={} and topic={} and consumerGroup={}",
                new Object[] { this.waitInterval.getTimeDefinition(), this.waitInterval.getTopicName(),
                        this.consumerGroup });
        try {
            try {
                while (true) {
                    this.lastPollDate = new Date();
                    LOG.trace("Poll for messages in topic={}", this.waitInterval.getTopicName());
                    ConsumerRecords<String, Long> records = this.consumer
                            .poll(Duration.ofMillis(this.waitInterval.getWaitTime()));
                    if (records == null || records.isEmpty()) {
                        continue;
                    }
                    this.kafkaProducer.beginTransaction();
                    for (ConsumerRecord<String, Long> record : records) {
                        String selectedTopic = this.topicSelector.selectTopic((Long) record.value());
                        LOG.debug("Selected topic={} with key={} and value={}",
                                new Object[] { selectedTopic, record.key(), record.value() });
                        ProducerRecord<String, Long> producerRecord = new ProducerRecord<>(selectedTopic,
                                record.key(), record.value());
                        this.kafkaProducer.send(producerRecord);
                    }
                    HashMap<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, Long>> partitionedRecords = records.records(partition);
                        long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
                        offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1L));
                    }
                    this.kafkaProducer.sendOffsetsToTransaction(offsetsToCommit, this.consumerGroup);
                    this.kafkaProducer.commitTransaction();
                }
            } catch (WakeupException e) {
                LOG.info("Shutdown waitHandler for timedefinition={}", this.waitInterval.getTimeDefinition());
                this.consumer.close();
                this.countDownLatch.countDown();
            }
        } catch (Throwable throwable) {
            this.consumer.close();
            this.countDownLatch.countDown();
            throw throwable;
        }
    }

    public Date getLastPollDate() {
        return this.lastPollDate;
    }

    public TopicSelector.WaitTopic getWaitTopic() {
        return this.waitInterval;
    }

    private void initTransaction() {
        RuntimeException lastException = null;
        boolean initSuccessful = false;
        for (int i = 0; i < 10; ++i) {
            try {
                this.kafkaProducer.initTransactions();
                initSuccessful = true;
                break;
            } catch (RuntimeException e) {
                lastException = e;
                LOG.warn(e.getMessage(), e);
                continue;
            }
        }
        if (!initSuccessful) {
            throw lastException;
        }
    }

    public void close() {
        this.consumer.wakeup();
        try {
            this.countDownLatch.await();
            LOG.info("Close kafkaProducer for timeDefinition={}",  this.waitInterval.getTimeDefinition());
            this.kafkaProducer.close();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
