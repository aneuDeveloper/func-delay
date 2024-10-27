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

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.delay.TopicSelector;

public class WaitTimeHandler implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(WaitTimeHandler.class);
    private TopicSelector.WaitTopic waitInterval;
    private KafkaProducer<String, Long> kafkaProducer;
    private KafkaConsumer<String, Long> consumer;
    private TopicSelector topicSelector;
    private ConsumerGroupMetadata consumerGroupMetadata;
    private Date lastPollDate;

    public WaitTimeHandler(KafkaProducer<String, Long> kafkaProducer, TopicSelector.WaitTopic waitInterval,
            TopicSelector topicSelector, Properties commonConsumerProperties,
            String consumerGroupPrefix) {
        this.kafkaProducer = kafkaProducer;
        this.waitInterval = waitInterval;
        this.topicSelector = topicSelector;
        LOG.info("Create WaitTimeHandler with waitInterval timeDefinition={} topicName={} waitTime={}",
                waitInterval.getTimeDefinition(), waitInterval.getTopicName(), waitInterval.getWaitTime());

        this.consumerGroupMetadata = new ConsumerGroupMetadata(consumerGroupPrefix + waitInterval.getTimeDefinition());
        Properties consumerProperties = new Properties();
        consumerProperties.putAll((Map<?, ?>) commonConsumerProperties);
        consumerProperties.put("group.id", consumerGroupMetadata.groupId());
        LOG.info("Adding group.id={}", consumerGroupMetadata.groupId());
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
        this.consumer = new KafkaConsumer<String, Long>(consumerProperties);

        LOG.info("Subscribe to topic {}", waitInterval.getTopicName());
        for (Map.Entry<Object, Object> entry : consumerProperties.entrySet()) {
            LOG.debug("Subscribe to topic {} with property {}={}", waitInterval.getTopicName(),
                    String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }

        this.consumer.subscribe(Arrays.asList(waitInterval.getTopicName()));
    }

    @Override
    public void run() {
        LOG.info("Starting consumer for waitHandler with timedefinition={} and topic={} and groupInstanceId={}",
                this.waitInterval.getTimeDefinition(), this.waitInterval.getTopicName(),
                consumerGroupMetadata.groupId());

        try {
            Duration waitDuration = Duration.ofMillis(this.waitInterval.getWaitTime());

            while (true) {
                this.lastPollDate = new Date();
                LOG.trace("Poll for messages in topic={} seconds={}", this.waitInterval.getTopicName(),
                        waitDuration.getSeconds());

                ConsumerRecords<String, Long> records = this.consumer.poll(waitDuration);
                if (records == null || records.isEmpty()) {
                    LOG.trace("No messages found for topic={}", this.waitInterval.getTopicName());
                    continue;
                }
                LOG.info("Got {} messages for topic={} start processing...", records.count(),
                        this.waitInterval.getTopicName());

                try {
                    this.kafkaProducer.beginTransaction();
                    for (ConsumerRecord<String, Long> record : records) {
                        String selectedTopic = this.topicSelector.selectTopic((Long) record.value());
                        LOG.debug("Selected topic={} with key={} and value={}", selectedTopic, record.key(),
                                record.value());
                        ProducerRecord<String, Long> producerRecord = new ProducerRecord<>(selectedTopic, record.key(),
                                record.value());
                        this.kafkaProducer.send(producerRecord);
                    }
                    HashMap<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, Long>> partitionedRecords = records.records(partition);
                        long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
                        offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1L));
                    }
                    this.kafkaProducer.sendOffsetsToTransaction(offsetsToCommit,
                            consumerGroupMetadata);
                    this.kafkaProducer.commitTransaction();
                } catch (ProducerFencedException e) {
                    LOG.error(e.getMessage(), e);
                    LOG.info("Closing kafkaProducer and waitTimeHandler for topic={}",
                            this.waitInterval.getTopicName());
                    this.kafkaProducer.close();
                    throw e;
                } catch (KafkaException e) {
                    LOG.error(e.getMessage() + " aborting transaction", e);
                    this.kafkaProducer.abortTransaction();
                }
            }
        } finally {
            LOG.info("Shutdown waitHandler for timedefinition={}", this.waitInterval.getTimeDefinition());
            this.consumer.close();
        }
    }

    public Date getLastPollDate() {
        return this.lastPollDate;
    }

    public TopicSelector.WaitTopic getWaitTopic() {
        return this.waitInterval;
    }

}
