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
package io.github.aneudeveloper.func.delay;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.delay.streams.DelayTopicToWaitTopicStream;
import io.github.aneudeveloper.func.delay.streams.RevokeStream;
import io.github.aneudeveloper.func.delay.wait.WaitManager;

public class DelayService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayService.class);
    public static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String DELAY_WAIT_TOPICS = "delay.topics";
    public static final String DELAY_WAIT_TOPICS_PREFIX = "delay.topics.prefix";
    public static final String DELAY_WAIT_TOPIC_PREFIX = "delay.wait.topic-prefix";
    public static final String DELAY_REVOKE_TOPIC = "delay.revoke-topic";
    public static final String DELAY_TOPIC = "delay.topic";
    public static final String DELAY_TOPIC_WITH_HEADER = "delay.with.header";
    public static final String DELAY_DEAD_LETTER_TOPIC = "delay.dead.letter.topic";
    public static final String TOPIC_DEFAULT_REPLICATION_FACTOR = "topic.default.replication.factor";
    public static final String TOPIC_DEFAULT_NUM_PARTITIONS = "topic.default.num.partitions";
    public static final String TOPIC_DEFAULT_RETENTION_MS = "topic.default.retention.ms";
    public static final String DELAY_REVOKE_STREAM_APP_NAME = "delay.revoke-stream-app-name";

    public static final String EXECUTE_AT = "execute_at";
    public static final String DESTINATION_TOPIC = "destination_topic";

    private DelayTopicToWaitTopicStream delayTableToWaitTopicStream;
    private WaitManager waitManager;
    private RevokeStream revokeStream;
    private Properties properties;
    private TopicSelector topicSelector;

    public DelayService(Properties properties, StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
        this.properties = this.addMissingPropertiesWithDefaultValues(properties);
        this.topicSelector = new TopicSelector(properties);
        this.delayTableToWaitTopicStream = new DelayTopicToWaitTopicStream(this.topicSelector, properties,
                uncaughtExceptionHandler);
        this.waitManager = new WaitManager(properties, this.topicSelector);
        this.revokeStream = new RevokeStream(properties, uncaughtExceptionHandler);
    }

    public Map<TopicSelector.WaitTopic, Date> getLastPolls() {
        return this.waitManager.getLastPolls();
    }

    public void start() {
        this.checkAndCreateTopics(this.properties, this.topicSelector);
        this.delayTableToWaitTopicStream.start();
        this.revokeStream.start();
        this.waitManager.start();
    }

    public void stop() {
        this.delayTableToWaitTopicStream.close();
        this.revokeStream.close();
        this.waitManager.close();
    }

    private Properties addMissingPropertiesWithDefaultValues(Properties properties) {
        this.addIfMissing(properties, DELAY_TOPIC, "DELAY");
        this.addIfMissing(properties, DELAY_TOPIC_WITH_HEADER, "DELAY_WITH_HEADER");
        this.addIfMissing(properties, DELAY_DEAD_LETTER_TOPIC, "DELAY_DEAD_LETTER");
        this.addIfMissing(properties, DELAY_REVOKE_TOPIC, "DELAY_REVOKE_TOPIC");
        this.addIfMissing(properties, KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092");
        this.addIfMissing(properties, DELAY_WAIT_TOPICS, "15S, 1M, 5M");
        this.addIfMissing(properties, DELAY_WAIT_TOPICS_PREFIX, "DELAY_WAIT_");
        this.addIfMissing(properties, DELAY_REVOKE_STREAM_APP_NAME, "DELAY");
        this.addIfMissing(properties, TOPIC_DEFAULT_REPLICATION_FACTOR, "1");
        this.addIfMissing(properties, TOPIC_DEFAULT_NUM_PARTITIONS, "1");
        this.addIfMissing(properties, TOPIC_DEFAULT_RETENTION_MS, "2592000000");
        return properties;
    }

    private void addIfMissing(Properties properties, String key, String value) {
        if (properties.get(key) == null) {
            properties.put(key, value);
        }
    }

    private void checkAndCreateTopics(Properties properties, TopicSelector topicSelector) {
        Properties adminClientProperties = new Properties();
        adminClientProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, properties.getProperty(KAFKA_BOOTSTRAP_SERVERS));
        try (AdminClient adminClient = AdminClient.create((Properties) adminClientProperties);) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            ArrayList<NewTopic> topicsToCreate = new ArrayList<NewTopic>();
            this.addTopicIfNotExist(existingTopics, topicsToCreate, properties.getProperty(DELAY_REVOKE_TOPIC),
                    properties, null);
            this.addTopicIfNotExist(existingTopics, topicsToCreate, properties.getProperty(DELAY_DEAD_LETTER_TOPIC),
                    properties, null);
            this.addTopicIfNotExist(existingTopics, topicsToCreate, properties.getProperty(DELAY_TOPIC), properties,
                    "compact");
            this.addTopicIfNotExist(existingTopics, topicsToCreate, properties.getProperty(DELAY_TOPIC_WITH_HEADER),
                    properties,
                    "compact");
            topicSelector.getWaitTopics().stream().forEach(timeInterval -> {
                String topicName = timeInterval.getTopicName();
                this.addTopicIfNotExist(existingTopics, topicsToCreate, topicName, properties, null);
            });
            if (!topicsToCreate.isEmpty()) {
                CreateTopicsResult createTopics = adminClient.createTopics(topicsToCreate);
                createTopics.values().values().stream().forEach(result -> {
                    try {
                        result.get();
                    } catch (InterruptedException | ExecutionException e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
            }
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void addTopicIfNotExist(Set<String> existingTopics, List<NewTopic> topicsToCreate, String topicName,
            Properties properties, String cleanupPolicy) {
        if (!existingTopics.contains(topicName)) {
            Integer defaultPartitionsNumber = Integer.valueOf(properties.getProperty(TOPIC_DEFAULT_NUM_PARTITIONS));
            Short defaultReplicationFactor = Short.valueOf(properties.getProperty(TOPIC_DEFAULT_REPLICATION_FACTOR));
            String defaultRetentionMs = properties.getProperty(TOPIC_DEFAULT_RETENTION_MS);
            NewTopic delayTopic = new NewTopic(topicName, defaultPartitionsNumber.intValue(),
                    defaultReplicationFactor.shortValue());
            delayTopic.configs(new HashMap<>());
            delayTopic.configs().put("retention.ms", defaultRetentionMs);
            if (cleanupPolicy != null) {
                delayTopic.configs().put("cleanup.policy", cleanupPolicy);
            }
            topicsToCreate.add(delayTopic);
        }
    }
}
