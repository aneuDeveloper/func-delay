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
package io.github.aneudeveloper.func.delay.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.delay.DelayService;
import io.github.aneudeveloper.func.delay.TopicSelector;

public class DelayTopicToWaitTopicStream {
    private static final Logger LOG = LoggerFactory.getLogger(DelayTopicToWaitTopicStream.class);
    private String delayTopicWithHeader;
    private TopicSelector topicSelector;
    private Properties delayTableToWaitTopicStreamConfig;
    private KafkaStreams streams;
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;

    public DelayTopicToWaitTopicStream(TopicSelector topicSelector, Properties properties,
            StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
        this.topicSelector = topicSelector;
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        this.delayTopicWithHeader = properties.getProperty(DelayService.DELAY_TOPIC_WITH_HEADER);
        String bootstapServer = properties.getProperty(DelayService.KAFKA_BOOTSTRAP_SERVERS);
        String replicationFactor = properties.getProperty("topic.default.replication.factor");
        this.delayTableToWaitTopicStreamConfig = new Properties();
        this.delayTableToWaitTopicStreamConfig.put("replication.factor", replicationFactor);
        this.delayTableToWaitTopicStreamConfig.put("application.id", "RetryTableToWaitTopicStream");
        this.delayTableToWaitTopicStreamConfig.put(DelayService.KAFKA_BOOTSTRAP_SERVERS, bootstapServer);
        this.delayTableToWaitTopicStreamConfig.put("default.key.serde", Serdes.String().getClass());
        this.delayTableToWaitTopicStreamConfig.put("default.value.serde", Serdes.String().getClass());
        this.delayTableToWaitTopicStreamConfig.put("processing.guarantee", "exactly_once_v2");
    }

    public void start() {
        LOG.info("Start");
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, byte[]> stream = streamsBuilder.stream(this.delayTopicWithHeader);
        stream.process(() -> new DelayToWaitProcessor())
                .to((key, value, recordContext) -> {
                    String waitTopic = this.topicSelector.selectTopic((long) value);
                    LOG.debug("Select topic={} key={} for nextRetryAt={}", waitTopic, key, value);
                    return waitTopic;
                }, Produced.valueSerde(Serdes.Long()));
        this.streams = new KafkaStreams(streamsBuilder.build(), this.delayTableToWaitTopicStreamConfig);
        if (this.uncaughtExceptionHandler != null) {
            this.streams.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);
        }
        this.streams.start();
    }

    public void close() {
        LOG.info("close");
        this.streams.close();
    }
}
