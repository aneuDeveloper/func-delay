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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.RecordContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.delay.MessageDeserializer;

public class RevokeStream {
    private static final Logger LOG = LoggerFactory.getLogger(RevokeStream.class);
    private Properties revokeStreamConfig;
    private KafkaStreams stream;
    private String revokeTopic;
    private String delayTopic;
    private String delayDeadLetterQueueTopic;
    private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
    private MessageDeserializer funcEventDeserializer = new MessageDeserializer();

    public RevokeStream(Properties properties, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
        this.uncaughtExceptionHandler = uncaughtExceptionHandler;
        this.delayDeadLetterQueueTopic = properties.getProperty("delay.dead.letter.topic");
        String revokeStreamName = properties.getProperty("delay.revoke-stream-app-name");
        this.revokeTopic = properties.getProperty("delay.revoke-topic");
        this.delayTopic = properties.getProperty("delay.topic");
        String bootstapServer = properties.getProperty("bootstrap.servers");
        String replicationFactor = properties.getProperty("topic.default.replication.factor");
        this.revokeStreamConfig = new Properties();
        this.revokeStreamConfig.put("replication.factor", replicationFactor);
        this.revokeStreamConfig.put("application.id", revokeStreamName);
        this.revokeStreamConfig.put("bootstrap.servers", bootstapServer);
        this.revokeStreamConfig.put("default.key.serde", Serdes.String().getClass());
        this.revokeStreamConfig.put("default.value.serde", Serdes.String().getClass());
        this.revokeStreamConfig.put("processing.guarantee", "exactly_once_v2");
    }

    public void start() {
        LOG.info("Start");
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> revokeStream = streamsBuilder.stream(this.revokeTopic);
        KTable<String, String> processStepsTable = streamsBuilder.table(this.delayTopic);

        revokeStream.join(processStepsTable, this::mergeValues) //
                .to(this::selectDestinationTopic);

        Topology topology = streamsBuilder.build();
        this.stream = new KafkaStreams(topology, this.revokeStreamConfig);
        if (this.uncaughtExceptionHandler != null) {
            this.stream.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);
        }
        this.stream.start();
    }

    private String mergeValues(String delayEvent, String originalProcessEvent) {
        LOG.debug("Revoke message delayEvent={} originalProcessEvent={}", delayEvent, originalProcessEvent);
        return originalProcessEvent;
    }

    private String selectDestinationTopic(String key, String processEventAsString, RecordContext recordContext) {
        try {
            String destinationTopic = funcEventDeserializer.getAsString(MessageDeserializer.SOURCE_TOPIC_KEY,
                    processEventAsString);
            LOG.debug("Revoke destinationTopic={} key={} processEvent={}",
                    new Object[] { destinationTopic, key, processEventAsString });
            if (destinationTopic == null || destinationTopic.isEmpty()) {
                LOG.error(
                        "Key={} destinationTopic could not be discovered. Forward to " + this.delayDeadLetterQueueTopic,
                        key);
                return this.delayDeadLetterQueueTopic;
            }
            return destinationTopic;
        } catch (Exception e) {
            LOG.error("Key=" + key + e.getMessage() + " forward to " + this.delayDeadLetterQueueTopic, e);
            return this.delayDeadLetterQueueTopic;
        }
    }

    public void close() {
        LOG.info("close");
        this.stream.close();
    }
}
