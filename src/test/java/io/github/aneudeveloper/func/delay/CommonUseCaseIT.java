package io.github.aneudeveloper.func.delay;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.github.aneudeveloper.func.delay.streams.DelayToWaitProcessor;

@Tag("integration")
public class CommonUseCaseIT {
    boolean exceptionInService = false;

    @Test
    public void expectMessageIsSendToTopicAfter1MinuteDelay() throws InterruptedException, ExecutionException {

        Properties props = new Properties();

        DelayService delayService = new DelayService(props, new StreamsUncaughtExceptionHandler() {
            @Override
            public StreamThreadExceptionResponse handle(Throwable exception) {
                System.err.println(exception);
                exceptionInService = true;
                return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            }
        });
        delayService.start();

        // ######################
        // Setup message and send it to delay topic. This message should be routed to
        // provided topic after 1 Minute
        // ######################

        KafkaHelper kafkaHelper = new KafkaHelper();
        ZonedDateTime executeAt = ZonedDateTime.now().plusMinutes(1);
        String key = UUID.randomUUID().toString();
        Map<String, String> someCustomHeader = new HashMap<>();
        someCustomHeader.put("my custom key", "value of key");

        String message = "some message. could be anything executeAt="
                + executeAt.format(DelayToWaitProcessor.TIME_STAMP_FORMATTER);

        String workingTopic = "TestDelay-WORKFLOW-" + System.currentTimeMillis();
        kafkaHelper.produceMessage(executeAt, workingTopic, key, message,
                someCustomHeader);

        // ######################
        // Check that the destination topic does not contain the message yet.
        // ######################
        boolean firstCheckOfTheMessage = false;
        List<ConsumerRecord<String, String>> messages = kafkaHelper.getMessages(workingTopic, "");
        for (ConsumerRecord<String, String> record : messages) {
            if (message.equals(record.value())) {
                firstCheckOfTheMessage = true;
            }
        }
        Assertions.assertFalse(firstCheckOfTheMessage);

        // ######################
        // Wait 3 Minutes until the message is send to topic after 1 Minute and check if
        // the
        // message with header und payload was send to provided topic.
        // ######################
        Thread.sleep(3 * 60 * 1000);

        Assertions.assertFalse(exceptionInService);

        messages = kafkaHelper.getMessages(workingTopic, "");

        boolean messageFound = false;
        boolean customHeaderFound = false;

        for (ConsumerRecord<String, String> record : messages) {
            if (new String(record.headers().headers("my custom key").iterator().next().value())
                    .equals("value of key")) {
                customHeaderFound = true;
            }
            if (message.equals(record.value())) {
                messageFound = true;
            }
        }

        Assertions.assertTrue(customHeaderFound);
        Assertions.assertTrue(messageFound);

    }
}
