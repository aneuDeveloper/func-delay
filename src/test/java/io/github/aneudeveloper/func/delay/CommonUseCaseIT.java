package io.github.aneudeveloper.func.delay;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.github.aneudeveloper.func.delay.streams.DelayToWaitProcessor;

@Tag("integration")
public class CommonUseCaseIT {
    @Test
    public void testExecuteIn30Seconds() throws InterruptedException, ExecutionException {

        Properties props = new Properties();

        DelayService delayService = new DelayService(props, new StreamsUncaughtExceptionHandler() {
            @Override
            public StreamThreadExceptionResponse handle(Throwable exception) {
                System.err.println(exception);
                // Assertions.assertTrue(false);
                return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            }
        });
        delayService.start();

        KafkaHelper kafkaHelper = new KafkaHelper();
        ZonedDateTime executeAt = ZonedDateTime.now().plusMinutes(1);
        String key = UUID.randomUUID().toString();
        kafkaHelper.produceMessage(executeAt, "MyTest-WORKFLOW", key,
                "some message. could be anything executeAt="
                        + executeAt.format(DelayToWaitProcessor.TIME_STAMP_FORMATTER));

        Thread.sleep(10 * 60 * 1000);

    }
}
