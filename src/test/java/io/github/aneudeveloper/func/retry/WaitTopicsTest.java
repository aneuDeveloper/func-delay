package io.github.aneudeveloper.func.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import io.github.aneudeveloper.func.delay.DelayService;
import io.github.aneudeveloper.func.delay.TopicSelector;
import io.github.aneudeveloper.func.delay.TopicSelector.WaitTopic;

public class WaitTopicsTest {
        @Test
        public void testTopicDetectionByTopicSelector() {
                Properties properties = new Properties();
                properties.put(DelayService.DELAY_WAIT_TOPICS_PREFIX, "DELAY");
                properties.put(DelayService.DELAY_WAIT_TOPICS, "15S, 1M, 15M");

                TopicSelector topicSelector = new TopicSelector(properties);
                List<WaitTopic> waitTopics = topicSelector.getWaitTopics();
                assertEquals(3, waitTopics.size(), "Should have 3 wait topics");

                assertEquals("DELAY15S",
                                waitTopics.stream().filter(wt -> wt.getTopicName().equals("DELAY15S"))
                                                .findFirst().get().getTopicName(),
                                "Has DELAY15S");
                assertEquals("DELAY1M",
                                waitTopics.stream().filter(wt -> wt.getTopicName().equals("DELAY1M")).findFirst().get()
                                                .getTopicName(),
                                "Has DELAY1M");
                assertEquals("DELAY15M",
                                waitTopics.stream().filter(wt -> wt.getTopicName().equals("DELAY15M"))
                                                .findFirst().get().getTopicName(),
                                "Has DELAY15M");
        }
}
