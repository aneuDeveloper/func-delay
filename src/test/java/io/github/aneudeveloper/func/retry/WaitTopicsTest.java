package io.github.aneudeveloper.func.retry;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Properties;

import org.junit.Test;

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
        assertEquals("Should have 3 wait topics", 3, waitTopics.size());

        assertEquals("Has DELAY15S", "DELAY15S", waitTopics.stream().filter(wt -> wt.getTopicName().equals("DELAY15S"))
                .findFirst().get().getTopicName());
        assertEquals("Has DELAY1M", "DELAY1M",
                waitTopics.stream().filter(wt -> wt.getTopicName().equals("DELAY1M")).findFirst().get().getTopicName());
        assertEquals("Has DELAY15M", "DELAY15M", waitTopics.stream().filter(wt -> wt.getTopicName().equals("DELAY15M"))
                .findFirst().get().getTopicName());
    }
}
