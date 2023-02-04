package io.github.aneudeveloper.func.retry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import io.github.aneudeveloper.func.delay.MessageDeserializer;

/**
 * Unit test for simple App.
 */
public class MessageDeserializerTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldGetStringValue() {
        MessageDeserializer funcEventDeserializer = new MessageDeserializer();
        assertEquals("Should get source", "workflow",
                funcEventDeserializer.getAsString(MessageDeserializer.SOURCE_TOPIC_KEY,
                        "id=234,test=kasdf,sourceTopic=workflow$e%,{test=234}"));

        assertEquals("Should get source", "workflow",
                funcEventDeserializer.getAsString(MessageDeserializer.SOURCE_TOPIC_KEY,
                        "sourceTopic=workflow,id=234,test=kasdf$e%,{test=234}"));

        assertEquals("Should get source", "workflow",
                funcEventDeserializer.getAsString(MessageDeserializer.SOURCE_TOPIC_KEY,
                        "id=234,sourceTopic=workflow,test=kasdf$e%,{test=234}"));
    }

    @Test
    public void shouldGetLongValue() {
        MessageDeserializer funcEventDeserializer = new MessageDeserializer();
        assertTrue(649345897 == funcEventDeserializer.getAsLong(MessageDeserializer.NEXT_RETRY_AT_KEY,
                "id=234,test=kasdf,sourceTopic=workflow,nextRetryAt=649345897$e%,{test=234}"));

    }
}
