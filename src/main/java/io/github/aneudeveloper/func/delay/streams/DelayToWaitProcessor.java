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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.aneudeveloper.func.delay.DelayService;

public class DelayToWaitProcessor implements Processor<String, byte[], String, Long> {
    private static final Logger LOG = LoggerFactory.getLogger(DelayToWaitProcessor.class);
    public static final DateTimeFormatter TIME_STAMP_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private ProcessorContext<String, Long> context;

    @Override
    public void init(ProcessorContext<String, Long> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, byte[]> record) {

        String executeAtAsString = new String(
                record.headers().headers(DelayService.EXECUTE_AT).iterator().next().value());

        ZonedDateTime time = ZonedDateTime.parse(executeAtAsString, TIME_STAMP_FORMATTER);
        if (time == null) {
            throw new IllegalStateException("executeAt could not be parsed");
        }

        Long executeAt = time.toInstant().toEpochMilli();
        LOG.debug("Map from delayTable executeAt={} executeAtAsString={}", executeAt, executeAtAsString);

        Record<String, Long> nextRecord = record.withValue(executeAt)
                .withKey(record.key());

        context.forward(nextRecord);
    }

}
