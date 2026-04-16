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

import org.apache.kafka.common.header.Header;
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
        Long executeAt = null;
        if (record.headers() != null) {
            Iterable<Header> executeAtHeaders = record.headers().headers(DelayService.EXECUTE_AT);
            if (executeAtHeaders != null) {
                Header next = executeAtHeaders.iterator().next();
                if (next != null) {
                    String executeAtAsString = new String(next.value());
                    try {
                        ZonedDateTime time = ZonedDateTime.parse(executeAtAsString, TIME_STAMP_FORMATTER);
                        if (time == null) {
                            LOG.error("executeAt header was not provided. Use current time instead.");
                        } else {
                            executeAt = time.toInstant().toEpochMilli();
                        }
                    } catch (Exception e) {
                        LOG.error(
                                "executeAt header was defined but was not parsable. Provided executeAt={}. Use current time will be used instead.",
                                executeAtAsString, e);
                    }
                } else {
                    LOG.error("executeAt header was not provided. Use current time instead.");
                }
            } else {
                LOG.error("executeAt header was not provided. Use current time instead.");
            }
        }

        if (executeAt == null) {
            LOG.warn("could not discover executeAt. The executeAt is set to current time.");
            executeAt = ZonedDateTime.now().toInstant().toEpochMilli();
        }

        LOG.debug("discovered executeAt={}", executeAt);

        Record<String, Long> nextRecord = record.withValue(executeAt)
                .withKey(record.key());

        context.forward(nextRecord);
    }

}
