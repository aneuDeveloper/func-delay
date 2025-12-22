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

import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

public class HeaderPopulatingProcessor implements Processor<String, PayloadWithHeaders, String, byte[]> {

    private ProcessorContext<String, byte[]> context;

    @Override
    public void init(ProcessorContext<String, byte[]> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, PayloadWithHeaders> record) {
        RecordHeaders recordHeaders = new RecordHeaders();
        for (Entry<String, String> entry : record.value().headers.entrySet()) {
            recordHeaders.add(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8));
        }

        Record<String, byte[]> originalRecord = record.withValue(record.value().originalMessagePayload)//
                .withKey(record.key())//
                .withHeaders(recordHeaders);

        context.forward(originalRecord);
    }

}
