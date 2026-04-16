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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(DelayToWaitProcessor.class);

    public static String getHeader(Headers headers, String headerKey) {
        try {
            if (headers != null) {
                Iterable<Header> headerIterator = headers.headers(headerKey);
                if (headerIterator != null && headerIterator.iterator() != null) {
                    Header next = headerIterator.iterator().next();
                    if (next != null && next.value() != null) {
                        String headerValueAsString = new String(next.value());
                        return headerValueAsString;
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("could not get header for headerKey={} continue processing", headerKey, e);
            return null;
        }

        LOG.warn("headerKey={} was not defined in message", headerKey);
        return null;
    }
}
