/**
* Copyright 2022 aneuDeveloper
* 
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the * "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
* 
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
* 
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
* 
*/
package io.github.aneudeveloper.func.delay;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageDeserializer {
    private static final Logger LOG = LoggerFactory.getLogger(MessageDeserializer.class);
    public static final String VERSION = "1";
    public static final String META_SEPARATOR = "$e%,";
    public static final String META_PROP_SEPARATOR = ",";
    public static final String META_PROP_KEY_SEPARATOR = "=";
    public static final String NEXT_RETRY_AT_KEY = "nextRetryAt";
    public static final String SOURCE_TOPIC_KEY = "sourceTopic";

    public String getAsString(String key, String data) {
        return getValue(key, data);
    }

    public Long getExecutionDateAsMillis(String data) {
        String dataAsMillis = getValue(MessageDeserializer.NEXT_RETRY_AT_KEY, data);
        try {
            return Long.parseLong(dataAsMillis);
        } catch (Exception e) {
            LOG.error("Could not parse nextRetryAt", e);
            return null;
        }
    }

    private String getValue(String key, String data) {
        CharacterIterator it = new StringCharacterIterator(data);

        String currentKey = null;
        StringBuilder builder = new StringBuilder();
        while (it.current() != CharacterIterator.DONE) {
            if (it.current() == ',' || it.current() == '$') {
                if (key.equalsIgnoreCase(currentKey)) {
                    return builder.toString();
                }
                currentKey = null;
                builder.setLength(0);
            } else if (it.current() == '=') {
                currentKey = builder.toString();
                builder.setLength(0);
            } else {
                builder.append(it.current());
            }
            it.next();
        }

        return "";
    }
}
