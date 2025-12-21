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
package io.github.aneudeveloper.func.delay;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class TopicSelector {
    private List<WaitTopic> waitTopics;
    private String revokeTopic;
    private Supplier<Long> currentTimeSupplier;

    public TopicSelector(Properties properties) {
        this.setupIntervals(properties.getProperty(DelayService.DELAY_WAIT_TOPICS),
                properties.getProperty(DelayService.DELAY_WAIT_TOPICS_PREFIX));
        this.revokeTopic = properties.getProperty(DelayService.DELAY_REVOKE_TOPIC);
        this.currentTimeSupplier = () -> System.currentTimeMillis();
    }

    public void setCurrentTimeSupplier(Supplier<Long> currentTimeSupplier) {
        this.currentTimeSupplier = currentTimeSupplier;
    }

    public String selectTopic(long nextRetryAt) {
        Long currentTime = this.currentTimeSupplier.get();
        if (this.shouldWait(nextRetryAt, currentTime)) {
            long timeToWaitMillis = nextRetryAt - currentTime;
            List<WaitTopic> allConfiguredWaitTopics = this.getWaitTopics();
            WaitTopic selectedWaitTime = this.getWaitTopics().get(0);
            for (WaitTopic waitTime : allConfiguredWaitTopics) {
                if (timeToWaitMillis <= waitTime.waitTime) {
                    return selectedWaitTime.topicName;
                }
                selectedWaitTime = waitTime;
            }
            if (selectedWaitTime == null) {
                throw new IllegalStateException("Could not find topic for nextRetryAt=" + nextRetryAt
                        + " timeToWaitMillis was " + timeToWaitMillis);
            }
            return selectedWaitTime.topicName;
        }
        return this.revokeTopic;
    }

    private boolean shouldWait(long nextRetryAt, long currentTime) {
        long timeToWaitMillis = nextRetryAt - currentTime;
        WaitTopic smallestWaitTime = this.getSmallestWaitTime();
        return timeToWaitMillis > smallestWaitTime.getWaitTime();
    }

    private WaitTopic getSmallestWaitTime() {
        WaitTopic smallestWaitTime = this.getWaitTopics().get(0);
        return smallestWaitTime;
    }

    private void setupIntervals(String waitTopicsConfig, String waitTopicPrefix) {
        this.waitTopics = new ArrayList<WaitTopic>();
        String[] split = waitTopicsConfig.split(",");
        for (int i = 0; i < split.length; ++i) {
            String timeDefinition = split[i].trim();
            Object waitNumber = "";
            Object type = "";
            for (int j = 0; j < timeDefinition.length(); ++j) {
                char currentChar = timeDefinition.charAt(j);
                if (Character.isDigit(currentChar)) {
                    waitNumber = (String) waitNumber + currentChar;
                    continue;
                }
                type = (String) type + currentChar;
            }
            int waitNumberInt = Integer.parseInt((String) waitNumber);
            long waitMillis = 0L;
            if ("S".equalsIgnoreCase((String) type)) {
                waitMillis = waitNumberInt * 1000;
            } else if ("M".equalsIgnoreCase((String) type)) {
                waitMillis = waitNumberInt * 1000 * 60;
            } else if ("H".equalsIgnoreCase((String) type)) {
                waitMillis = waitNumberInt * 1000 * 60 * 60;
            }
            this.waitTopics.add(new WaitTopic(waitMillis, timeDefinition, waitTopicPrefix + timeDefinition));
        }
        this.waitTopics.sort((arg0, arg1) -> arg0.waitTime.compareTo(arg1.waitTime));
    }

    public List<WaitTopic> getWaitTopics() {
        return this.waitTopics;
    }

    public static class WaitTopic {
        private Long waitTime;
        private String timeDefinition;
        private String topicName;

        public WaitTopic(Long waitTime, String timeDefinition, String topicName) {
            this.waitTime = waitTime;
            this.topicName = topicName;
            this.timeDefinition = timeDefinition;
        }

        public Long getWaitTime() {
            return this.waitTime;
        }

        public String getTimeDefinition() {
            return this.timeDefinition;
        }

        public String getTopicName() {
            return this.topicName;
        }
    }
}
