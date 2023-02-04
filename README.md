# func-delay

### How to use

#### Import the library 

```xml
<dependency>
	<groupId>io.github.aneudeveloper.func</groupId>
	<artifactId>func-delay</artifactId>
	<version>1.0.0</version>
</dependency>
```

#### Start kafka stream
How to start the delay service.
```java

    Properties properties = new Properties();
    properties.put(RetryService.KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092");
    properties.put(RetryService.TOPIC_DEFAULT_REPLICATION_FACTOR, "1");
    properties.put(RetryService.TOPIC_DEFAULT_NUM_PARTITIONS, "3");

    RetryService retryService = new RetryService(properties,
            new UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread thread, Throwable exception) {
                    // Shutdown and restart if unexpected exception occur.
                    System.exit(1);
                }
            });
    
    retryService.start();

```

