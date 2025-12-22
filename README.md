# func-delay

### How to use

#### Import the library 

```xml
<dependency>
	<groupId>io.github.aneudeveloper.func</groupId>
	<artifactId>func-delay</artifactId>
</dependency>
```

#### Start kafka stream
How to start the delay service.
```java

    Properties properties = new Properties();
    properties.put(DelayService.KAFKA_BOOTSTRAP_SERVERS, "127.0.0.1:9092");
    properties.put(DelayService.TOPIC_DEFAULT_REPLICATION_FACTOR, "1");
    properties.put(DelayService.TOPIC_DEFAULT_NUM_PARTITIONS, "3");

    DelayService delayService = new DelayService(properties,
            new UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread thread, Throwable exception) {
                    // Shutdown and restart if unexpected exception occur.
                    System.exit(1);
                }
            });
    
    delayService.start();

```

#### Configuration
DELAY_WAIT_TOPICS example 15S, 1M, 5M, 15M, 1H, 12H

#### Integration Tests
Run kafka locally at port 9092 before executing integration tests.

