package micronaut.simple.kafka;

import io.micronaut.configuration.kafka.serde.JsonSerde;
import io.micronaut.context.annotation.Value;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class CustomStateStore {
    private final KafkaConsumer<String, Event> eventConsumer;

    private ConcurrentHashMap<String, String> pendingEvents = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Event> actualEvents = new ConcurrentHashMap<>();
    private AtomicBoolean closing = new AtomicBoolean(false);
    private AtomicBoolean ready = new AtomicBoolean(false);

    @Inject
    CustomStateStore(BlockingOffsetChecker blockingOffsetChecker, @Value("${kafka.bootstrap.servers}") String bootstrapServers) {
        this.eventConsumer = new KafkaConsumer<>(createConsumerConfig(bootstrapServers), new StringDeserializer(), new JsonSerde<>(Event.class));
        blockingOffsetChecker.latestEventIds().forEach(id-> pendingEvents.put(id, id));
        checkReady();
        this.eventConsumer.subscribe(Collections.singletonList("test-events"));
        new Thread(() -> {
            while (!this.closing.get()) {
                var records = this.eventConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Event> record : records) {
                    receive(record);
                }
            }
        }).start();
    }

    public static Properties createConsumerConfig(@NonNull final String bootstrapServers) {
        var config = new Properties();
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, getUniqueHostName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, getUniqueConsumerGroupId()); // Hack to always start from scratch
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return config;
    }

    private static String getUniqueConsumerGroupId() {
        return "state-store-consumer-" + UUID.randomUUID();
    }

    private static String getUniqueHostName() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            return "localhost-" + UUID.randomUUID();
        }
    }

    public void put(Event event) {
        String eventId = UUID.randomUUID().toString();
        event.setEventId(eventId);
        pendingEvents.put(eventId, eventId);
        System.out.println("pendingEvents = " + pendingEvents.size());
    }

    public Event get(String key) {
        while (!pendingEvents.isEmpty()) {
            try {
                System.out.println("Sleeping");
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return actualEvents.get(key);
    }

    private void receive(ConsumerRecord<String, Event> consumerRecord) {
        String key = consumerRecord.key();
        Event value = consumerRecord.value();
        if (value != null) {
            actualEvents.put(key, value);
            pendingEvents.remove(value.getEventId());
            checkReady();
        } else {
            actualEvents.remove(key);
        }
    }

    private void checkReady() {
        this.ready.set(this.pendingEvents.size() == 0);
        if (ready.get()) {
            System.out.println("~~~~~~~ READY ~~~~~~~~");
        }
    }

    @PreDestroy
    public void close() {
        this.closing.set(true);
    }
}
