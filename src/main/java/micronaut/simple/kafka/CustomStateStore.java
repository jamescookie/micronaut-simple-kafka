package micronaut.simple.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.configuration.kafka.serde.JsonSerde;
import io.micronaut.context.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class CustomStateStore {
    private final KafkaConsumer<String, Event> eventConsumer;
    private final MeterRegistry meterRegistry;

    private ConcurrentHashMap<String, String> pendingEvents = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Event> actualEvents = new ConcurrentHashMap<>();
    private AtomicBoolean closing = new AtomicBoolean(false);
    private AtomicBoolean ready = new AtomicBoolean(false);

    @Inject
    CustomStateStore(BlockingOffsetChecker blockingOffsetChecker, CustomConsumerFactory customConsumerFactory, MeterRegistry meterRegistry) {
        this.eventConsumer = customConsumerFactory.createConsumer(new StringDeserializer(), new JsonSerde<>(Event.class));
        this.meterRegistry = meterRegistry;
        blockingOffsetChecker.latestEventIds().forEach(id-> pendingEvents.put(id, id));
        checkReady();
        this.eventConsumer.subscribe(Collections.singletonList("test-events"));
        new Thread(() -> {
            while (!closing.get()) {
                var records = this.eventConsumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Event> record : records) {
                    receive(record);
                }
            }
        }).start();
    }

    public void put(Event event) {
        String eventId = UUID.randomUUID().toString();
        event.setEventId(eventId);
        pendingEvents.put(eventId, eventId);
//        System.out.println("pendingEvents = " + pendingEvents.size());
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
            meterRegistry.counter("state.store.counter").increment();
        } else {
            actualEvents.remove(key);
        }
    }

    private void checkReady() {
        if (!ready.get()) {
            ready.set(pendingEvents.size() == 0);
        }
    }

    public boolean isReady() {
        return ready.get();
    }

    @PreDestroy
    public void close() {
        closing.set(true);
    }
}
