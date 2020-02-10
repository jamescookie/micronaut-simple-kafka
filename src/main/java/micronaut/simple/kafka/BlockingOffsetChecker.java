package micronaut.simple.kafka;

import io.micronaut.configuration.kafka.serde.JsonSerde;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Singleton
public class BlockingOffsetChecker {

    private final CustomConsumerFactory customConsumerFactory;
    private List<String> eventIds = new ArrayList<>();

    @Inject
    BlockingOffsetChecker(CustomConsumerFactory customConsumerFactory) {
        this.customConsumerFactory = customConsumerFactory;
        readOffsets();
    }

    public void readOffsets() {
        KafkaConsumer<String, Event> initConsumer = customConsumerFactory.createConsumer(new StringDeserializer(), new JsonSerde<>(Event.class));
        initConsumer.subscribe(Collections.singletonList("test-events"));

        var assignments = getAssignment(initConsumer);
        var endOffsets = initConsumer.endOffsets(assignments);
        initConsumer.pause(assignments);
        for (var endOffset : endOffsets.entrySet()) {
            retrieveLastEventId(initConsumer, endOffset.getKey(), endOffset.getValue());
        }
        initConsumer.close();
    }

    public List<String> latestEventIds() {
        System.out.println("blocking finished. EventIds = " + eventIds);
        return eventIds;
    }

    private Set<TopicPartition> getAssignment(KafkaConsumer<String, Event> initConsumer) {
        var assignments = initConsumer.assignment();
        while (assignments.isEmpty()) {
            initConsumer.poll(Duration.ofMillis(10));
            assignments = initConsumer.assignment();
        }

        System.out.println("Assignments: " + assignments);
        return assignments;
    }


    private void retrieveLastEventId(KafkaConsumer<String, Event> initConsumer, TopicPartition partition, Long endOffset) {
        var currentOffset = endOffset;
        var found = false;
        initConsumer.resume(Collections.singletonList(partition));
        while (currentOffset > 0 && !found) {
            currentOffset -= 1;
            initConsumer.seek(partition, currentOffset);
            var records = initConsumer.poll(Duration.ofMillis(100));
            if (records != null) {
                for (var record : records) {
                    if (record != null) {
                        var value = record.value();
                        if (value != null) {
                            var eventId = value.getEventId();
                            if (eventId != null) {
                                eventIds.add(eventId);
                                found = true;
                            }
                        }
                    }
                }
            }
        }
        initConsumer.pause(Collections.singletonList(partition));
    }


}
