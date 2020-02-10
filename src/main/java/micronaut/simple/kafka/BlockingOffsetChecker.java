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

    private final KafkaConsumer<String, Event> initConsumer;
    private List<String> eventIds = new ArrayList<>();

    @Inject
    BlockingOffsetChecker(CustomConsumerFactory customConsumerFactory) {
        this.initConsumer = customConsumerFactory.createConsumer(new StringDeserializer(), new JsonSerde<>(Event.class));
        this.initConsumer.subscribe(Collections.singletonList("test-events"));

        var assignments = this.getAssignment();
        var endOffsets = this.initConsumer.endOffsets(assignments);
        this.initConsumer.pause(assignments);
        for (var endOffset : endOffsets.entrySet()) {
            retrieveLastEventId(endOffset.getKey(), endOffset.getValue());
        }
        this.initConsumer.close();
    }

    public List<String> latestEventIds() {
        System.out.println("blocking finished eventIds = " + eventIds);
        return eventIds;
    }

    private Set<TopicPartition> getAssignment() {
        var assignments = this.initConsumer.assignment();
        while (assignments.isEmpty()) {
            this.initConsumer.poll(Duration.ofMillis(10));
            assignments = this.initConsumer.assignment();
        }

        System.out.println("Assignments: " + assignments);
        return assignments;
    }


    private void retrieveLastEventId(TopicPartition partition, Long endOffset) {
        var currentOffset = endOffset;
        var found = false;
        this.initConsumer.resume(Collections.singletonList(partition));
        while (currentOffset > 0 && !found) {
            currentOffset -= 1;
            this.initConsumer.seek(partition, currentOffset);
            var records = this.initConsumer.poll(Duration.ofMillis(100));
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
        this.initConsumer.pause(Collections.singletonList(partition));
    }


}
