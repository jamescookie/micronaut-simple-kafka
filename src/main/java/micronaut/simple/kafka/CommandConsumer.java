package micronaut.simple.kafka;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.UUID;

@Singleton
class CommandConsumer {
    @Inject
    CustomStateStore customStateStore;

    @KafkaListener(
            groupId = "test",
            offsetReset = OffsetReset.EARLIEST,
            offsetStrategy = OffsetStrategy.AUTO
//            properties = [
//                    @Property(name = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringDeserializer"),
//                    @Property(name = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, value = "io.confluent.kafka.serializers.KafkaAvroDeserializer")
//            ]
    )
    @Topic(patterns = "test-commands")
    void receive(ConsumerRecord<String, String> consumerRecord) {
        System.out.println("reading command = " + consumerRecord);
        String key = UUID.randomUUID().toString();
        customStateStore.put(
                key,
                Event.builder().message(consumerRecord.value()).id(UUID.randomUUID().toString()).build());
        Event event = customStateStore.get(key);
        System.out.println("read event from store = " + event);
    }
}
