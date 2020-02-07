package micronaut.simple.kafka.test;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.OffsetStrategy;
import io.micronaut.configuration.kafka.annotation.Topic;
import micronaut.simple.kafka.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.inject.Singleton;

@Singleton
class TestEventConsumer {
//    @KafkaListener(
//            groupId = "test",
//            offsetReset = OffsetReset.EARLIEST,
//            offsetStrategy = OffsetStrategy.AUTO
//    )
//    @Topic("test-events")
//    void receive(ConsumerRecord<String, Event> consumerRecord) {
//        System.out.println("got: " + consumerRecord.value());
//    }
}
