package micronaut.simple.kafka;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Property;
import org.apache.kafka.clients.producer.ProducerConfig;

@KafkaClient(
//        properties = [
//                @Property(name = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer"),
//                @Property(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value = "io.confluent.kafka.serializers.KafkaAvroSerializer")
//        ]
)
public
interface EventProducer {
    void sendEvent(@Topic String topic, @KafkaKey String key, Event value);
}
