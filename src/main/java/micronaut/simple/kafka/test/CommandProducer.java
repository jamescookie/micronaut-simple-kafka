package micronaut.simple.kafka.test;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;

@KafkaClient()
@Topic("test-commands")
interface CommandProducer {
    void sendCommand(@KafkaKey String key, String value);
}
