package micronaut.simple.kafka;

import io.micronaut.context.annotation.Value;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

public class CustomConsumerFactory {
    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    public <K, V> KafkaConsumer<K, V> createConsumer(Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        return new KafkaConsumer<>(createConsumerConfig(bootstrapServers), keyDeserializer, valueDeserializer);
    }

    private static Properties createConsumerConfig(@NonNull final String bootstrapServers) {
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
}
