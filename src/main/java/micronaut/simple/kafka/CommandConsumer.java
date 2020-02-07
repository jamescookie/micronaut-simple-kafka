package micronaut.simple.kafka;

import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Properties;
import java.util.UUID;

@Factory
class CommandConsumer {
    @Inject
    CustomStateStore customStateStore;

    @Singleton
    @Named("test-commands")
    KStream<String, Event> commandStream(ConfiguredStreamBuilder builder) {
        Properties props = builder.getConfiguration();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStream<String, String> source = builder.stream("test-commands",
                Consumed.with(getKeySerde(String.class), getValueSerde(String.class)));

        KStream<String, Event> stream = source
                .mapValues(v -> Event.builder().message(v).id(UUID.randomUUID().toString()).build());//this would be the processing

        stream
                .peek((k, v) -> customStateStore.put(v))
                .to("test-events");

        return stream;
    }

    private <K> Serde<K> getKeySerde(Class<K> keyType) {
        return Serdes.serdeFrom(keyType);
    }

    private <V> Serde<V> getValueSerde(Class<V> valueType) {
        return Serdes.serdeFrom(valueType);
    }

}
