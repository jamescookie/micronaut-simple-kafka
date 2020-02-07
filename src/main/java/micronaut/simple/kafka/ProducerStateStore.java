package micronaut.simple.kafka;

import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Custom state store that uses a producer to write to the topic.
 * @param <K> The key type of the custom state store.
 * @param <V> The value type of the custom state store.
 */
//public class ProducerStateStore<K, V extends GetEventId> implements CustomStateStore<K, V> {
public class ProducerStateStore {
//
//    /**
//     * The number of times to retry getting a value for a key.
//     */
//    private static final int GET_RETRY_COUNT = 1000;
//
//    /**
//     * The name of the event topic that backs this state store.
//     */
//    @NonNull
//    private final String eventTopicName;
//
//    /**
//     * Flag to indicate whether the consumer is closing.
//     */
//    @NonNull
//    private final AtomicBoolean closing = new AtomicBoolean(false);
//
//    /**
//     * Flag to indicate whether the consumer is started.
//     */
//    @NonNull
//    private final AtomicBoolean started = new AtomicBoolean(false);
//
//    /**
//     * Flag to indicate whether this state store is ready.
//     */
//    @NonNull
//    private final AtomicBoolean ready = new AtomicBoolean(false);
//
//    /**
//     * Backing store for the data (events) of the topic.
//     */
//    @NonNull
//    private final ConcurrentHashMap<K, V> dataStore = new ConcurrentHashMap<>();
//
//    /**
//     * Backing store for the offsets that are processed by this state store.
//     */
//    @NonNull
//    private final ConcurrentHashMap<UUID, UUID> eventIdStore = new ConcurrentHashMap<>();
//
//    /**
//     * Kafka consumer that reads the data (in it's own thread).
//     */
//    @NonNull
//    private final KafkaConsumer<K, V> dataConsumer;
//
//    @NonNull
//    private final KafkaConsumer<K, V> initConsumer;
//
//    /**
//     * Kafka producer that writes the data to the topic.
//     */
//    @NonNull
//    private final Producer<K, V> producer;
//
//    /**
//     * Create a new instance of this class.
//     *
//     * @param bootstrapServers The Kafka bootstrap servers.
//     * @param eventTopicName The name of the event topic that backs this state store.
//     * @param keyDeserializer The deserializer for the key.
//     * @param valueDeserializer The deserializer for the value.
//     * @param keySerializer The serializer for the key.
//     * @param valueSerializer The serializer for the value.
//     * @throws NullPointerException if the bootstrapServers is null.
//     * @throws NullPointerException if the eventtopicName is null.
//     * @throws NullPointerException if the keyDeserializer is null.
//     * @throws NullPointerException if the valueDeserializer is null.
//     * @throws NullPointerException if the keySerializer is null.
//     * @throws NullPointerException if the valueSerializer is null.
//     */
//    public ProducerStateStore(
//            @NonNull final String bootstrapServers,
//            @NonNull final String eventTopicName,
//            @NonNull final Deserializer<K> keyDeserializer,
//            @NonNull final Deserializer<V> valueDeserializer,
//            @NonNull final Serializer<K> keySerializer,
//            @NonNull final Serializer<V> valueSerializer
//    ) {
//        this.eventTopicName = eventTopicName;
//        this.dataConsumer = new KafkaConsumer<>(createConsumerConfig(bootstrapServers), keyDeserializer, valueDeserializer);
//        this.dataConsumer.subscribe(Collections.singletonList(this.eventTopicName));
//        this.dataConsumer.poll(Duration.ZERO);
//        this.dataConsumer.seekToBeginning(this.dataConsumer.assignment());
//        this.initConsumer = new KafkaConsumer<>(createConsumerConfig(bootstrapServers), keyDeserializer, valueDeserializer);
//        this.initConsumer.subscribe(Collections.singletonList(this.eventTopicName));
//        this.initConsumer.poll(Duration.ZERO);
//        this.producer = new KafkaProducer<>(createProducerConfig(bootstrapServers), keySerializer, valueSerializer);
//        this.producer.initTransactions();
//    }
//
//    /**
//     * Delete the value with the specified key.
//     *
//     * @param key The key of the value to delete.
//     * @throws NullPointerException if the key is null.
//     */
//    @Override
//    public void delete(@NonNull final K key) {
//        this.producer.beginTransaction();
//        this.producer.send(new ProducerRecord<>(this.eventTopicName, key, null));
//        this.producer.commitTransaction();
//    }
//
//    /**
//     * Try to get the value by the key, returns null if not found.
//     *
//     * @param key The key to get the value for.
//     * @return The value associated with the key or null if not found.
//     * @throws NullPointerException if the key is null.
//     * @throws TimeoutException if the local state store is not up to date after 10 seconds.
//     */
//    @Override
//    public V get(@NonNull final K key) {
//        var retries = 0;
//        while (!this.eventIdStore.isEmpty()) {
//            if (retries < GET_RETRY_COUNT) {
//                retries += 1;
//                sleep10Millis();
//            } else {
//                throw new TimeoutException("Bringing state store up to date took too long");
//            }
//        }
//        return this.dataStore.get(key);
//    }
//
//    public boolean isReady() {
//        return this.ready.get();
//    }
//
//    /**
//     * Store the key value pair.
//     *
//     * @param key The key to store.
//     * @param value The value to store.
//     * @throws NullPointerException if the key is null.
//     * @throws NullPointerException if the value is null.
//     */
//    @Override
//    public void put(@NonNull final K key, @NonNull final V value) {
//        final var eventId = value.getEventId();
//        this.eventIdStore.put(eventId, eventId);
//        this.producer.beginTransaction();
//        this.producer.send(new ProducerRecord<>(this.eventTopicName, key, value));
//        this.producer.commitTransaction();
//    }
//
//    /**
//     * Shutdown this state store and close the connections with Kafka.
//     */
//    @Override
//    public void shutdown() {
//        this.producer.close();
//        this.ready.set(false);
//        this.closing.set(true);
//        this.dataConsumer.wakeup();
//    }
//
//    /**
//     * Start this state store.
//     */
//    public void start() {
//        if (!this.started.get()) {
//            this.started.set(true);
//            var assignments = this.getAssignment();
//            var endOffsets = this.initConsumer.endOffsets(assignments);
//            this.initConsumer.pause(assignments);
//            for(var endOffset : endOffsets.entrySet()) {
//                retrieveLastEventId(endOffset.getKey(), endOffset.getValue());
//            }
//
//            this.loadInitialData(assignments);
//
//            for(var pair : this.dataStore.entrySet()) {
//                System.out.println("Key: " + pair.getKey() + ", Value: " + pair.getValue());
//            }
//
//            new Thread(this::run).start();
//
//            this.ready.set(true);
//        }
//    }
//
//    private Set<TopicPartition> getAssignment() {
//        var assignments = this.initConsumer.assignment();
//        while (assignments.isEmpty()) {
//            this.initConsumer.poll(Duration.ofMillis(10));
//            assignments = this.initConsumer.assignment();
//        }
//
//        System.out.println("Assignments: " + assignments);
//        return assignments;
//    }
//
//    private void retrieveLastEventId(TopicPartition partition, Long endOffset) {
//        var currentOffset = endOffset;
//        var found = false;
//        this.initConsumer.resume(Collections.singletonList(partition));
//        while(currentOffset > 0 && !found) {
//            currentOffset -= 1;
//            this.initConsumer.seek(partition, currentOffset);
//            var records = this.initConsumer.poll(Duration.ofMillis(100));
//            if (records != null) {
//                for (var record : records) {
//                    if (record != null) {
//                        var value = record.value();
//                        if (value != null) {
//                            var eventId = value.getEventId();
//                            if (eventId != null) {
//                                this.eventIdStore.put(eventId, eventId);
//                                found = true;
//                            }
//                        }
//                    }
//                }
//            }
//        }
//        this.initConsumer.pause(Collections.singletonList(partition));
//    }
//
//    private void loadInitialData(Collection<TopicPartition> assignments) {
//        this.initConsumer.seekToBeginning(assignments);
//        this.initConsumer.resume(assignments);
//
//        while(!this.eventIdStore.isEmpty()) {
//            var records = this.initConsumer.poll(Duration.ofMillis(10));
//            if (records != null) {
//                for (var record : records) {
//                    if (record != null) {
//                        var key = record.key();
//                        if (key != null) {
//                            var value = record.value();
//                            if (value != null) {
//                                this.dataStore.put(key, value);
//                                var eventId = value.getEventId();
//                                if (eventId != null) {
//                                    this.eventIdStore.remove(eventId);
//                                }
//                            } else {
//                                this.dataStore.remove(key);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    /**
//     * Run the data consumer to update the data store.
//     */
//    private void run() {
//        try {
//            while (!this.closing.get()) {
//                var records = this.dataConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
//                if (records != null) {
//                    for (var record : records) {
//                        if (record != null) {
//                            final var key = record.key();
//                            final var value = record.value();
//                            if (value != null) {
//                                this.dataStore.put(key, value);
//                                final var eventId = value.getEventId();
//                                if (eventId != null) {
//                                    this.eventIdStore.remove(eventId);
//                                }
//                            } else {
//                                this.dataStore.remove(key);
//                            }
//                        }
//                    }
//                }
//            }
//        } catch (WakeupException e) {
//            // Ignore exception if closing
//            if (!this.closing.get()) {
//                throw e;
//            }
//        } finally {
//            this.dataConsumer.close();
//        }
//    }
//
//    /**
//     * Create a consumer configuration based on the supplied bootstrap servers.
//     *
//     * @param bootstrapServers The Kafka bootstrap servers.
//     * @return The consumer configuration.
//     */
//    @NonNull
//    private static Properties createConsumerConfig(@NonNull final String bootstrapServers) {
//        var config = new Properties();
//        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        config.put(ConsumerConfig.CLIENT_ID_CONFIG, getUniqueHostName());
//        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        config.put(ConsumerConfig.GROUP_ID_CONFIG, getUniqueConsumerGroupId()); // Hack to always start from scratch
//        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
//        return config;
//    }
//
//    /**
//     * Create a producer configuration based on the supplied bootstrap servers.
//     *
//     * @param bootstrapServers The Kafka bootstrap servers.
//     * @return The producer configuration.
//     */
//    @NonNull
//    private static Properties createProducerConfig(@NonNull final String bootstrapServers) {
//        var config = new Properties();
//        config.put(ProducerConfig.ACKS_CONFIG, "all");
//        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        config.put(ProducerConfig.CLIENT_ID_CONFIG, getUniqueHostName());
//        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
//        return config;
//    }
//
//    /**
//     * Get a unique group identifier for a consumer.
//     *
//     * @return the unique identifier.
//     */
//    private static String getUniqueConsumerGroupId() {
//        return "state-store-consumer-" + UUID.randomUUID();
//    }
//
//    /**
//     * Get a unique version of the hostname (appends UUID to the hostname).
//     *
//     * @return the unique hostname.
//     */
//    private static String getUniqueHostName() {
//        try {
//            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID();
//        } catch (UnknownHostException e) {
//            return "localhost-" + UUID.randomUUID();
//        }
//    }
//
//    /**
//     * Sleep the current thread for 10 milliseconds and ignore possible InterruptedExceptions.
//     */
//    private static void sleep10Millis() {
//        try {
//            Thread.sleep(10);
//        } catch(InterruptedException e) {
//            // Do nothing
//        }
//    }
}