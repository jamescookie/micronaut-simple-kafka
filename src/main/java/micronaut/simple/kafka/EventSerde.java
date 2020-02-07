package micronaut.simple.kafka;

import io.micronaut.configuration.kafka.serde.JsonSerde;

public class EventSerde extends JsonSerde<Event> {
    public EventSerde() {
        super(Event.class);
    }
}
