package micronaut.simple.kafka;

import io.micronaut.context.annotation.Requires;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.endpoint.health.HealthEndpoint;
import io.micronaut.management.health.indicator.HealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import org.reactivestreams.Publisher;

import javax.inject.Inject;
import javax.inject.Singleton;
 
@Singleton
// Only create bean when configuration property
// endpoints.health.enabled equals true,
// and HealthEndpoint bean to expose /health endpoint is available.
@Requires(property = HealthEndpoint.PREFIX + ".enabled", value = "true", defaultValue = "true")
@Requires(beans = HealthEndpoint.class)
public class OffsetHealthIndicator implements HealthIndicator {

    private static final String NAME = "offset-health";
    private final CustomStateStore customStateStore;

    @Inject
    public OffsetHealthIndicator(CustomStateStore customStateStore) {
        this.customStateStore = customStateStore;
    }
 
    @Override
    public Publisher<HealthResult> getResult() {
        final HealthStatus healthStatus = customStateStore.isReady() ? HealthStatus.UP : HealthStatus.DOWN;
 
        return Publishers.just(HealthResult.builder(NAME, healthStatus)
                           .build());
    }

}