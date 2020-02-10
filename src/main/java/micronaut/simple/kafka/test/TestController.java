package micronaut.simple.kafka.test;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;

import javax.inject.Inject;
import java.util.UUID;

@Controller("/test")
public class TestController {

    @Inject
    CommandProducer commandProducer;

    @Get("/send/{message}")
    @Produces(MediaType.TEXT_PLAIN)
    public String send(String message) {
        for (int i = 0; i < 1_000_000; i++) {
            if (i % 10000 == 0) System.out.println("done: " + i);
            String key = UUID.randomUUID().toString();
            commandProducer.sendCommand(key, message);
        }
        return "sent";
    }
}