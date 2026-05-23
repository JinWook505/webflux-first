package com.example.webfluxfirst.chapter1;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@SpringBootTest
@Slf4j
public class SubscriberPublishAsyncTest {

    @Test
    public void produceOneToNineFlux() {
        Flux<Integer> intFlux = Flux.<Integer>create(sink -> {
            for (int i = 1; i <= 9; i++) {

                try {
                    Thread.sleep(500);
                } catch(Exception ignored) {}

                sink.next(i);
            }
            sink.complete();
        }).subscribeOn(Schedulers.boundedElastic());

        intFlux.subscribe(data -> {
            log.info("Current Thread name => {}", Thread.currentThread().getName());
            log.info("WebFlux Subscribe => {}", data);
        });

        log.info("thead return to Netty Event loop.");

        try {
            Thread.sleep(5000);
        } catch(Exception ignored) {}
    }
}
