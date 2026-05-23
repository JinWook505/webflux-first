package com.example.webfluxfirst;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@SpringBootTest
@Slf4j
public class WebClientTest {

    private WebClient webClient = WebClient.builder().build();

    @Test
    public void testWebClient() {
        Flux<Integer> intFlux = webClient.get()
                .uri("http://localhost:8080/reactive/onenine/flux")
                .retrieve()
                .bodyToFlux(Integer.class);

        intFlux.subscribe(data -> {
            System.out.println("Current Thread name => "+ Thread.currentThread().getName());
            System.out.println("WebFlux Subscribe => "+ data);
        });

        System.out.println("thead return to Netty Event loop.");

        try {
            Thread.sleep(5000);
        } catch(Exception ignored) {}
    }
}
