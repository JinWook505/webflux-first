package com.example.webfluxfirst.chapter2;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BasicFluxOperatorTest {

    /**
     * Flux
     *
     * 데이터로 부터 시작 : just, empty, fromXXX시리즈
     * 함수로 부터 시작 : defer, create
     */

    @Test
    public void testFluxFromData() {
        Flux.just(1, 2, 3, 4, 5)
                .subscribe(data -> log.info("data from just => {}", data));

        Flux.empty()
                .subscribe(data -> log.info("data => {}", data));

        List<Integer> basicList = List.of(1, 2, 3, 4, 5);

        Flux.fromIterable(basicList)
                .subscribe(data -> log.info("data from iterable => {}", data));
    }

    /**
     * Flux defer -> 안에서 Flux 객체를 반환해야함.
     * Flux create -> 안에서 동기적인 객체를 반환해야함.
     */
    @Test
    public void testFluxFromFunction() {
        Flux.defer(() -> Flux.just(1, 2, 3, 4, 5))
                .subscribe(data -> log.info("data from defer => {}", data));

        Flux.create(sink -> {
            sink.next(1);
            sink.next(2);
            sink.next(3);

            sink.complete(); // sink 사용할 때 마지막에 호출
        }).subscribe(data -> log.info("data from create => {}", data));
    }

    @Test
    public void testSinkDetail() {
        Flux.<String>create(sink ->
                        {
                            recursiveFunction(sink);
                            recursiveFunction(sink);
                            recursiveFunction(sink);
                        }
                )
                // Tomcat ThreadLocal -> FluxSink.context
                .contextWrite(Context.of("counter", new AtomicInteger(0)))
                .subscribe(data -> log.info("data from sink detail => {}", data));
    }

    // Flux -> Mono 변환 = collectList
    @Test
    public void testFluxCollectList() {
        Mono<List<Integer>> listMono = Flux.<Integer>just(1, 2, 3, 4, 5)
                .map(i -> i * 2)
                .filter(i -> i % 4 == 0)
                .collectList();

        listMono.subscribe(data -> log.info("collectList가 변환한 list data => {}", data));
    }

    private void recursiveFunction(FluxSink<String> sink) {
        AtomicInteger counter = sink.contextView().get("counter");

        if (counter.incrementAndGet() < 10) {
            sink.next("sink count : " + counter);
            recursiveFunction(sink);
        } else {
            sink.complete();
        }
    }
}
