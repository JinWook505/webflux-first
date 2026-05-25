package com.example.webfluxfirst.chapter2;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class SchedulerTest {

    /**
     * scheduler 할당 가능 시점 : subscribe, publish
     */
    @Test
    public void testBasicFluxMono() {

        Mono.<Integer>just(2)
                .map(i -> {
                    log.info("map Thread Name => {}", Thread.currentThread().getName());
                    return i * 2;
                })
                .publishOn(Schedulers.parallel())
                .filter(i -> {
                    log.info("filter Thread Name => {}", Thread.currentThread().getName());
                    return i % 4 == 0;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(i -> log.info("Mono 구독 데이터 => {}", i));
    }
}
