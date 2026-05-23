package com.example.webfluxfirst.chapter2;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class BasicFluxMonoTest {

    /**
     * 첫번째, 플럭스와 모노는 크게 데이터 흐름 시작, 데이터 가공, 그리고 구독 이 흐름으로 이루어지며,
     * 두 번째, 플럭스는 0개 이상의 무한정 데이터를 방출 가능하다.
     */

    @Test
    public void testBasicFluxMono() {

        Flux.<Integer>just(1, 2, 3, 4, 5)
                .map(i -> i * 2)
                .filter(i -> i % 4 == 0)
                .subscribe(i -> log.info("Flux 구독 데이터 => {}", i));

        Mono.<Integer>just(2)
                .map(i -> i * 2)
                .filter(i -> i % 4 == 0)
                .subscribe(i -> log.info("Mono 구독 데이터 => {}", i));
    }

    @Test
    public void testFluxMonoBlock() {
        /**
         * 메인 스레드가 접근해서 만들고 바로 생성되는 값이라 blocking 해도 아무런 시간적 손실이 없지만,
         * 만약에 Mono 안에서 다른 스레드가 메인 스레드 대신 시간이 오래 걸리는 blocking 작업을 처리해주고 있다면
         * 메인 스레드도 같이 blocking 된 상태로 10초를 기다리게됨.
         * 하지만 이벤트 루프 스레드는 절대로 blocking 되면 안됨.
         * 스케줄러의 스레드는 어느정도는 blocking 돼도 괜찮지만 피하는게 좋다.
         */
        Mono<String> justString = Mono.just("string1");
        String string = justString.block();

        log.info("string => {}", string);
    }
}
