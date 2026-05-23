package com.example.webfluxfirst.chapter2;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class BasicMonoOperatorTest {

    /**
     * Mono의 흐름 시작 방법
     *
     * 1. 데이터로부터 시작 -> 일반적인 경우 just / 특이한 상황 = empty (Optional.empty())
     * 2. 함수로부터 시작 ->
     *      동기적인 객체를 Mono로 반환하고 싶을 때 fromCallable / 코드의 흐름을 Mono 안에서 관리하고, Mono를 반환하고 싶을 때
     */

    // just, empty
    @Test
    public void startMonoFromData() {
        Mono.just(1).subscribe(data -> log.info("data => {}", data));

        // ex 사소한 에러 발생 시 로그만 남기고 empty인 Mono를 전파할 때 쓸 수 있음.
        Mono.empty().subscribe(data -> log.info("data => {}", data));
    }

    // fromCallable, defer

    /**
     * fromCallable -> 동기적인 객체를 반환할 때 사용
     * defer -> Mono를 반환하고 싶을 때 사용
     */
    @Test
    public void startMonoFromFunction() {
        /**
         * 임시 마이그레이션
         * restTemplate, JPA 등 blocking 발생 라이브러리 Mono로 스레드 분리하여 처리
         */
        Mono<String> fromCallable = Mono.fromCallable(() -> {
            // 로직 실행

            return callRestTemplate("동기객체");
        }).subscribeOn(Schedulers.boundedElastic());



        Mono<String> monoFromJust = Mono.just("안녕하세요."); // 즉시생성.

        Mono<String> monoFromDefer = Mono.defer(() ->
                callWebclient("안녕하세요."));

        monoFromDefer.subscribe(); // defer는 여기서 생성됨.
    }

    @Test
    public void testDeferNecessity() {
        Mono<String> defer = Mono.defer(() -> {
            // 하나의 큰 흐름을 하나의 Mono 안에서 관리하고 싶을 때 defer 사용
            String a = "안녕";
            String b = "하세";
            String c = "요";

            return callWebclient(a + b + c);
        });
    }

    // Mono에서 데이터 방출의 개수가 많아져 Flux로 변환이 필요할 때 flatMapMany 사용.
    @Test
    public void monoToFlux() {
        Mono<Integer> justMono = Mono.just(1);

        Flux<Integer> integerFlux = justMono.flatMapMany(data -> {
            return Flux.just(data, data + 1, data + 2);
        });

        integerFlux.subscribe(data -> log.info("data => {}", data));
    }

    private Mono<String> callWebclient(String request) {
        return Mono.just(request + "callWebclient 응답");
    }

    private String callRestTemplate(String request) {
        return request + "callREstTemplate 응답";
    }


}
