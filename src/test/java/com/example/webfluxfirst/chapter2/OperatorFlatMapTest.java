package com.example.webfluxfirst.chapter2;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Slf4j
public class OperatorFlatMapTest {
    /**
     * Flux<Mono<T>>
     * Mono<Mono<T>> --> 이 구조 안에 있는 Mono 는 flatMap, merge로 벗겨낼 수 있다.
     *               --> flatMap, merge는 순서를 보장하지 않는다. 보장이 필요하다면 xxxSequential 사용
     * Mono<Flux<T>> --> flatMapMany --> Flux<T>의 순서가 보장됨
     * Flux<Flux<T>> ? collectList --> Flux<Mono<List>> --> Flux<List><T>>
     */

    @Test
    public void monoToFlux() {
        Mono<Integer> justMono = Mono.just(1);

        Flux<Integer> integerFlux = justMono.flatMapMany(data -> {
            return Flux.just(data, data + 1, data + 2);
        });
        integerFlux.subscribe(data -> log.info("data => {}", data));
    }

    @Test
    public void testWebClientFlatMap() {

        // 완료된 처리 순서대로 방출 -> 순서 보장 X
        Flux<String> justFlux = Flux.just(callWebclient("1단계 - 문제 이해하기", 1500),
                        callWebclient("2단계 - 문제 단계별로 풀어가기", 1000),
                        callWebclient("3단계 - 최종 응답", 500))
                .flatMap(monoData -> monoData);

        justFlux.subscribe(data -> log.info("FlatMapped data => {}", data));

        // 넣은 순서대로 방출 -> 순서 보장 O
        Flux<String> justSequentialFlux = Flux.just(callWebclient("1단계 - 문제 이해하기", 1500),
                        callWebclient("2단계 - 문제 단계별로 풀어가기", 1000),
                        callWebclient("3단계 - 최종 응답", 500))
                .flatMapSequential(monoData -> monoData);

        justSequentialFlux.subscribe(data -> log.info("FlatMap Sequential data => {}", data));

        // .map(~~)과 같은 처리가 필요없다면 merge 이용 -> 순서 보장 X
        Flux<String> mergeFlux = Flux.merge(callWebclient("1단계 - 문제 이해하기", 1500),
                callWebclient("2단계 - 문제 단계별로 풀어가기", 1000),
                callWebclient("3단계 - 최종 응답", 500));

        mergeFlux.subscribe(data -> log.info("Merged data => {}", data));

        // .map(~~)과 같은 처리가 필요없으며, 넣은 순서대로 방출 -> 순서 보장 O
        Flux<String> mergeSequentialFlux = Flux.mergeSequential(callWebclient("1단계 - 문제 이해하기", 1500),
                callWebclient("2단계 - 문제 단계별로 풀어가기", 1000),
                callWebclient("3단계 - 최종 응답", 500));

        mergeSequentialFlux.subscribe(data -> log.info("Merged Sequential data => {}", data));

        // 단계식으로 비동기 처리가 완료 후 실행됨 -> 순서 보장 O
        Flux<String> concatFlux = Flux.concat(callWebclient("1단계 - 문제 이해하기", 1500),
                callWebclient("2단계 - 문제 단계별로 풀어가기", 1000),
                callWebclient("3단계 - 최종 응답", 500));

        concatFlux.subscribe(data -> log.info("concat data => {}", data));

        try {
            Thread.sleep(10000);
        } catch (Exception ignore) {}
    }

    private Mono<String> callWebclient(String request, long delay) {
        return Mono.defer(() -> {

            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                return Mono.empty();
            }
            return Mono.just(request + " -> delay : " + delay);
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
