package com.example.webfluxfirst.chapter2;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
public class FluxMonoErrorAndSignalTest {

    @Test
    public void testBasicSignal() {
        Flux.just(1, 2, 3, 4)
                .doOnNext(publishedData -> log.info("publishedData => {}", publishedData))
                .doOnComplete(() -> log.info("Stream Exit."))
                .doOnError(ex -> log.error("exception catch!!! ", ex))
                .subscribe(data -> log.info("data => {}", data));

    }

    @Test
    public void testFluxMonoError() {
        try {
            Flux.just(1, 2, 3, 4)
                    .map(data -> {
                        if (data == 3) {
                            throw new RuntimeException();
                        }
                        return data * 2;
                    })
                    /**
                     * onErrorComplete():: onError signal을 onComplete signal로 변경
                     * onErrorContinue(BiConsumer<Throwable, Object>): 해당 Element 스킵 후 다음 element로 로직 계속 진행.
                     *  -> consumer에서 적절한 처리 지정.
                     *  -> Mono.onErrorContinue는 upstream에 존재하는 Flux에게 configuration을 전파하기 위함
                     * onErrorMap(): Exception을 다른 Exception으로 타입 전환
                     * onErrorResume(Function<Throwable, Mono<T>>): 지정된 Fallback method 실행
                     * onErrorReturn(T): 예외 발생한 Element를 Fallback Value로 대체
                     */
//                    .onErrorMap(e -> new IllegalArgumentException())
//                    .onErrorReturn(999)
//                    .onErrorComplete()
                    .subscribe(data -> log.info("data => {}", data));
        } catch(Exception e) {
            log.error("exception!!!");
        }
    }

    @Test
    public void testFluxMonoDotError() {
        Flux.just(1, 2, 3, 4, 5)
                .flatMap(data -> {
                    if (data != 3) {
                        return Mono.just(data);
                    } else {
                        // throw는 리액티브 스트림 내부 코드에서 error signal로 바꾸어 처리
//                        throw new RuntimeException();
                        // Mono.error로 감싸 exception을 던지면 직접적으로 error signal 발생시킴
                        return Mono.error(new RuntimeException());
                    }
                }).subscribe(data -> log.info("data => {}", data));
    }
}
