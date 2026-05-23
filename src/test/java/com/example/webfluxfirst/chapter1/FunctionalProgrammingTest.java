package com.example.webfluxfirst.chapter1;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

@SpringBootTest
@Slf4j
public class FunctionalProgrammingTest {

    @Test
    public void produceOneToNine() {
        ArrayList<Integer> sink = new ArrayList<>();
        for (int i = 1; i <= 9; i++) {
            sink.add(i);
        }

        // 요구사항 : 모든 요소에 * n을 하고 m의 배수들만 남겨두고싶다.
        sink = map(sink, (data) -> data * 4);       // n == 2
        sink = filter(sink, (data) -> data % 4 == 0);    // valid % 4 == 0

        forEachPrint(sink, (data) ->  log.info("{}", data));
    }

    @Test
    public void produceOneToNineStream() {
        IntStream.rangeClosed(1, 9).boxed()
                .map(i -> i * 2)
                .filter(i -> i % 4 == 0)
                .forEach(i -> log.info("{}", i));
    }


    @Test
    public void produceOneToNineFlux() {
        Flux<Integer> intFlux = Flux.create(sink -> {
            for (int i = 0; i <= 9; i++) {
                sink.next(i);
            }
            sink.complete();
        });

        intFlux.subscribe(data -> log.info("WebFlux Subscribe => {}", data));

        log.info("thead return to Netty Event loop.");
    }

    @Test
    public void produceOneToNineFluxOperator() {

        Flux.fromIterable(IntStream.rangeClosed(1, 9).boxed().toList())
                .map(i -> i * 2)
                .filter(i -> i % 4 == 0)
                .subscribe(i -> log.info("{}", i));
    }



    private void forEachPrint(ArrayList<Integer> sink,
                              Consumer<Integer> consumer) {
        for (Integer integer : sink) {
            consumer.accept(integer);
        }
    }

    private ArrayList<Integer> filter(ArrayList<Integer> sink,
                                      Function<Integer, Boolean> predicate) {
        ArrayList<Integer> newSink2 = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            if (predicate.apply(sink.get(i)))
                newSink2.add(sink.get(i));
        }

        return newSink2;
    }

    private ArrayList<Integer> map(ArrayList<Integer> sink,
                                   Function<Integer, Integer> mapper) {
        ArrayList<Integer> newSink1 = new ArrayList<>();

        for (int i = 0; i < 9; i++) {
            newSink1.add(mapper.apply(sink.get(i)));
        }

        return newSink1;
    }
}
