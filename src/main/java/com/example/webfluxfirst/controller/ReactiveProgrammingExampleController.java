package com.example.webfluxfirst.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/reactive")
@Slf4j
public class ReactiveProgrammingExampleController {

    // 1~9까지 출력하는 api
    @GetMapping("/onenine/list")
    public List<Integer> produceOneToNine() {
        ArrayList<Integer> sink = new ArrayList<>();

        for (int i = 0; i <= 9; i++) {
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            sink.add(i);
        }

        return sink;
    }

    // 1~9까지 출력하는 api with flux
    @GetMapping("/onenine/flux")
    public Flux<Integer> produceOneToNineFlux() {
        return Flux.<Integer>create(sink -> {
            for (int i = 0; i <= 9; i++) {
                try {
                    log.info("현재 처리하고 있는 스레드 이름 => {}", Thread.currentThread().getName());
                    Thread.sleep(500);
                } catch (Exception ignore) {}

                sink.next(i);
            }
            sink.complete();
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
