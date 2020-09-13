package be.covid.stats.controllers;

import be.covid.stats.services.CachedStatsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/data")
public class DataController {

    private final CachedStatsService cachedStatsService;

    public DataController(CachedStatsService cachedStatsService) {
        this.cachedStatsService = cachedStatsService;
    }

    @GetMapping("/provinces")
    private Mono<List<String>> getProvince(@RequestParam(required = false) String q) {
        return Mono.from(cachedStatsService.getProvinces(q).collectList());
    }
}
