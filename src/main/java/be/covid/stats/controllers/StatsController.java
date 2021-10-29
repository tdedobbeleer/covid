package be.covid.stats.controllers;

import be.covid.stats.data.CasesPerDayDTO;
import be.covid.stats.services.CachedStatsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/stats")
public class StatsController {

    private final CachedStatsService cachedStatsService;

    public StatsController(CachedStatsService cachedStatsService) {
        this.cachedStatsService = cachedStatsService;
    }

    @GetMapping("/day")
    private Flux<CasesPerDayDTO> getAllCases() {
        return cachedStatsService.getCasesPerDay(14);
    }

    @GetMapping("/day/municipalities/{m}")
    private Flux<CasesPerDayDTO> getAllCasesPerDayForMunicipality(@PathVariable String m) {
        return cachedStatsService.getCasesPerDayForMunicipality(14, m);
    }

    @GetMapping("/day/provinces/{m}")
    private Flux<CasesPerDayDTO> getAllCasesPerDayForProvince(@PathVariable String m) {
        return cachedStatsService.getCasesPerDayForProvince(14, m);
    }
}
