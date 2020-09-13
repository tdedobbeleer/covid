package be.covid.stats.services;

import be.covid.stats.data.CasesPerDayDTO;
import reactor.core.publisher.Flux;

public interface StatsService {
    Flux<CasesPerDayDTO> getCasesPerDay(int maxDays);

    Flux<String> getProvinces(String q);
}
