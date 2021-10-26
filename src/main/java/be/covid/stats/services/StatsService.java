package be.covid.stats.services;

import be.covid.stats.data.CasesPerDayDTO;
import reactor.core.publisher.Flux;

import java.io.IOException;

public interface StatsService {

    void preloadCache() throws IOException;

    Flux<CasesPerDayDTO> getCasesPerDay(int maxDays);

    Flux<CasesPerDayDTO> getCasesPerDayForMunicipality(int maxDays, String municipality);

    Flux<CasesPerDayDTO> getCasesPerDayForProvince(int maxDays, String province);

    Flux<String> getProvinces(String q);

    Flux<String> getMunicipalities(String q);
}
