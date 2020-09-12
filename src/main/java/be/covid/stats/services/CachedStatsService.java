package be.covid.stats.services;

import be.covid.stats.data.CasesPerDayDTO;
import be.covid.stats.utils.DateConversionUtils;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.integration.CacheLoader;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import reactor.core.publisher.Flux;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static be.covid.stats.utils.DateConversionUtils.*;

@Service
public class CachedStatsService implements StatsService {
    private static final String SCIENSANO_URL = "https://epistat.sciensano.be/Data/%s";
    private static final String AGE_SEX_URL = SCIENSANO_URL + "/COVID19BE_CASES_AGESEX_%s.json";
    private static final String DATE_MUNI = SCIENSANO_URL + "/COVID19BE_CASES_MUNI_%s.json";
    private static final String AGE_SEX_KEY = "AGE_SEX";
    private final RestTemplate restTemplate = new RestTemplate();
    private final Cache<String, String> cachedResponses = Cache2kBuilder.of(String.class, String.class)
            .permitNullValues(false)
            .expireAfterWrite(1, TimeUnit.DAYS)
            .loader(new CacheLoader<>() {
                @Override
                public String load(String key) throws Exception {
                    if (key.equals(AGE_SEX_KEY)) {
                        String date = DateConversionUtils.convert(LocalDate.now(), DEFAULT_DATE_FORMAT);
                        ResponseEntity<String> ageSex
                                = restTemplate.getForEntity(String.format(AGE_SEX_URL, date, date), String.class);
                        return ageSex.getBody();
                    }
                    return "";
                }
            })
            .build();
    private final Cache<String, Integer> totalPerDayCache = Cache2kBuilder.of(String.class, Integer.class)
            .expireAfterWrite(1, TimeUnit.DAYS)
            .permitNullValues(false)
            .loader(totalPerDyCacheLoader())
            .build();

    @Override
    public Flux<CasesPerDayDTO> getCasesPerDay(int maxDays) {
        return Flux.fromStream(IntStream.range(1, ++maxDays).boxed())
                .map(i -> {
                    String date = convert(LocalDate.now().minusDays(i), JSON_DATE_FORMAT);
                    return CasesPerDayDTO.builder()
                            .date(date)
                            .total(totalPerDayCache.get(date)).build();
                });
    }

    private Integer totalFor(String json, String date) {
        JSONArray jsonArray = JsonPath.read(json, "$.[?(@.DATE=='" + date + "')]");
        return jsonArray.parallelStream()
                .map(e -> {
                    if (e instanceof LinkedHashMap) {
                        return (int) ((LinkedHashMap) e).get("CASES");
                    }
                    return 0;
                })
                .mapToInt(Integer::intValue)
                .sum();
    }

    private CacheLoader<String, Integer> totalPerDyCacheLoader() {
        return new CacheLoader<>() {
            @Override
            public Integer load(String key) throws Exception {
                return totalFor(cachedResponses.get(AGE_SEX_KEY), key);
            }
        };
    }
}
