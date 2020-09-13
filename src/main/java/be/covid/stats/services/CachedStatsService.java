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

import javax.annotation.PostConstruct;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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

    private List<String> cities = List.of();
    private List<String> provinces = List.of();

    @PostConstruct
    public void preloadCache() {
        cachedResponses.get(AGE_SEX_KEY);
        provinces = collectProvinces();
    }

    @Override
    public Flux<CasesPerDayDTO> getCasesPerDay(int maxDays) {
        return Flux.fromStream(IntStream.range(1, ++maxDays).boxed().sorted(Collections.reverseOrder()))
                .map(i -> {
                    String date = convert(LocalDate.now().minusDays(i), JSON_DATE_FORMAT);
                    return CasesPerDayDTO.builder()
                            .date(date)
                            .total(totalPerDayCache.get(date)).build();
                });
    }

    @Override
    public Flux<String> getProvinces(String q) {
        return Flux.fromIterable(provinces)
                .filter(s -> q == null || q.equals("*") || q.isBlank() || s.contains(q));

    }

    private Integer totalFor(String json, String date) {
        JSONArray jsonArray = JsonPath.read(json, "$.[?(@.DATE=='" + date + "')]");
        return jsonArray.parallelStream()
                .map(e -> this.<Integer>getKey(e, "CASES"))
                .map(i -> i == null ? 0 : i)
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

    private List<String> collectProvinces() {
        String json = cachedResponses.get(AGE_SEX_KEY);
        JSONArray jsonArray = JsonPath.read(json, "$.[*]");
        return jsonArray.parallelStream()
                .map(e -> this.<String>getKey(e, "PROVINCE"))
                .filter(Objects::nonNull)
                .distinct()
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private <T> T getKey(Object e, String name) {
        if (e instanceof LinkedHashMap) {
            return (T) ((LinkedHashMap) e).get(name);
        }
        return null;
    }
}
