package be.covid.stats.services;

import be.covid.stats.data.CasesPerDayDTO;
import be.covid.stats.utils.DateConversionUtils;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.integration.CacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger log = LoggerFactory.getLogger(StatsService.class);
    private static final String SCIENSANO_URL = "https://epistat.sciensano.be/Data/%s";
    private static final String AGE_SEX_URL = SCIENSANO_URL + "/COVID19BE_CASES_AGESEX_%s.json";
    private static final String DATE_MUNI = SCIENSANO_URL + "/COVID19BE_CASES_MUNI_%s.json";
    private static final String AGE_SEX_KEY = "AGE_SEX";
    private static final String DATE_MUNI_KEY = "DATE_MUNI";
    private final RestTemplate restTemplate = new RestTemplate();
    private final Cache<String, String> cachedResponses = Cache2kBuilder.of(String.class, String.class)
            .permitNullValues(false)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .loader(new CacheLoader<>() {
                @Override
                public String load(String key) throws Exception {
                    String date = DateConversionUtils.convert(LocalDate.now(), DEFAULT_DATE_FORMAT);
                    if (key.equals(AGE_SEX_KEY)) {
                        ResponseEntity<String> ageSex
                                = restTemplate.getForEntity(String.format(AGE_SEX_URL, date, date), String.class);
                        return ageSex.getBody();
                    } else if (key.equals(DATE_MUNI_KEY)) {
                        ResponseEntity<String> dateMuni
                                = restTemplate.getForEntity(String.format(DATE_MUNI, date, date), String.class);
                        return dateMuni.getBody();
                    }
                    return "";
                }
            })
            .build();
    private final Cache<String, Integer> totalPerDayCache = Cache2kBuilder.of(String.class, Integer.class)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .permitNullValues(false)
            .loader(totalPerDyCacheLoader())
            .build();

    private final Cache<ComplexKey, Integer> totalPerDayPerMunicipalityCache = Cache2kBuilder.of(ComplexKey.class, Integer.class)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .permitNullValues(false)
            .loader(totalPerDayForMunicipalityCacheLoader())
            .build();

    private final Cache<ComplexKey, Integer> totalPerDayPerProvinceCache = Cache2kBuilder.of(ComplexKey.class, Integer.class)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .permitNullValues(false)
            .loader(totalPerDayForProvinceCacheLoader())
            .build();

    private List<String> municipalities = List.of();
    private List<String> provinces = List.of();

    @Override
    public void preloadCache() {
        log.info("Preloading cache");
        cachedResponses.get(AGE_SEX_KEY);
        cachedResponses.get(DATE_MUNI_KEY);
        provinces = collectProvinces();
        municipalities = collectMunicipalities();

    }

    @Override
    public Flux<CasesPerDayDTO> getCasesPerDay(int maxDays) {
        return Flux.fromStream(IntStream.range(2, maxDays + 2).boxed().sorted(Collections.reverseOrder()))
                .map(i -> {
                    String date = convert(LocalDate.now().minusDays(i), JSON_DATE_FORMAT);
                    return CasesPerDayDTO.builder()
                            .date(date)
                            .total(totalPerDayCache.get(date)).build();
                });
    }

    @Override
    public Flux<CasesPerDayDTO> getCasesPerDayForMunicipality(int maxDays, String municipality) {
        return Flux.fromStream(IntStream.range(2, ++maxDays + 2).boxed().sorted(Collections.reverseOrder()))
                .map(i -> {
                    String date = convert(LocalDate.now().minusDays(i), JSON_DATE_FORMAT);
                    return CasesPerDayDTO.builder()
                            .date(date)
                            .total(totalPerDayPerMunicipalityCache.get(ComplexKey.of(municipality, date))).build();
                });
    }

    @Override
    public Flux<CasesPerDayDTO> getCasesPerDayForProvince(int maxDays, String province) {
        return Flux.fromStream(IntStream.range(2, ++maxDays + 2).boxed().sorted(Collections.reverseOrder()))
                .map(i -> {
                    String date = convert(LocalDate.now().minusDays(i), JSON_DATE_FORMAT);
                    return CasesPerDayDTO.builder()
                            .date(date)
                            .total(totalPerDayPerProvinceCache.get(ComplexKey.of(province, date))).build();
                });
    }

    @Override
    public Flux<String> getProvinces(String q) {
        return Flux.fromIterable(provinces)
                .filter(s -> {
                    if (q == null) return true;
                    if (q.equals("*")) return true;
                    if (q.isBlank()) return true;
                    return s.toLowerCase().contains(q.toLowerCase());
                });

    }

    @Override
    public Flux<String> getMunicipalities(String q) {
        return Flux.fromIterable(municipalities)
                .filter(s -> {
                    if (q == null) return true;
                    if (q.equals("*")) return true;
                    if (q.isBlank()) return true;
                    return s.toLowerCase().contains(q.toLowerCase());
                });

    }

    private Integer totalFor(String json, String date) {
        JSONArray jsonArray = JsonPath.read(json, "$.[?(@.DATE=='" + date + "')]");
        return jsonArray.parallelStream()
                .map(e -> this.<Integer>getKey(e, "CASES"))
                .map(i -> i == null ? 0 : i)
                .mapToInt(Integer::intValue)
                .sum();
    }

    private Integer totalForMunicipality(String json, String municipality, String date) {
        JSONArray jsonArray = JsonPath.read(json, "$.[?(@.TX_DESCR_NL=='" + municipality + "' && @.DATE=='" + date + "')]");
        return jsonArray.parallelStream()
                .map(e -> this.<String>getKey(e, "CASES"))
                .map(s -> {
                    try {
                        return Integer.parseInt(s);
                    } catch (NumberFormatException e) {
                        return 0;
                    }
                })
                .mapToInt(Integer::intValue)
                .sum();
    }

    private Integer totalForProvince(String json, String province, String date) {
        JSONArray jsonArray = JsonPath.read(json, "$.[?(@.PROVINCE=='" + province + "' && @.DATE=='" + date + "')]");
        return jsonArray.parallelStream()
                .map(e -> this.<Integer>getKey(e, "CASES"))
                .map(i -> i == null ? 0 : i)
                .mapToInt(Integer::intValue)
                .sum();
    }

    private CacheLoader<ComplexKey, Integer> totalPerDayForMunicipalityCacheLoader() {
        return new CacheLoader<>() {
            @Override
            public Integer load(ComplexKey pair) throws Exception {
                return totalForMunicipality(cachedResponses.get(DATE_MUNI_KEY), pair.getKey(), pair.getValue());
            }
        };
    }

    private CacheLoader<ComplexKey, Integer> totalPerDayForProvinceCacheLoader() {
        return new CacheLoader<>() {
            @Override
            public Integer load(ComplexKey pair) throws Exception {
                return totalForProvince(cachedResponses.get(AGE_SEX_KEY), pair.getKey(), pair.getValue());
            }
        };
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

    private List<String> collectMunicipalities() {
        String json = cachedResponses.get(DATE_MUNI_KEY);
        JSONArray jsonArray = JsonPath.read(json, "$.[*]");
        return jsonArray.parallelStream()
                .map(e -> this.<String>getKey(e, "TX_DESCR_NL"))
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
