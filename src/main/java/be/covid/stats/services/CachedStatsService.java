package be.covid.stats.services;

import be.covid.stats.data.CasesPerDayDTO;
import be.covid.stats.utils.DateConversionUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.apache.logging.log4j.util.Strings;
import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;
import org.cache2k.event.CacheEntryExpiredListener;
import org.cache2k.integration.CacheLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
    private final Cache<String, Path> cachedResponses = Cache2kBuilder.of(String.class, Path.class)
            .permitNullValues(false)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .addListener((CacheEntryExpiredListener<String, Path>) (cache, entry) -> {
                boolean deleted = entry.getValue().toFile().delete();
                if (deleted) {
                    log.info("Tmp file " + entry.getValue().toString() + " successfully deleted");
                } else {
                    log.error("Could not delete tmp file " + entry.getValue().toString());
                }
            })
            .loader(new CacheLoader<>() {
                @Override
                public Path load(String key) throws Exception {
                    return getResponses(key);
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

    private List<String> provinces = new ArrayList<>();
    private List<String> municipalities = new ArrayList<>();

    @Override
    public void preloadCache() throws IOException {
        log.info("Preloading cache");
        log.info("Preloading " + AGE_SEX_KEY + " file");
        cachedResponses.get(AGE_SEX_KEY);
        log.info("Preloading " + DATE_MUNI_KEY + " file");
        cachedResponses.get(DATE_MUNI_KEY);
        log.info("Collect provinces");
        provinces = collectProvinces();
        log.info("Collect municipalities");
        municipalities = collectMunicipalities();
        log.info("Done preloading");

    }

    private Path getResponses(String key) {
        String date = DateConversionUtils.convert(LocalDate.now(), DEFAULT_DATE_FORMAT);
        if (key.equals(AGE_SEX_KEY)) {
            try {
                Path path = Files.createTempFile(AGE_SEX_KEY, ".json");
                getStream(String.format(AGE_SEX_URL, date, date), path);
                return path;
            } catch (IOException e) {
                log.error("Could not get response.");
            }
        } else if (key.equals(DATE_MUNI_KEY)) {
            try {
                Path path = Files.createTempFile(DATE_MUNI_KEY, ".json");
                getStream(String.format(DATE_MUNI, date, date), path);
                return path;
            } catch (IOException e) {
                log.error("Could not get response.");
            }
        }
        return null;
    }

    private void getStream(String url, Path path) {
        // Optional Accept header
        RequestCallback requestCallback = request -> request
                .getHeaders()
                .setAccept(Arrays.asList(MediaType.APPLICATION_OCTET_STREAM, MediaType.ALL));

        // Streams the response instead of loading it all in memory
        ResponseExtractor<Void> responseExtractor = response -> {
            // Here you can write the inputstream to a file or any other place
            Files.copy(response.getBody(), path, StandardCopyOption.REPLACE_EXISTING);
            return null;
        };
        restTemplate.execute(url, HttpMethod.GET, requestCallback, responseExtractor);
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

    private Integer totalFor(File json, String date) throws IOException {
        try {
            List<String> result = new ArrayList<>();
            JsonFactory jsonfactory = new JsonFactory();
            ObjectMapper mapper = new ObjectMapper();
            JsonParser parser = jsonfactory.createParser(json);

            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException("Expected an array");
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {

                ObjectNode node = mapper.readTree(parser);
                String d = getNodeStringValue(node, "DATE");

                if (d.equalsIgnoreCase(date)) {
                    result.add(getNodeStringValue(node, "CASES"));
                }
            }

            parser.close();
            return result.parallelStream().filter(Strings::isNotEmpty).map(i -> {
                        try {
                            return Integer.parseInt(i);
                        } catch (NumberFormatException e) {
                            return 0;
                        }
                    }
            ).mapToInt(Integer::intValue).sum();

        } catch (IOException jge) {
            jge.printStackTrace();
        }
        return 0;
    }

    private Integer totalForMunicipality(File json, String municipality, String date) throws IOException {
        try {
            List<String> result = new ArrayList<>();
            JsonFactory jsonfactory = new JsonFactory();
            ObjectMapper mapper = new ObjectMapper();
            JsonParser parser = jsonfactory.createParser(json);

            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException("Expected an array");
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {

                ObjectNode node = mapper.readTree(parser);
                String m = getNodeStringValue(node, "TX_DESCR_NL");
                String d = getNodeStringValue(node, "DATE");

                if (m.equalsIgnoreCase(municipality)
                        && d.equalsIgnoreCase(date)) {
                    result.add(getNodeStringValue(node, "CASES"));
                }
            }

            parser.close();
            return result.parallelStream().filter(Strings::isNotEmpty).map(i -> {
                        try {
                            return Integer.parseInt(i);
                        } catch (NumberFormatException e) {
                            return 0;
                        }
                    }
            ).mapToInt(Integer::intValue).sum();

        } catch (IOException jge) {
            jge.printStackTrace();
        }
        return 0;
    }

    private Integer totalForProvince(File json, String province, String date) throws IOException {
        try {
            List<String> result = new ArrayList<>();
            JsonFactory jsonfactory = new JsonFactory();
            ObjectMapper mapper = new ObjectMapper();
            JsonParser parser = jsonfactory.createParser(json);

            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException("Expected an array");
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {

                ObjectNode node = mapper.readTree(parser);
                String p = getNodeStringValue(node, "PROVINCE");
                String d = getNodeStringValue(node, "DATE");

                if (p.equalsIgnoreCase(province)
                        && d.equalsIgnoreCase(date)) {
                    result.add(getNodeStringValue(node, "CASES"));
                }
            }

            parser.close();
            return result.parallelStream().filter(Strings::isNotEmpty).map(i -> {
                        try {
                            return Integer.parseInt(i);
                        } catch (NumberFormatException e) {
                            return 0;
                        }
                    }
            ).mapToInt(Integer::intValue).sum();

        } catch (IOException jge) {
            jge.printStackTrace();
        }
        return 0;
    }

    private CacheLoader<ComplexKey, Integer> totalPerDayForMunicipalityCacheLoader() {
        return new CacheLoader<>() {
            @Override
            public Integer load(ComplexKey pair) throws Exception {
                return totalForMunicipality(cachedResponses.get(DATE_MUNI_KEY).toFile(), pair.getKey(), pair.getValue());
            }
        };
    }

    private CacheLoader<ComplexKey, Integer> totalPerDayForProvinceCacheLoader() {
        return new CacheLoader<>() {
            @Override
            public Integer load(ComplexKey pair) throws Exception {
                return totalForProvince(cachedResponses.get(AGE_SEX_KEY).toFile(), pair.getKey(), pair.getValue());
            }
        };
    }

    private CacheLoader<String, Integer> totalPerDyCacheLoader() {
        return new CacheLoader<>() {
            @Override
            public Integer load(String key) throws Exception {
                return totalFor(cachedResponses.get(AGE_SEX_KEY).toFile(), key);
            }
        };
    }

    private List<String> collectProvinces() throws IOException {
        JSONArray jsonArray = JsonPath.read(cachedResponses.get(AGE_SEX_KEY).toFile(), "$.[*].PROVINCE");
        return jsonArray.parallelStream()
                .map(Object::toString)
                .filter(Objects::nonNull)
                .distinct()
                .sorted(Comparator.naturalOrder())
                .collect((Collectors.toList()));
    }

    private List<String> collectMunicipalities() throws IOException {
        try {
            TreeSet<String> result = new TreeSet<>();
            JsonFactory jsonfactory = new JsonFactory();
            ObjectMapper mapper = new ObjectMapper();
            JsonParser parser = jsonfactory.createParser(cachedResponses.get(DATE_MUNI_KEY).toFile());

            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException("Expected an array");
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                ObjectNode node = mapper.readTree(parser);
                String entry = getNodeStringValue(node, "TX_DESCR_NL");
                if (Strings.isNotEmpty(entry)) result.add(entry);
            }

            parser.close();
            return result.parallelStream().collect(Collectors.toList());

        } catch (IOException jge) {
            jge.printStackTrace();
        }
        return List.of();
    }

    private String getNodeStringValue(ObjectNode node, String fieldName) {
        JsonNode entry = node.get(fieldName);
        if (entry != null) {
            return entry.asText();
        }
        return "";
    }

    @SuppressWarnings("unchecked")
    private <T> T getKey(Object e, String name) {
        if (e instanceof LinkedHashMap) {
            return (T) ((LinkedHashMap) e).get(name);
        }
        return null;
    }

}
