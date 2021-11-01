package be.covid.stats.config;

import be.covid.stats.services.CachedStatsService;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

@Configuration
@AllArgsConstructor
public class AppConfig {
    private final CachedStatsService cachedStatsService;

    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    //Fill cache after spring has loaded
    @EventListener(ApplicationReadyEvent.class)
    public void doSomethingAfterStartup() {
        try {
            cachedStatsService.preloadCache();
        } catch (Exception e) {
            log.error("Could not preload data.", e);
        }
    }
}
