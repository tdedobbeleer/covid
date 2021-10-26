package be.covid.stats.config;

import be.covid.stats.services.CachedStatsService;
import lombok.AllArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.io.IOException;

@Configuration
@AllArgsConstructor
public class AppConfig {
    private final CachedStatsService cachedStatsService;

    //Fill cache after spring has loaded
    @EventListener(ApplicationReadyEvent.class)
    public void doSomethingAfterStartup() throws IOException {
        cachedStatsService.preloadCache();
    }
}
