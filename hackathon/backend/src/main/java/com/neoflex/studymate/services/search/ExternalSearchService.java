package com.neoflex.studymate.services.search;

import com.neoflex.studymate.dto.ExternalLink;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class ExternalSearchService {

    public List<ExternalLink> search(String query) {
        List<ExternalLink> results = new ArrayList<>();
        try {
            results.add(searchHabr(query));
            results.add(searchCyberLeninka(query));
            results.add(searchGoogleScholar(query));
        } catch (Exception e) {
            log.error("External search failed for query: '{}'", query, e);
        }
        log.info("External search completed with total {} results", results.size());
        return results;
    }

    private ExternalLink searchCyberLeninka(String query) {
        String url = "https://cyberleninka.ru/search?q=" +
                URLEncoder.encode(query, StandardCharsets.UTF_8);
        return new ExternalLink("CyberLeninka", url, url);
    }

    private ExternalLink searchHabr(String query) {
        String url = "https://habr.com/ru/search/?q=" +
                URLEncoder.encode(query, StandardCharsets.UTF_8);
        return new ExternalLink("Habr", url, url);
    }

    private ExternalLink searchGoogleScholar(String query) {
        String url = "https://scholar.google.com/scholar?q=" +
                URLEncoder.encode(query, StandardCharsets.UTF_8);
        return new ExternalLink("Google Scholar", url, url);
    }
}
