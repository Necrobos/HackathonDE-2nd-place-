package com.neoflex.studymate.services.search;

import com.neoflex.studymate.entities.Chunk;
import com.neoflex.studymate.services.yandex.YandexGptService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class VectorDbService {

    private final YandexGptService yandexGptService;

    public List<Chunk> findTopRelevantChunks(String query, List<Chunk> chunks) {
        if (chunks == null || chunks.isEmpty()) {
            log.warn("No chunks provided for relevance search");
            return List.of();
        }

        double[] queryEmbedding = toArray(yandexGptService.getEmbedding(query));
        double threshold = 0;

        List<Chunk> topChunks = chunks.stream()
                .map(p -> Map.entry(p, cosineSimilarity(queryEmbedding, toArray(yandexGptService.getEmbedding(p.getContent())))))
                .peek(e -> log.debug("Chunk ID: {}, score: {}", e.getKey().getId(), e.getValue()))
                .filter(e -> e.getValue() >= threshold)
                .sorted((e1, e2) -> Double.compare(e2.getValue(), e1.getValue()))
                .limit(3)
                .map(Map.Entry::getKey)
                .toList();

        if (topChunks.isEmpty()) {
            log.warn("No relevant chunks found above threshold {}", threshold);
        } else {
            log.info("Top relevant chunk IDs above threshold {}: {}", threshold, topChunks.stream().map(Chunk::getId).toList());
        }

        return topChunks;
    }

    private double cosineSimilarity(double[] vec1, double[] vec2) {
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < vec1.length; i++) {
            dot += vec1[i] * vec2[i];
            normA += vec1[i] * vec1[i];
            normB += vec2[i] * vec2[i];
        }
        double similarity = dot / (Math.sqrt(normA) * Math.sqrt(normB));
        log.debug("Cosine similarity calculated: {}", similarity);
        return similarity;
    }

    private double[] toArray(List<Double> list) {
        return list.stream().mapToDouble(Double::doubleValue).toArray();
    }
}