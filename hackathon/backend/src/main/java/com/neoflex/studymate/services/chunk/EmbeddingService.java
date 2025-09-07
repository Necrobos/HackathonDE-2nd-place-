package com.neoflex.studymate.services.chunk;

import com.neoflex.studymate.services.yandex.YandexGptService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmbeddingService {

    private final JdbcTemplate jdbcTemplate;
    private final YandexGptService yandexGptService;

    public void fillChunkEmbeddings() {
        log.info("Starting embedding fill for chunks...");

        List<Map<String, Object>> chunks = getChunksWithoutEmbeddings();
        log.info("Found {} chunks without embeddings", chunks.size());

        chunks.forEach(this::processChunk);

        log.info("Embedding fill completed.");
    }

    private List<Map<String, Object>> getChunksWithoutEmbeddings() {
        return jdbcTemplate.queryForList(
                "SELECT p.id, p.content " +
                        "FROM dm.chunks p " +
                        "LEFT JOIN dm.chunks_vectors e ON p.id = e.chunk_id " +
                        "WHERE e.chunk_id IS NULL"
        );
    }

    private void processChunk(Map<String, Object> chunk) {
        Long chunkId = ((Number) chunk.get("id")).longValue();
        String text = (String) chunk.get("content");

        try {
            List<Double> vectorList = yandexGptService.getEmbedding(text);
            double[] vector = toArray(vectorList);

            saveEmbedding(chunkId, vector);
            log.info("Filled embedding for chunk_id={}", chunkId);
        } catch (Exception e) {
            log.error("Failed to fill embedding for chunk_id={}", chunkId, e);
        }
    }

    private void saveEmbedding(Long chunkId, double[] vector) {
        jdbcTemplate.update(
                "INSERT INTO dm.chunks_vectors(chunk_id, embedding) VALUES (?, ?::vector)",
                chunkId, vectorToString(vector)
        );
    }

    private String vectorToString(double[] vector) {
        return "[" + java.util.Arrays.stream(vector)
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(",")) + "]";
    }

    private double[] toArray(List<Double> list) {
        return list.stream().mapToDouble(Double::doubleValue).toArray();
    }
}