package com.neoflex.studymate.services.chunk;

import com.neoflex.studymate.entities.Chunk;
import com.neoflex.studymate.entities.Tag;
import com.neoflex.studymate.repositiries.TagRepository;
import com.neoflex.studymate.repositiries.ChunkRepository;
import com.neoflex.studymate.services.yandex.YandexGptService;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ChunkService {

    private final ChunkRepository chunkRepository;
    private final EmbeddingService embeddingService;
    private final TagRepository tagRepository;
    private final YandexGptService yandexGptService;

    public Chunk saveChunk(Chunk chunk) {
        Chunk saved = chunkRepository.save(chunk);
        embeddingService.fillChunkEmbeddings();
        addTagsToEmptyChunks();
        return saved;
    }

    @Transactional
    public void deleteChunk(Long chunkId) {
        Chunk chunk = chunkRepository.findById(chunkId)
                .orElseThrow(() -> new IllegalArgumentException("Chunk not found: " + chunkId));

        if (chunk.getTags() != null) {
            chunk.getTags().clear();
        }

        chunkRepository.delete(chunk);
    }

    public List<Chunk> findByTags(List<String> tags) {
        return chunkRepository.findByTagsIgnoreCase(tags);
    }

    @Transactional
    public void addTagsToEmptyChunks() {
        List<Chunk> chunks = chunkRepository.findAll();
        for (Chunk chunk : chunks) {
            List<String> extractedTags = yandexGptService.extractTags(chunk.getContent());
            if (extractedTags != null && !extractedTags.isEmpty()) {
                Set<Tag> tags = extractedTags.stream()
                        .map(title -> tagRepository.findByName(title)
                                .orElseGet(() -> {
                                    Tag tag = new Tag();
                                    tag.setName(title);
                                    return tagRepository.save(tag);
                                }))
                        .collect(Collectors.toSet());

                if (chunk.getTags() == null) {
                    chunk.setTags(new HashSet<>());
                }

                chunk.getTags().addAll(tags);
                chunkRepository.save(chunk);
            }
        }
    }

    public List<Chunk> findAllChunks() {
        return chunkRepository.findAll();
    }
}
