package com.neoflex.studymate.init;

import com.neoflex.studymate.services.chunk.EmbeddingService;
import com.neoflex.studymate.services.chunk.ChunkService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final EmbeddingService embeddingService;
    private final ChunkService chunkService;

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Запуск инициализации данных...");

        chunkService.addTagsToEmptyChunks();

        embeddingService.fillChunkEmbeddings();

        System.out.println("Инициализация данных завершена");
    }
}
