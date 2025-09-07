package com.neoflex.studymate.services.bot;

import com.neoflex.studymate.dto.ExternalLink;
import com.neoflex.studymate.entities.Chunk;
import com.neoflex.studymate.services.chunk.ChunkService;
import com.neoflex.studymate.services.search.ExternalSearchService;
import com.neoflex.studymate.services.search.VectorDbService;
import com.neoflex.studymate.services.yandex.YandexGptService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class BotService {

    private final VectorDbService vectorDbService;
    private final ExternalSearchService externalSearchService;
    private final YandexGptService yandexGptService;
    private final TelegramService telegramService;
    private final ChunkService chunkService;

    private static final int MAX_MESSAGE_LENGTH = 254;

    public void handleUserMessage(Long chatId, String query) {
        try {
            if (query.length() > MAX_MESSAGE_LENGTH) {
                log.warn("Message too long ({} chars) from chatId {}. Max allowed is {}",
                        query.length(), chatId, MAX_MESSAGE_LENGTH);
                telegramService.sendMessage(chatId,
                        "Извини, твоё сообщение слишком длинное\uD83D\uDE33\n" +
                                "Пожалуйста, отправь сообщение короче 255 символов");
                return;
            }

            boolean isStudyQuestion = yandexGptService.checkIsStudyQuestion(query);
            if (!isStudyQuestion) {
                String shortAnswer = yandexGptService.answerShort(query);
                telegramService.sendMessage(chatId, shortAnswer +
                        "\n\nМожет, у тебя есть вопрос по учебным материалам?\uD83D\uDE09");
                return;
            }

            List<String> tags = yandexGptService.extractTags(query);
            List<Chunk> candidateChunks = chunkService.findByTags(tags);
            List<Chunk> bestChunks = vectorDbService.findTopRelevantChunks(query, candidateChunks);
            List<ExternalLink> externalLinks = externalSearchService.search(query);

            if (bestChunks.isEmpty() && externalLinks.isEmpty()) {
                telegramService.sendMessage(chatId,
                        "Извини, я не смог найти ответ ни в учебных материалах, ни в открытых источниках\uD83D\uDE14\n" +
                        "Попробуй переформулировать свой вопрос");
                return;
            }

            StringBuilder answer = new StringBuilder();

            boolean isTryFindLocation = yandexGptService.checkIsTryFindLocationQuestion(query);
            if (!isTryFindLocation && !bestChunks.isEmpty()) {
                answer.append(yandexGptService.answerCompleteAnswer(query, bestChunks));
            }

            if (!bestChunks.isEmpty()) {
                answer.append(formatChunksAnswer(bestChunks));
            } else {
                answer.append("К сожалению, я не нашел в учебных материалах ничего по твоему вопросу\uD83E\uDDD0\n");
                answer.append("\nОтвечу, опираясь на общедоступные сведения:");
                answer.append("\n").append(yandexGptService.answerShort(query)).append("\n");
            }

            if (!externalLinks.isEmpty()) {
                answer.append("\nЯ подобрал для тебя ссылки на внешние источники, если ты захочешь изучить эту тему более подробно\uD83D\uDE0A:\n")
                        .append(formatExternalLinks(externalLinks));
            }

            telegramService.sendMessage(chatId, answer.toString());

        } catch (Exception e) {
            log.error("Error while handling user message", e);
            telegramService.sendMessage(chatId,
                    "Извини, сейчас я не могу обработать твой запрос\uD83D\uDE35\u200D\uD83D\uDCAB\n" +
                            "Попробуй повторить через несколько минут");
        }
    }


    private String formatChunksAnswer(List<Chunk> chunks) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\nРасположение информации в учебных материалах:\n");

        LinkedHashMap<String, LinkedHashMap<String, List<String>>> grouped = chunks.stream()
                .filter(p -> p.getDownloadedFile() != null && p.getDownloadedFile().getCourse() != null)
                .collect(Collectors.groupingBy(
                        p -> p.getDownloadedFile().getCourse().getName(),
                        LinkedHashMap::new,
                        Collectors.groupingBy(
                                p -> p.getDownloadedFile().getName(),
                                LinkedHashMap::new,
                                Collectors.mapping(Chunk::getUrl, Collectors.toList())
                        )
                ));

        for (var courseEntry : grouped.entrySet()) {
            String course = courseEntry.getKey();
            for (var sectionEntry : courseEntry.getValue().entrySet()) {
                String file = sectionEntry.getKey();
                List<String> chunkUrls = sectionEntry.getValue();
                Collections.sort(chunkUrls);
                sb.append("- Курс: ").append(course)
                        .append(", Файл: ").append(file)
                        .append(", ").append(chunkUrls.stream()
                                .map(String::valueOf).collect(Collectors.joining(", \n")))
                        .append("\n");
            }
        }

        return sb.toString();
    }

    public String formatExternalLinks(List<ExternalLink> links) {
        return links.stream()
                .map(link -> "- [" + link.getSite() + "](" + link.getUrl() + ")")
                .collect(Collectors.joining("\n"));
    }
}