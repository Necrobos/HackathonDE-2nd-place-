package com.neoflex.studymate.services.yandex;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.neoflex.studymate.entities.Chunk;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class YandexGptService {

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${yandex.api.api-key}")
    private String apiKey;

    @Value("${yandex.api.folder-id}")
    private String folderId;

    private static final String COMPLETION_URL = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion";
    private static final String EMBEDDING_URL = "https://llm.api.cloud.yandex.net/foundationModels/v1/textEmbedding";

    public List<String> extractTags(String text) {
        log.info("Extracting tags for text: '{}'", text);
        String prompt = "Выдели ключевые слова из текста и приведи их к единственному числу и нижнему регистру: \"" + text + "\"";
        String response = askModel(prompt);
        log.debug("Raw tags response: {}", response);

        List<String> tags = Arrays.stream(response.replaceAll("[^\\p{IsAlphabetic}\\s]", "")
                        .toLowerCase()
                        .split("\\s+"))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());

        log.info("Extracted tags: {}", tags);
        return tags;
    }

    public List<Double> getEmbedding(String text) {
        log.info("Requesting embedding for text: '{}'", text);

        Map<String, Object> requestBody = Map.of(
                "modelUri", "emb://" + folderId + "/text-search-query/latest",
                "text", text
        );

        Map<String, Object> response = postRequest(EMBEDDING_URL, requestBody, Map.class);

        if (response == null || !response.containsKey("embedding")) {
            log.error("No embedding returned from Yandex API for text: '{}'", text);
            throw new RuntimeException("No embedding returned from Yandex API");
        }

        List<Double> embedding = (List<Double>) response.get("embedding");
        log.debug("Embedding size: {}", embedding.size());
        return embedding;
    }

    public boolean checkIsStudyQuestion(String query) {
        log.info("Checking if query is study-related: '{}'", query);

        String prompt = """
        Определи, является ли этот вопрос учебным. 
        Учебный вопрос — это вопрос по школьным или университетским предметам (математика, физика, история, литература, информатика и т.п.), 
        по конкретным учебным курсам или учебным материалам. 
        НЕ учебные вопросы: общие разговоры ("как дела?", "что нового?"), приветствия, бытовые вопросы, личные темы. 
        
        Ответь только '1' или '0', где 1 - да, 0 - нет.
        Вопрос: %s
        """.formatted(query);

        String answer = askModel(prompt);
        log.debug("Study check raw response: {}", answer);

        boolean isStudy = answer.contains("1");
        log.info("Is study question: {}", isStudy);
        return isStudy;
    }

    public boolean checkIsTryFindLocationQuestion(String query) {
        log.info("Checking if query is for finding location of educational materials: '{}'", query);

        String prompt = """
        Определи, пытается ли пользователь найти, где и в каком курсе находится информация, которую он хочет узнать. 
       
        Ответь только '1' или '0', где 1 - да, 0 - нет.
        Вопрос: %s
        """.formatted(query);

        String answer = askModel(prompt);
        log.debug("Study check raw response: {}", answer);

        boolean isFindLocation = answer.contains("1");
        log.info("Is finding location question: {}", isFindLocation);
        return isFindLocation;
    }

    public String answerCompleteAnswer(String question, List<Chunk> chunks){
        String context = chunks.stream()
                .map(Chunk::getContent)
                .collect(Collectors.joining("\n\n"));

        String prompt = """
                        Ответь на вопрос пользователя, опираясь исключительно на приведённые материалы. 
                        Никакой дополнительной информации от себя не добавляй. Обращайся к пользователю только на ты
                        Вопрос: %s
                        Материалы:
                        %s
                        """.formatted(question, context);

        return askModel(prompt);
    }

    public String answerShort(String question){
        return askModel(
                "Ответь кратко и вежливо на этот вопрос, без лишних деталей, обращайся к пользователю только на ты, не здоровайся: \"" + question + "\""
        );
    }

    private String askModel(String prompt) {
        log.info("Sending request to Yandex GPT. Prompt: '{}'", prompt +" Не используй никакие символы кроме букв, цифр и знаков препинания");

        Map<String, Object> requestBody = Map.of(
                "modelUri", "gpt://" + folderId + "/yandexgpt-lite",
                "completionOptions", Map.of(
                        "stream", false,
                        "temperature", 0.6,
                        "maxTokens", 500
                ),
                "messages", List.of(
                        Map.of("role", "system", "text", "Ты помощник по учебным материалам"),
                        Map.of("role", "user", "text", prompt)
                )
        );

        GptResponse response = postRequest(COMPLETION_URL, requestBody, GptResponse.class);

        if (response != null && response.getResult() != null && !response.getResult().getAlternatives().isEmpty()) {
            String answer = response.getResult().getAlternatives().get(0).getMessage().getText();
            log.info("Received response from Yandex GPT: {}", answer);
            return answer;
        }

        log.warn("Received empty response from Yandex GPT");
        return "Извини, я не нашел ответа на твой вопрос. Попробуй переформулировать его";
    }

    private <T> T postRequest(String url, Object body, Class<T> responseType) {
        log.debug("Sending POST request to Yandex API: {}", url);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(apiKey);

        HttpEntity<Object> entity = new HttpEntity<>(body, headers);

        try {
            ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.POST, entity, responseType);
            log.debug("Received response status: {}", response.getStatusCode());
            return response.getBody();
        } catch (HttpClientErrorException | HttpServerErrorException e) {
            log.error("HTTP error from Yandex API: Status {}, Response: {}", e.getStatusCode(), e.getResponseBodyAsString());
            throw new RuntimeException("Yandex API request failed: " + e.getMessage(), e);
        } catch (RestClientException e) {
            log.error("RestClient error: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to connect to Yandex API", e);
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class GptResponse {
        private Result result;

        @Data
        public static class Result {
            private List<Alternative> alternatives;
        }

        @Data
        public static class Alternative {
            private Message message;
        }

        @Data
        public static class Message {
            private String role;
            private String text;
        }
    }
}
