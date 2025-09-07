package com.neoflex.studymate.services.bot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class TelegramService {
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${app.bot.token}")
    private String token;

    public void sendMessage(Long chatId, String text) {
        text = escapeMarkdownV2(text);
        String url = "https://api.telegram.org/bot" + token + "/sendMessage";
        Map<String, Object> request = new HashMap<>();
        request.put("chat_id", chatId);
        request.put("text", text);
        request.put("parse_mode", "MarkdownV2");

        try {
            restTemplate.postForObject(url, request, String.class);
            log.debug("Message sent to chatId {}: {}", chatId, text);
        } catch (Exception e) {
            log.error("Failed to send message to chatId {}", chatId, e);
        }
    }

    private String escapeMarkdownV2(String text) {
        if (text == null) return null;
        return text.replaceAll("([_\\*\\[\\]\\(\\)~`>#+\\-=|{}.!])", "\\\\$1");
    }
}
