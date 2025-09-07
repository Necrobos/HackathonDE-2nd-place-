package com.neoflex.studymate.controllers;

import com.neoflex.studymate.services.bot.BotService;
import com.neoflex.studymate.services.bot.TelegramService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/telegram")
@Tag(name = "Bot Controller", description = "Контроллер для обработки сообщений от Telegram бота")
public class BotController {

    private final BotService botService;
    private final TelegramService telegramService;

    @Operation(
            summary = "Обработка вебхука от Telegram",
            description = "Принимает и обрабатывает входящие сообщения от Telegram бота",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Сообщение успешно обработано"),
            }
    )
    @PostMapping("/webhook")
    public ResponseEntity<Void> onUpdate(@RequestBody Map<String, Object> update) {
        try {
            Map<String, Object> message = (Map<String, Object>) update.get("message");
            if (message != null) {
                Map<String, Object> chat = (Map<String, Object>) message.get("chat");
                Long chatId = ((Number) chat.get("id")).longValue();
                String text = (String) message.get("text");

                if (text != null && !text.isBlank()) {
                    log.info("Received message from chat {}: {}", chatId, text);
                    botService.handleUserMessage(chatId, text);
                } else {
                    log.warn("Received non-text message from chat {}: {}", chatId, message);
                    telegramService.sendMessage(chatId, "Извини, но я понимаю только текст");
                }
            } else {
                log.warn("Received update without message: {}", update);
            }
        } catch (Exception e) {
            log.error("Error processing Telegram update: {}", update, e);
        }
        return ResponseEntity.ok().build();
    }
}
