/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.agentic.ai.workflow.graph.utils;

import io.github.agentic.ai.api.chat.ChatMessage;
import io.github.agentic.ai.api.chat.ChatRole;
import io.github.agentic.ai.api.chat.ContentPart;
import io.github.agentic.ai.api.chat.ToolCall;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageUtilsNeutralTest {

	@Test
	void preservesToolResultsAndMetadataFromMapAndJson() throws Exception {
		var source = Map.of("role", "tool", "text", "result", "toolCallId", "id", "toolName", "search", "metadata",
				Map.of("source", "checkpoint"));
		var expected = ChatMessage.tool("id", "search", "result").withMetadata(Map.of("source", "checkpoint"));
		assertEquals(expected, MessageUtils.convertMapToMessage(source));
		assertEquals(expected, MessageUtils.convertJsonNodeToMessage(new ObjectMapper().valueToTree(source)));
		assertEquals(List.of(expected), MessageUtils.convertToMessages(List.of(source)));
	}

	@Test
	void preservesStructuredPartsAndAssistantCalls() {
		var part = ContentPart.json("{\"ok\":true}");
		var tool = new ToolCall("id", "search", "{}");
		assertEquals(ChatMessage.builder(ChatRole.ASSISTANT).content(List.of(part)).toolCalls(List.of(tool)).build(),
				MessageUtils.convertMapToMessage(
						Map.of("role", "assistant", "content", List.of(part), "toolCalls", List.of(tool))));
		assertEquals(ChatMessage.user("plain"), MessageUtils.convertMapToMessage(Map.of("text", "plain")));
		assertNull(MessageUtils.convertMapToMessage(Map.of()));
	}

	@Test
	void convertsValidPojoMessagesAndPreservesSerializationFallback() {
		assertEquals(List.of(ChatMessage.assistant("answer")),
				MessageUtils.convertToMessages(new MessagePayload("assistant", "answer")));
		assertEquals(List.of(), MessageUtils.convertToMessages(new MessagePayload("user", null)));
		for (String text : List.of("fallback", "")) {
			Object payload = new Object() {
				@Override
				public String toString() {
					return text;
				}
			};
			assertEquals(text.isEmpty() ? List.of() : List.of(ChatMessage.user(text)),
					MessageUtils.convertToMessages(payload));
		}
	}

	@Test
	void rejectsLegacyMapsAndInvalidNeutralMessages() throws Exception {
		for (String marker : List.of("@class", "@type", "@typeHint")) {
			assertThrows(IllegalArgumentException.class,
					() -> MessageUtils.convertToMessages(Map.of(marker, "ASSISTANT", "text", "old")));
		}
		assertThrows(IllegalArgumentException.class,
				() -> MessageUtils.convertMapToMessage(Map.of("role", "other", "text", "x")));
		assertThrows(IllegalArgumentException.class,
				() -> MessageUtils.convertMapToMessage(Map.of("content", Map.of("text", "x"))));
		assertThrows(IllegalArgumentException.class,
				() -> MessageUtils.convertMapToMessage(Map.of("text", "x", "metadata", List.of())));
		assertThrows(IllegalArgumentException.class,
				() -> MessageUtils.convertMapToMessage(Map.of("role", "tool", "text", "x")));
		var legacy = new ObjectMapper().readTree("{\"@type\":\"ASSISTANT\",\"text\":\"old\"}");
		assertThrows(IllegalArgumentException.class, () -> MessageUtils.convertToMessages(legacy));
		assertThrows(IllegalArgumentException.class,
				() -> MessageUtils.convertToMessages(new MessagePayload("other", "bad")));
	}

	record MessagePayload(String role, String text) {
	}

}
