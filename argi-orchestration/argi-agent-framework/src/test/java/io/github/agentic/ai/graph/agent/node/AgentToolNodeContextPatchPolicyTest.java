/*
 * Copyright 2025-2026 the original author or authors.
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
package io.github.agentic.ai.graph.agent.node;

import io.github.agentic.ai.api.chat.ChatMessage;
import io.github.agentic.ai.api.chat.ToolCall;
import io.github.agentic.ai.api.tool.Tool;
import io.github.agentic.ai.api.tool.ToolContext;
import io.github.agentic.ai.api.tool.ToolRegistry;
import io.github.agentic.ai.api.tool.ToolResult;
import io.github.agentic.ai.graph.OverAllState;
import io.github.agentic.ai.graph.RunnableConfig;
import io.github.agentic.ai.graph.agent.interceptor.BusinessOutcomeMapperRegistry;
import io.github.agentic.ai.graph.agent.tool.patch.ToolContextPatchPolicy;
import io.github.agentic.ai.graph.agent.tool.patch.ToolContextPatchSpec;
import io.github.agentic.ai.graph.agent.tool.patch.ToolContextPatchViolationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import static io.github.agentic.ai.graph.agent.tools.ToolContextConstants.AGENT_STATE_FOR_UPDATE_CONTEXT_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Merge-boundary behavior of the opt-in {@link ToolContextPatchPolicy} on
 * {@link AgentToolNode}: failed calls contribute no state (acceptance 1 of T37) and
 * reserved keys, undeclared keys and type mismatches are rejected (acceptance 2). All
 * scenarios run without a model; the node is driven directly with neutral tools.
 */
@Timeout(10)
class AgentToolNodeContextPatchPolicyTest {

	private static final Duration TIMEOUT = Duration.ofMillis(300);

	// === Acceptance 1: a failed call must not pollute the next turn's context ===

	@Test
	void discardsPatchWhenToolThrowsAfterWriting() throws Exception {
		Tool failing = tool("failing", (call, context) -> {
			updates(context).put("leak", "leaked");
			throw new IllegalStateException("boom");
		});
		var output = execute(false, policy(), null, failing);
		assertThat(output).doesNotContainKey("leak");
		assertThat(messages(output)).hasSize(1);
	}

	@Test
	void discardsPatchWhenToolReturnsErrorResult() throws Exception {
		Tool failing = writing("failing", "leak", CompletableFuture.completedFuture(ToolResult.error("boom")));
		var output = execute(false, policy(), null, failing);
		assertThat(output).doesNotContainKey("leak");
		assertThat(messages(output)).hasSize(1);
	}

	@Test
	void errorResultPatchStillMergesWithoutPolicy_legacyBehavior() throws Exception {
		Tool failing = writing("failing", "leak", CompletableFuture.completedFuture(ToolResult.error("boom")));
		var output = execute(false, null, null, failing);
		assertThat(output).containsEntry("leak", "leaked");
	}

	@Test
	void discardsPatchOnBusinessMappedFailure() throws Exception {
		var mappers = BusinessOutcomeMapperRegistry.builder()
			.declare("failing", (call, result) -> ToolResult.error("business failure"))
			.build();
		Tool failing = writing("failing", "leak", CompletableFuture.completedFuture(ToolResult.text("looks fine")));
		var output = execute(false, policy(), mappers, failing);
		assertThat(output).doesNotContainKey("leak");
		assertThat(messages(output)).hasSize(1);
	}

	@Test
	void discardsPatchWhenToolTimesOut() throws Exception {
		var stalled = new CompletableFuture<ToolResult>();
		Tool failing = writing("failing", "leak", stalled);
		var output = execute(false, policy(), null, failing);
		assertThat(output).doesNotContainKey("leak");
		assertThat(stalled).isCancelled();
	}

	@Test
	void keepsPatchOfSuccessfulCallWhileDiscardingFailedOne() throws Exception {
		Tool ok = writing("ok", "kept", CompletableFuture.completedFuture(ToolResult.text("fine")));
		Tool failing = writing("failing", "leak", CompletableFuture.completedFuture(ToolResult.error("boom")));
		var output = execute(false, policy(), null, ok, failing);
		assertThat(output).containsEntry("kept", "leaked").doesNotContainKey("leak");
		assertThat(messages(output)).hasSize(2);
	}

	@Test
	void parallelDiscardsPatchOfFailedCallOnly() throws Exception {
		Tool ok = writing("ok", "kept", CompletableFuture.completedFuture(ToolResult.text("fine")));
		Tool failing = writing("failing", "leak", CompletableFuture.completedFuture(ToolResult.error("boom")));
		var output = execute(true, policy(), null, ok, failing);
		assertThat(output).containsEntry("kept", "leaked").doesNotContainKey("leak");
		assertThat(messages(output)).hasSize(2);
	}

	// === Acceptance 2: reserved keys and declared specs are enforced ===

	@Test
	void rejectsPatchWritingMessagesProtocolKey() throws Exception {
		Tool violating = writingEntries("violating", Map.of("messages", List.of("hijacked"), "business", "value"),
				CompletableFuture.completedFuture(ToolResult.text("done")));
		var output = execute(false, policy(), null, violating);
		// The whole patch is one unit: both entries are dropped, protocol stays intact.
		assertThat(output).doesNotContainKey("business");
		assertThat(messages(output)).hasSize(1);
		assertThat(messages(output).get(0).text()).isEqualTo("done");
	}

	@Test
	void rejectsPatchWritingArgiNamespace() throws Exception {
		Tool violating = writingEntries("violating", Map.of("argi.actionDecision.final", "STOP"),
				CompletableFuture.completedFuture(ToolResult.text("done")));
		var output = execute(false, policy(), null, violating);
		assertThat(output).doesNotContainKey("argi.actionDecision.final");
	}

	@Test
	void rejectsPatchViolatingDeclaredSpec() throws Exception {
		var spec = ToolContextPatchSpec.builder().writableKey("allowed").keyType("allowed", String.class).build();
		var policy = ToolContextPatchPolicy.builder().specFor("violating", spec).build();
		Tool typeMismatch = writingEntries("violating", Map.of("allowed", 42),
				CompletableFuture.completedFuture(ToolResult.text("done")));
		Tool undeclared = writingEntries("violating", Map.of("allowed", "fine", "other", "value"),
				CompletableFuture.completedFuture(ToolResult.text("done")));

		assertThat(execute(false, policy, null, typeMismatch)).doesNotContainKey("allowed");
		assertThat(execute(false, policy, null, undeclared)).doesNotContainKey("allowed");
	}

	@Test
	void acceptsPatchConformingToDeclaredSpec() throws Exception {
		var spec = ToolContextPatchSpec.builder().writableKey("allowed").keyType("allowed", String.class).build();
		var policy = ToolContextPatchPolicy.builder().specFor("obedient", spec).build();
		Tool obedient = writingEntries("obedient", Map.of("allowed", "value"),
				CompletableFuture.completedFuture(ToolResult.text("done")));
		var output = execute(false, policy, null, obedient);
		assertThat(output).containsEntry("allowed", "value");
	}

	@Test
	void abortsRunOnViolationWhenConfigured() throws Exception {
		Tool violating = writingEntries("violating", Map.of("messages", List.of("hijacked")),
				CompletableFuture.completedFuture(ToolResult.text("done")));
		var policy = ToolContextPatchPolicy.builder().onViolation(ToolContextPatchPolicy.ViolationPolicy.ABORT_RUN)
			.build();
		assertThatThrownBy(() -> execute(false, policy, null, violating))
			.isInstanceOf(ToolContextPatchViolationException.class)
			.hasMessageContaining("messages");
	}

	@Test
	void parallelRejectsReservedKeyWhileMergingOthers() throws Exception {
		Tool violating = writingEntries("violating", Map.of("messages", List.of("hijacked")),
				CompletableFuture.completedFuture(ToolResult.text("done")));
		Tool ok = writing("ok", "kept", CompletableFuture.completedFuture(ToolResult.text("fine")));
		var output = execute(true, policy(), null, violating, ok);
		assertThat(output).containsEntry("kept", "leaked");
		assertThat(messages(output)).hasSize(2);
		assertThat(messages(output).get(0).text()).isEqualTo("done");
	}

	@Test
	void reservedKeyFlowsThroughWithoutPolicy_legacyBehavior() throws Exception {
		Tool violating = writingEntries("violating", Map.of("messages", List.of("hijacked")),
				CompletableFuture.completedFuture(ToolResult.text("done")));
		var output = execute(false, null, null, violating);
		// Legacy merge order lets the patch overwrite the protocol messages entry;
		// this pin documents why a policy is needed, and that the default is unchanged.
		assertThat(output.get("messages")).isEqualTo(List.of("hijacked"));
	}

	// === harness ===

	private static ToolContextPatchPolicy policy() {
		return ToolContextPatchPolicy.builder().build();
	}

	private static Map<String, Object> execute(boolean parallel, ToolContextPatchPolicy policy,
			BusinessOutcomeMapperRegistry mappers, Tool... tools) throws Exception {
		var calls = new ArrayList<ToolCall>();
		for (int index = 0; index < tools.length; index++) {
			calls.add(new ToolCall("call-" + index, tools[index].definition().name(), "{}"));
		}
		var state = new OverAllState(Map.of("messages", new ArrayList<>(List.of(ChatMessage.assistant("", calls)))));
		var builder = AgentToolNode.builder()
			.toolRegistry(new ToolRegistry(List.of(tools)))
			.parallelToolExecution(parallel)
			.throwOnToolError(false)
			.toolExecutionTimeout(TIMEOUT);
		if (policy != null) {
			builder.toolContextPatchPolicy(policy);
		}
		if (mappers != null) {
			builder.businessOutcomeMappers(mappers);
		}
		return builder.build().apply(state, RunnableConfig.builder().build());
	}

	private static Tool writing(String name, String key, CompletionStage<ToolResult> stage) {
		return writingEntries(name, Map.of(key, "leaked"), stage);
	}

	private static Tool writingEntries(String name, Map<String, Object> entries, CompletionStage<ToolResult> stage) {
		return tool(name, (call, context) -> {
			updates(context).putAll(entries);
			return stage;
		});
	}

	private static Tool tool(String name, BiFunction<ToolCall, ToolContext, CompletionStage<ToolResult>> execution) {
		return new Tool() {
			public io.github.agentic.ai.api.chat.ToolDefinition definition() {
				return new io.github.agentic.ai.api.chat.ToolDefinition(name, name, "{}");
			}

			public CompletionStage<ToolResult> execute(ToolCall call, ToolContext context) {
				return execution.apply(call, context);
			}
		};
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> updates(ToolContext context) {
		return (Map<String, Object>) context.attributes().get(AGENT_STATE_FOR_UPDATE_CONTEXT_KEY);
	}

	@SuppressWarnings("unchecked")
	private static List<ChatMessage> messages(Map<String, Object> output) {
		return (List<ChatMessage>) output.get("messages");
	}

}
