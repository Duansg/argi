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
package io.github.agentic.ai.graph.agent.a2a;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.a2a.spec.AgentCard;
import io.github.agentic.ai.graph.GraphResponse;
import io.github.agentic.ai.graph.NodeOutput;
import io.github.agentic.ai.graph.OverAllState;
import io.github.agentic.ai.graph.RunnableConfig;
import io.github.agentic.ai.graph.execution.DelegationScope;
import io.github.agentic.ai.graph.execution.ExecutionBudget;
import io.github.agentic.ai.graph.execution.PrincipalReference;
import io.github.agentic.ai.graph.execution.TenantReference;
import io.github.agentic.ai.graph.execution.TrustedExecutionContext;
import io.github.agentic.ai.graph.observation.trace.AgentTraceKeys;
import io.github.agentic.ai.graph.observation.trace.AgentTraceOperation;
import io.github.agentic.ai.graph.observation.trace.AgentTracer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class A2aNodeActionTraceCorrelationTests {

	private final RecordingHandler handler = new RecordingHandler();

	private final ObjectMapper json = new ObjectMapper();

	private final AtomicReference<JsonNode> capturedRequest = new AtomicReference<>();

	private AgentTracer enabledTracer() {
		ObservationRegistry registry = ObservationRegistry.create();
		registry.observationConfig().observationHandler(this.handler);
		return new AgentTracer(registry);
	}

	private RunnableConfig parentConfig() {
		return RunnableConfig.builder()
			.threadId("thread-1")
			.trustedExecutionContext(TrustedExecutionContext.builder()
				.tenant(new TenantReference("example-iam", "tenant-a"))
				.principal(new PrincipalReference("example-iam", "principal-a"))
				.runId("run-1")
				.correlationId("correlation-a")
				.budget(new ExecutionBudget(Map.of("tokens", 100L)))
				.delegationScope(new DelegationScope(Set.of("agents:delegate")))
				.build())
			.build();
	}

	private HttpServer startServer(String responseBody) throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/", exchange -> {
			try {
				this.capturedRequest.set(this.json.readTree(exchange.getRequestBody()));
				byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
				exchange.getResponseHeaders().add("Content-Type", "application/json");
				exchange.sendResponseHeaders(200, bytes.length);
				exchange.getResponseBody().write(bytes);
			}
			finally {
				exchange.close();
			}
		});
		server.start();
		return server;
	}

	@Test
	void sendAndReceiveSpansCorrelateAndPropagateProducerContext() throws Exception {
		HttpServer server = startServer("""
				{"jsonrpc":"2.0","id":"response-1","result":{"kind":"artifact-update","artifact":{"parts":[{"text":"remote ok"}]}}}
				""");
		PrintStream originalOut = System.out;
		ByteArrayOutputStream stdout = new ByteArrayOutputStream();
		try {
			System.setOut(new PrintStream(stdout, true, StandardCharsets.UTF_8));
			AgentTracer tracer = enabledTracer();
			String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
			A2aNodeActionWithConfig action = new A2aNodeActionWithConfig(createAgentCardWrapper(baseUrl), "remote-agent",
					false, "reply", "secret instruction", false, tracer);

			Map<String, Object> result = action.apply(new OverAllState(), parentConfig());

			assertThat(result.get("reply")).isEqualTo("remote ok");
		}
		finally {
			System.setOut(originalOut);
			server.stop(0);
		}

		// The producer context travels in the A2A request metadata.
		JsonNode metadata = this.capturedRequest.get().at("/params/metadata");
		assertThat(metadata.path("argiRunId").asText()).isEqualTo("run-1");
		assertThat(metadata.path("argiTenant").asText()).isEqualTo("tenant-a");
		assertThat(metadata.path("argiThreadId").asText()).isEqualTo("thread-1_subgraph_test-agent");
		assertThat(metadata.path("argiDelegationId").asText()).isNotBlank();

		// Send and receive spans share the delegation id and carry the correlation facts.
		assertThat(this.handler.stopped).hasSize(2);
		Observation.Context send = this.handler.stopped.get(0);
		Observation.Context receive = this.handler.stopped.get(1);
		assertThat(send.getName()).isEqualTo("argi.agent.a2a.send");
		assertThat(receive.getName()).isEqualTo("argi.agent.a2a.receive");
		assertThat(send.getLowCardinalityKeyValue(AgentTraceKeys.ROLE).getValue()).isEqualTo("producer");
		assertThat(receive.getLowCardinalityKeyValue(AgentTraceKeys.ROLE).getValue()).isEqualTo("consumer");
		String delegationId = send.getHighCardinalityKeyValue(AgentTraceKeys.DELEGATION_ID).getValue();
		assertThat(delegationId).isNotBlank();
		assertThat(receive.getHighCardinalityKeyValue(AgentTraceKeys.DELEGATION_ID).getValue()).isEqualTo(delegationId);
		assertThat(metadata.path("argiDelegationId").asText()).isEqualTo(delegationId);
		for (Observation.Context span : List.of(send, receive)) {
			assertThat(span.getHighCardinalityKeyValue(AgentTraceKeys.RUN_ID).getValue()).isEqualTo("run-1");
			assertThat(span.getHighCardinalityKeyValue(AgentTraceKeys.TENANT).getValue()).isEqualTo("tenant-a");
			assertThat(span.getLowCardinalityKeyValue(AgentTraceKeys.SUCCESS).getValue()).isEqualTo("true");
			assertThat(span.getHighCardinalityKeyValue(AgentTraceKeys.ERROR)).isNull();
		}
		// The instruction text is never recorded on the spans.
		assertThat(String.valueOf(send.getAllKeyValues())).doesNotContain("secret instruction");
	}

	@Test
	void streamingSpansSurviveTheWorkerThread() throws Exception {
		HttpServer server = startServer("""
				{"jsonrpc":"2.0","id":"response-1","result":{"kind":"artifact-update","artifact":{"parts":[{"text":"remote ok"}]}}}
				""");
		try {
			AgentTracer tracer = enabledTracer();
			String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
			A2aNodeActionWithConfig action = new A2aNodeActionWithConfig(createAgentCardWrapper(baseUrl), "remote-agent",
					false, "reply", "secret instruction", true, tracer);

			Flux<GraphResponse<NodeOutput>> flux = (Flux<GraphResponse<NodeOutput>>) action
				.apply(new OverAllState(), parentConfig())
				.get("reply");
			List<GraphResponse<NodeOutput>> outputs = flux.collectList().block(java.time.Duration.ofSeconds(5));
			assertThat(outputs).isNotNull();
			assertThat(((Map<?, ?>) outputs.get(outputs.size() - 1).resultValue().orElseThrow()).get("reply"))
				.isEqualTo("remote ok");
		}
		finally {
			server.stop(0);
		}

		assertThat(this.handler.stopped).hasSize(2);
		// In the streaming worker the receive span is nested inside the send span's HTTP
		// round trip, so it stops first; look the spans up by name instead of order.
		Observation.Context send = this.handler.stopped.stream()
			.filter(span -> span.getName().equals(AgentTraceOperation.A2A_SEND.operationName()))
			.findFirst()
			.orElseThrow();
		Observation.Context receive = this.handler.stopped.stream()
			.filter(span -> span.getName().equals(AgentTraceOperation.A2A_RECEIVE.operationName()))
			.findFirst()
			.orElseThrow();
		assertThat(send.getName()).isEqualTo("argi.agent.a2a.send");
		assertThat(receive.getName()).isEqualTo("argi.agent.a2a.receive");
		assertThat(receive.getHighCardinalityKeyValue(AgentTraceKeys.DELEGATION_ID).getValue())
			.isEqualTo(send.getHighCardinalityKeyValue(AgentTraceKeys.DELEGATION_ID).getValue());
		assertThat(send.getHighCardinalityKeyValue(AgentTraceKeys.RUN_ID).getValue()).isEqualTo("run-1");
		assertThat(receive.getHighCardinalityKeyValue(AgentTraceKeys.RUN_ID).getValue()).isEqualTo("run-1");
	}

	@Test
	void disabledTracerKeepsLegacyPayloadAndEmitsNoSpans() throws Exception {
		HttpServer server = startServer("""
				{"jsonrpc":"2.0","id":"response-1","result":{"kind":"artifact-update","artifact":{"parts":[{"text":"remote ok"}]}}}
				""");
		try {
			A2aNodeActionWithConfig action = new A2aNodeActionWithConfig(createAgentCardWrapper("http://127.0.0.1:"
					+ server.getAddress().getPort() + "/"), "remote-agent", false, "reply", "secret instruction", false);

			Map<String, Object> result = action.apply(new OverAllState(), parentConfig());

			assertThat(result.get("reply")).isEqualTo("remote ok");
		}
		finally {
			server.stop(0);
		}

		JsonNode metadata = this.capturedRequest.get().at("/params/metadata");
		assertThat(metadata.has("argiRunId")).isFalse();
		assertThat(metadata.has("argiDelegationId")).isFalse();
		assertThat(metadata.path("threadId").asText()).isEqualTo("thread-1_subgraph_test-agent");
		assertThat(this.handler.stopped).isEmpty();
	}

	private static AgentCardWrapper createAgentCardWrapper(String url) {
		AgentCard agentCard = mock(AgentCard.class);
		when(agentCard.name()).thenReturn("test-agent");
		when(agentCard.url()).thenReturn(url);
		return new AgentCardWrapper(agentCard);
	}

	static final class RecordingHandler implements ObservationHandler<Observation.Context> {

		final List<Observation.Context> stopped = new CopyOnWriteArrayList<>();

		@Override
		public boolean supportsContext(Observation.Context context) {
			return true;
		}

		@Override
		public void onStop(Observation.Context context) {
			this.stopped.add(context);
		}

	}

}
