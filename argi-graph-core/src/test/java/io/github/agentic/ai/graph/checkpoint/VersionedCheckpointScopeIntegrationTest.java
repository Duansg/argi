/*
 * Copyright 2025-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.agentic.ai.graph.checkpoint;

import io.github.agentic.ai.graph.CompileConfig;
import io.github.agentic.ai.graph.CompiledGraph;
import io.github.agentic.ai.graph.KeyStrategy;
import io.github.agentic.ai.graph.NodeOutput;
import io.github.agentic.ai.graph.RunnableConfig;
import io.github.agentic.ai.graph.StateGraph;
import io.github.agentic.ai.graph.action.AsyncNodeActionWithConfig;
import io.github.agentic.ai.graph.checkpoint.config.SaverConfig;
import io.github.agentic.ai.graph.checkpoint.savers.MemorySaver;
import io.github.agentic.ai.graph.checkpoint.savers.VersionedMemoryCheckpointSaver;
import io.github.agentic.ai.graph.state.strategy.ReplaceStrategy;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static io.github.agentic.ai.graph.StateGraph.END;
import static io.github.agentic.ai.graph.StateGraph.START;
import static io.github.agentic.ai.graph.action.AsyncEdgeAction.edge_async;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(30)
class VersionedCheckpointScopeIntegrationTest {

	@Test
	void staleVersionedExecutionCannotReplaceWinningCheckpoint() throws Exception {
		VersionedMemoryCheckpointSaver backend = new VersionedMemoryCheckpointSaver();
		VersionedCheckpointSaver staleFacade = new FacadeVersionedCheckpointSaver(backend);
		VersionedCheckpointSaver winnerFacade = new FacadeVersionedCheckpointSaver(backend);
		ControlledGraph stale = new ControlledGraph(staleFacade);
		ControlledGraph winner = new ControlledGraph(winnerFacade);
		RunnableConfig config = config("stale-run");

		CompletableFuture<NodeOutput> staleUpdate = stale.submit("stale", config);
		Invocation staleInvocation = stale.awaitInvocation("stale");
		CompletableFuture<NodeOutput> winningUpdate = winner.submit("winner", config);
		Invocation winnerInvocation = winner.awaitInvocation("winner");

		winnerInvocation.complete("winner");
		assertEquals("winner", result(winningUpdate).state().value("result").orElseThrow());

		staleInvocation.complete("stale");
		ExecutionException failure = assertThrows(ExecutionException.class,
				() -> staleUpdate.get(10, TimeUnit.SECONDS));
		assertInstanceOf(CheckpointConflictException.class, rootCause(failure));
		assertEquals("winner", backend.getVersioned(config).checkpoint().orElseThrow().getState().get("result"));
	}

	@Test
	void staleScopedManualUpdateCannotReplaceWinningCheckpointOrReroute() throws Exception {
		VersionedMemoryCheckpointSaver backend = new VersionedMemoryCheckpointSaver();
		VersionedCheckpointSaver staleFacade = new FacadeVersionedCheckpointSaver(backend);
		VersionedCheckpointSaver winnerFacade = new FacadeVersionedCheckpointSaver(backend);
		AtomicInteger routeCalls = new AtomicInteger();
		CompiledGraph staleGraph = routedGraph(staleFacade, routeCalls);
		CompiledGraph winnerGraph = routedGraph(winnerFacade, routeCalls);
		RunnableConfig config = config("manual-update");

		CompletableFuture<VersionedCheckpointScope> staleScope = captureScope(staleFacade, config);
		RunnableConfig winnerConfig = winnerGraph.updateState(seed(winnerFacade, config, "seed"),
				Map.of("result", "winner"), "route");

		CheckpointConflictException conflict = assertThrows(CheckpointConflictException.class,
				() -> staleGraph.updateState(winnerConfig, Map.of("result", "stale"), "route", staleScope.get()));

		assertEquals(0, conflict.getExpectedRevision());
		assertEquals("winner", backend.getVersioned(config).checkpoint().orElseThrow().getState().get("result"));
		assertEquals(1, routeCalls.get());
	}

	@Test
	void staleScopedReleaseCannotRemoveWinningCheckpoint() throws Exception {
		VersionedMemoryCheckpointSaver backend = new VersionedMemoryCheckpointSaver();
		VersionedCheckpointSaver staleFacade = new FacadeVersionedCheckpointSaver(backend);
		VersionedCheckpointSaver winnerFacade = new FacadeVersionedCheckpointSaver(backend);
		RunnableConfig config = config("manual-release");

		CompletableFuture<VersionedCheckpointScope> staleScope = captureScope(staleFacade, config);
		seed(winnerFacade, config, "winner");

		CheckpointConflictException conflict = assertThrows(CheckpointConflictException.class,
				() -> staleScope.get().release(config));

		assertEquals(0, conflict.getExpectedRevision());
		assertEquals("winner", backend.getVersioned(config).checkpoint().orElseThrow().getState().get("result"));
	}

	@Test
	void coldSubscriptionsGetFreshScopesAndNamespacesStayIndependent() throws Exception {
		VersionedMemoryCheckpointSaver backend = new VersionedMemoryCheckpointSaver();
		RunnableConfig firstThread = config("first-thread");
		RunnableConfig secondThread = config("second-thread");

		VersionedCheckpointScope.withScope(backend, firstThread, scope -> {
			try {
				scope.put(firstThread, checkpoint("first"));
				return reactor.core.publisher.Flux.just(scope.snapshot().revision());
			}
			catch (Exception ex) {
				return reactor.core.publisher.Flux.error(ex);
			}
		}).blockLast(Duration.ofSeconds(2));

		Long freshRevision = VersionedCheckpointScope
			.withScope(backend, firstThread, scope -> reactor.core.publisher.Flux.just(scope.snapshot().revision()))
			.blockLast(Duration.ofSeconds(2));
		Long independentRevision = VersionedCheckpointScope
			.withScope(backend, secondThread, scope -> reactor.core.publisher.Flux.just(scope.snapshot().revision()))
			.blockLast(Duration.ofSeconds(2));

		assertEquals(1, freshRevision);
		assertEquals(0, independentRevision);
	}

	@Test
	void legacySaverStillAllowsLastWriterToWin() throws Exception {
		MemorySaver backend = new MemorySaver();
		ControlledGraph stale = new ControlledGraph(new FacadeCheckpointSaver(backend));
		ControlledGraph winner = new ControlledGraph(new FacadeCheckpointSaver(backend));
		RunnableConfig config = config("legacy-run");

		CompletableFuture<NodeOutput> staleUpdate = stale.submit("stale", config);
		Invocation staleInvocation = stale.awaitInvocation("stale");
		CompletableFuture<NodeOutput> winningUpdate = winner.submit("winner", config);
		Invocation winnerInvocation = winner.awaitInvocation("winner");

		winnerInvocation.complete("winner");
		assertEquals("winner", result(winningUpdate).state().value("result").orElseThrow());
		staleInvocation.complete("stale");

		assertEquals("stale", result(staleUpdate).state().value("result").orElseThrow());
		assertEquals("stale", backend.get(config).orElseThrow().getState().get("result"));
	}

	private static CompletableFuture<VersionedCheckpointScope> captureScope(VersionedCheckpointSaver saver,
			RunnableConfig config) {
		return VersionedCheckpointScope
			.withScope(saver, config, scope -> reactor.core.publisher.Flux.just(scope))
			.single()
			.toFuture();
	}

	private static RunnableConfig seed(VersionedCheckpointSaver saver, RunnableConfig config, String result)
			throws Exception {
		return saver.putIfVersion(config, checkpoint(result), saver.getVersioned(config).revision());
	}

	private static Checkpoint checkpoint(String result) {
		return Checkpoint.builder().state(Map.of("result", result)).nodeId("route").nextNodeId(END).build();
	}

	private static CompiledGraph routedGraph(VersionedCheckpointSaver saver, AtomicInteger routeCalls) throws Exception {
		return new StateGraph(() -> Map.<String, KeyStrategy>of("result", new ReplaceStrategy()))
			.addNode("route", AsyncNodeActionWithConfig.node_async((state, config) -> Map.of()))
			.addConditionalEdges("route", edge_async(state -> {
				routeCalls.incrementAndGet();
				return END;
			}), Map.of(END, END))
			.addEdge(START, "route")
			.compile(CompileConfig.builder().saverConfig(SaverConfig.builder().register(saver).build()).build());
	}

	private static RunnableConfig config(String threadId) {
		return RunnableConfig.builder().threadId(threadId).build();
	}

	private static NodeOutput result(CompletableFuture<NodeOutput> future) throws Exception {
		return future.get(10, TimeUnit.SECONDS);
	}

	private static Throwable rootCause(Throwable failure) {
		Throwable cause = failure;
		while (cause.getCause() != null) {
			cause = cause.getCause();
		}
		return cause;
	}

	private record Invocation(String request, CompletableFuture<Map<String, Object>> completion) {

		private void complete(String result) {
			assertTrue(completion.complete(Map.of("result", result)));
		}

	}

	private static final class ControlledGraph {

		private final CompiledGraph graph;

		private final BlockingQueue<Invocation> pending = new LinkedBlockingQueue<>();

		private ControlledGraph(BaseCheckpointSaver saver) throws Exception {
			this.graph = new StateGraph(() -> Map.<String, KeyStrategy>of("result", new ReplaceStrategy()))
				.addNode("write", (state, config) -> {
					String request = state.value("request", String.class).orElseThrow();
					CompletableFuture<Map<String, Object>> completion = new CompletableFuture<>() {
						@Override
						public boolean cancel(boolean mayInterruptIfRunning) {
							return false;
						}
					};
					pending.add(new Invocation(request, completion));
					return completion;
				})
				.addEdge(START, "write")
				.addEdge("write", END)
				.compile(CompileConfig.builder().saverConfig(SaverConfig.builder().register(saver).build()).build());
		}

		private CompletableFuture<NodeOutput> submit(String request, RunnableConfig config) {
			return graph.stream(Map.of("request", request), config).last().toFuture();
		}

		private Invocation awaitInvocation(String request) throws InterruptedException {
			Invocation invocation = pending.poll(10, TimeUnit.SECONDS);
			assertNotNull(invocation);
			assertEquals(request, invocation.request());
			return invocation;
		}

	}

	private record FacadeVersionedCheckpointSaver(VersionedCheckpointSaver delegate) implements VersionedCheckpointSaver {

		@Override
		public Collection<Checkpoint> list(RunnableConfig config) {
			return delegate.list(config);
		}

		@Override
		public Optional<Checkpoint> get(RunnableConfig config) {
			return delegate.get(config);
		}

		@Override
		public RunnableConfig put(RunnableConfig config, Checkpoint checkpoint) throws Exception {
			return delegate.put(config, checkpoint);
		}

		@Override
		public BaseCheckpointSaver.Tag release(RunnableConfig config) throws Exception {
			return delegate.release(config);
		}

		@Override
		public CheckpointSnapshot getVersioned(RunnableConfig config) {
			return delegate.getVersioned(config);
		}

		@Override
		public RunnableConfig putIfVersion(RunnableConfig config, Checkpoint checkpoint, long expectedRevision)
				throws Exception {
			return delegate.putIfVersion(config, checkpoint, expectedRevision);
		}

		@Override
		public BaseCheckpointSaver.Tag releaseIfVersion(RunnableConfig config, long expectedRevision) throws Exception {
			return delegate.releaseIfVersion(config, expectedRevision);
		}

	}

	private record FacadeCheckpointSaver(BaseCheckpointSaver delegate) implements BaseCheckpointSaver {

		@Override
		public Collection<Checkpoint> list(RunnableConfig config) {
			return delegate.list(config);
		}

		@Override
		public Optional<Checkpoint> get(RunnableConfig config) {
			return delegate.get(config);
		}

		@Override
		public RunnableConfig put(RunnableConfig config, Checkpoint checkpoint) throws Exception {
			return delegate.put(config, checkpoint);
		}

		@Override
		public BaseCheckpointSaver.Tag release(RunnableConfig config) throws Exception {
			return delegate.release(config);
		}

	}

}
