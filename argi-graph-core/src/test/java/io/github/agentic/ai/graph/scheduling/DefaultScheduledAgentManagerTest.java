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
package io.github.agentic.ai.graph.scheduling;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.github.agentic.ai.graph.CompiledGraph;
import io.github.agentic.ai.graph.KeyStrategy;
import io.github.agentic.ai.graph.StateGraph;
import io.github.agentic.ai.graph.action.AsyncNodeAction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static io.github.agentic.ai.graph.StateGraph.END;
import static io.github.agentic.ai.graph.StateGraph.START;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultScheduledAgentManagerTest {

	private final DefaultScheduledAgentManager manager = new DefaultScheduledAgentManager();

	@AfterEach
	void tearDown() {
		manager.shutdown();
		ScheduledAgentManagerFactory.getInstance().registerProvider(DefaultScheduledAgentManager::getInstance);
	}

	@Test
	void registrationRacingShutdownIsRejectedAndLeavesNoTask() throws Exception {
		ScheduledAgentManagerFactory.getInstance().registerProvider(() -> manager);
		CountDownLatch shutdownInProgress = new CountDownLatch(1);
		CountDownLatch releaseShutdown = new CountDownLatch(1);
		ScheduledAgentTask existing = new ScheduledAgentTask(graph(), ScheduleConfig.builder()
			.fixedDelay(60_000)
			.initialDelay(60_000)
			.addListener(new ScheduleLifecycleListener() {
				@Override
				public void onEvent(ScheduleEvent event, Object data) {
					if (event == ScheduleEvent.STOPPED) {
						shutdownInProgress.countDown();
						await(releaseShutdown);
					}
				}
			})
			.build()).start();
		CompiledGraph lateGraph = graph();

		Thread shutdown = new Thread(manager::shutdown, "scheduled-manager-shutdown-test");
		shutdown.start();
		assertTrue(shutdownInProgress.await(5, TimeUnit.SECONDS));

		AtomicReference<Throwable> registrationFailure = new AtomicReference<>();
		Thread registration = new Thread(() -> {
			try {
				new ScheduledAgentTask(lateGraph, ScheduleConfig.builder().initialDelay(0).build());
			}
			catch (Throwable throwable) {
				registrationFailure.set(throwable);
			}
		}, "scheduled-manager-register-test");
		registration.start();
		awaitBlocked(registration);
		releaseShutdown.countDown();
		shutdown.join(TimeUnit.SECONDS.toMillis(5));
		registration.join(TimeUnit.SECONDS.toMillis(5));

		assertFalse(shutdown.isAlive());
		assertFalse(registration.isAlive());
		assertTrue(existing.isStopped());
		assertInstanceOf(IllegalStateException.class, registrationFailure.get());
		assertTrue(manager.isShutdown());
		assertEquals(0, manager.getActiveTaskCount());
	}

	@Test
	void registrationAfterShutdownIsRejected() throws Exception {
		ScheduledAgentManagerFactory.getInstance().registerProvider(() -> manager);
		CompiledGraph graph = graph();
		manager.shutdown();

		assertThrows(IllegalStateException.class,
				() -> new ScheduledAgentTask(graph, ScheduleConfig.builder().initialDelay(0).build()));
		assertEquals(0, manager.getActiveTaskCount());
	}

	@Test
	void shutdownStopsAndUnregistersTasksWithoutDeadlock() throws Exception {
		ScheduledAgentManagerFactory.getInstance().registerProvider(() -> manager);
		ScheduledAgentTask task = new ScheduledAgentTask(graph(),
				ScheduleConfig.builder().fixedDelay(60_000).initialDelay(60_000).build()).start();

		manager.shutdown();
		manager.shutdown();
		task.stop();

		assertTrue(task.isStopped());
		assertEquals(0, manager.getActiveTaskCount());
		assertFalse(manager.unregisterTask(task.getTaskId()));
	}

	private static void awaitBlocked(Thread thread) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (thread.getState() != Thread.State.WAITING && thread.getState() != Thread.State.BLOCKED) {
			assertTrue(thread.isAlive() && System.nanoTime() < deadline, "registration did not block on shutdown");
			Thread.onSpinWait();
		}
	}

	private static void await(CountDownLatch latch) {
		try {
			assertTrue(latch.await(5, TimeUnit.SECONDS));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	private static CompiledGraph graph() throws Exception {
		Map<String, KeyStrategy> strategies = new HashMap<>();
		strategies.put("result", (left, right) -> right);
		return new StateGraph("schedule-manager-test", () -> strategies)
			.addNode("attempt", AsyncNodeAction.node_async(state -> Map.of("result", "ok")))
			.addEdge(START, "attempt")
			.addEdge("attempt", END)
			.compile();
	}

}
