/*
 * Copyright 2024-2026 the original author or authors.
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

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.agentic.ai.graph.CompiledGraph;
import io.github.agentic.ai.graph.OverAllState;
import io.github.agentic.ai.graph.OverAllStateBuilder;
import io.github.agentic.ai.graph.RunnableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

/**
 * ScheduledGraphExecution
 *
 * @author yaohui &#064;create 2025/8/20 15:04
 **/
public class ScheduledAgentTask {

	private static final Logger log = LoggerFactory.getLogger(CompiledGraph.class);

	private final ScheduledAgentManager manager;

	private final TaskScheduler taskScheduler;

	private final CompiledGraph graph;

	private final ScheduleConfig config;

	private final Object lifecycleLock = new Object();

	private ScheduledFuture<?> scheduledFuture;

	private volatile State state = State.NEW;

	private volatile boolean started = false;

	private final AtomicBoolean unregistered = new AtomicBoolean(false);

	private final String taskId;

	public ScheduledAgentTask(CompiledGraph graph, ScheduleConfig config) {
		this.graph = graph;
		this.config = config;
		this.manager = ScheduledAgentManagerFactory.getInstance().getManager();
		this.taskScheduler = manager.getTaskScheduler();
		// Register with the active manager; later unregistration goes to the same owner
		this.taskId = manager.registerTask(this);
		log.debug("Created ScheduledAgentTask with ID: {}", taskId);
	}

	/**
	 * Start the scheduled execution
	 */
	public ScheduledAgentTask start() {
		synchronized (lifecycleLock) {
			switch (state) {
				case NEW:
					state = State.STARTING;
					break;
				case STOPPED:
					throw new IllegalStateException("Schedule already stopped");
				case FAILED:
					throw new IllegalStateException("Schedule failed to start");
				default:
					throw new IllegalStateException("Schedule already started");
			}
		}

		ScheduledFuture<?> future;
		try {
			future = schedule();
		}
		catch (RuntimeException e) {
			synchronized (lifecycleLock) {
				if (state == State.STARTING) {
					state = State.FAILED;
				}
			}
			unregisterTask();
			throw e;
		}

		boolean running;
		synchronized (lifecycleLock) {
			running = state == State.STARTING;
			if (running) {
				scheduledFuture = future;
				started = true;
				state = State.RUNNING;
			}
		}
		if (!running) {
			// stop() won the race while scheduling; drop the late future
			future.cancel(false);
			return this;
		}

		notifyListeners(ScheduleLifecycleListener.ScheduleEvent.STARTED);
		return this;
	}

	private ScheduledFuture<?> schedule() {
		switch (config.getMode()) {
			case CRON:
				return taskScheduler.schedule(this::executeGraph, new CronTrigger(config.getCronExpression()));
			case FIXED_DELAY:
				return taskScheduler.scheduleWithFixedDelay(this::executeGraph,
						Instant.now().plusMillis(config.getInitialDelay()), Duration.ofMillis(config.getFixedDelay()));
			case FIXED_RATE:
				return taskScheduler.scheduleAtFixedRate(this::executeGraph,
						Instant.now().plusMillis(config.getInitialDelay()), Duration.ofMillis(config.getFixedRate()));
			case ONE_TIME:
				return taskScheduler.schedule(this::executeGraph, Instant.now().plusMillis(config.getInitialDelay()));
			case TRIGGER:
				return taskScheduler.schedule(this::executeGraph, config.getTrigger());
			default:
				throw new IllegalStateException("Unexpected value: " + config.getMode());
		}
	}

	/**
	 * Stop the scheduled execution
	 */
	public void stop() {
		ScheduledFuture<?> future;
		synchronized (lifecycleLock) {
			if (state == State.STOPPED) {
				return;
			}
			state = State.STOPPED;
			future = scheduledFuture;
		}
		if (future != null && !future.isCancelled()) {
			future.cancel(false);
		}

		// Unregister from active manager
		unregisterTask();
		log.debug("Stopped and unregistered ScheduledAgentTask with ID: {}", taskId);

		notifyListeners(ScheduleLifecycleListener.ScheduleEvent.STOPPED);
	}

	/**
	 * Execute the graph
	 * @param runnableConfig
	 * @param inputs
	 */
	public void execute(RunnableConfig runnableConfig, Map<String, Object> inputs) {
		try {
			executeGraphAttempt(runnableConfig, inputs);
		}
		catch (Exception e) {
			// Public manual execution historically reports failures through listeners
			// without throwing to callers.
		}
	}

	/**
	 * Execute the graph with retry logic
	 */
	private void executeGraph() {
		try {
			int attempt = 0;
			while (attempt <= config.getMaxRetries()) {
				try {
					executeGraphAttempt(config.getRunnableConfig(), config.getInputs());
					return;
				}
				catch (Exception e) {
					log.warn("Graph execution failed (attempt {}): {}", attempt + 1, e.getMessage());
					if (attempt < config.getMaxRetries() && config.getRetryPredicate().apply(e)) {
						attempt++;
						try {
							Thread.sleep(config.getRetryDelay().toMillis());
						}
						catch (InterruptedException ie) {
							Thread.currentThread().interrupt();
							break;
						}
					}
					else {
						break;
					}
				}
			}
		}
		finally {
			if (config.getMode() == ScheduleConfig.ScheduleMode.ONE_TIME) {
				unregisterTask();
			}
		}
	}

	private Optional<OverAllState> executeGraphAttempt(RunnableConfig runnableConfig, Map<String, Object> inputs) {
		try {
			notifyListeners(ScheduleLifecycleListener.ScheduleEvent.EXECUTION_STARTED);
			OverAllState initialState = createInitialState(inputs);
			if (runnableConfig == null) {
				String threadId = String.format("%s-%d", taskId, System.currentTimeMillis());
				runnableConfig = RunnableConfig.builder().threadId(threadId).build();
			}
			Optional<OverAllState> result = graph.invoke(initialState, runnableConfig);
			notifyListeners(ScheduleLifecycleListener.ScheduleEvent.EXECUTION_COMPLETED, result.orElse(null));
			return result;
		}
		catch (Exception e) {
			log.error("Graph execution failed", e);
			notifyListeners(ScheduleLifecycleListener.ScheduleEvent.EXECUTION_FAILED, e);
			throw e;
		}
	}

	private OverAllState createInitialState(Map<String, Object> inputs) {
		return OverAllStateBuilder.builder()
			.withKeyStrategies(graph.getKeyStrategyMap())
			.withData(inputs)
			.build();
	}

	private void notifyListeners(ScheduleLifecycleListener.ScheduleEvent event, Object data) {
		config.getListeners().forEach(listener -> {
			try {
				listener.onEvent(event, data);
			}
			catch (Exception e) {
				log.error("Error in schedule listener", e);
			}
		});
	}

	private void notifyListeners(ScheduleLifecycleListener.ScheduleEvent event) {
		notifyListeners(event, null);
	}

	private void unregisterTask() {
		if (unregistered.compareAndSet(false, true)) {
			manager.unregisterTask(taskId);
		}
	}

	// Getters for monitoring
	public boolean isStarted() {
		return started;
	}

	public boolean isStopped() {
		return state == State.STOPPED;
	}

	/**
	 * Get the unique task ID assigned by the global manager
	 */
	public String getTaskId() {
		return taskId;
	}

	/**
	 * Get the name of the agent associated with this task
	 */
	public String getName() {
		return graph.stateGraph.getName();
	}

	private enum State {

		NEW, STARTING, RUNNING, STOPPED, FAILED

	}

}
