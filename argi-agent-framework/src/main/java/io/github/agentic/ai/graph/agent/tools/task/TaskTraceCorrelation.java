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

package io.github.agentic.ai.graph.agent.tools.task;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.github.agentic.ai.graph.observation.trace.AgentTraceContext;

/**
 * Shared, in-memory correlation store between the {@link TaskTool} delegation site and the
 * {@link TaskOutputTool} receive site of background tasks.
 * <p>
 * The producer span (background sub-agent execution) runs on a repository worker thread and
 * is usually finished by the time the caller polls for the result, so the receive span
 * cannot be its child. Both sides instead reference the same {@link AgentTraceContext},
 * whose delegation identifier becomes the span link key of the consumer. Entries are only
 * recorded when an {@link AgentTracer} is enabled.
 */
public final class TaskTraceCorrelation {

	private final Map<String, AgentTraceContext> contextsByTaskId = new ConcurrentHashMap<>();

	/** @return an empty correlation store */
	public static TaskTraceCorrelation empty() {
		return new TaskTraceCorrelation();
	}

	/**
	 * Record the correlation facts of a background delegation.
	 * @param taskId the background task id
	 * @param context the correlation facts captured at the delegation site
	 */
	public void record(String taskId, AgentTraceContext context) {
		if (taskId != null && context != null) {
			this.contextsByTaskId.put(taskId, context);
		}
	}

	/**
	 * @param taskId the background task id
	 * @return the correlation facts recorded at the delegation site, if any
	 */
	public Optional<AgentTraceContext> correlation(String taskId) {
		return taskId == null ? Optional.empty() : Optional.ofNullable(this.contextsByTaskId.get(taskId));
	}

	/**
	 * Drop the correlation facts of a finished background task.
	 * @param taskId the background task id
	 */
	public void forget(String taskId) {
		if (taskId != null) {
			this.contextsByTaskId.remove(taskId);
		}
	}

}
