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
package io.github.agentic.ai.contract;

import io.github.agentic.ai.runtime.execution.TrustedExecutionContext;
import io.github.agentic.ai.runtime.execution.run.MemoryRunStore;
import io.github.agentic.ai.runtime.execution.run.RunCommit;
import io.github.agentic.ai.runtime.execution.run.RunLease;
import io.github.agentic.ai.runtime.execution.run.RunPayload;
import io.github.agentic.ai.runtime.execution.run.RunSnapshot;
import io.github.agentic.ai.runtime.execution.run.RunSpec;
import io.github.agentic.ai.runtime.execution.run.RunSpecCodec;
import io.github.agentic.ai.runtime.execution.run.RunState;
import io.github.agentic.ai.runtime.execution.run.RunStore;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/** Compile the opt-in contract as a consumer without a model SDK or Extensions. */
final class RunStoreApiFixture {

	private RunStoreApiFixture() {
	}

	static RunSnapshot compileOnly(TrustedExecutionContext authenticated) {
		RunStore store = new MemoryRunStore();
		RunSpec spec = RunSpec.from(authenticated, "application", "session", "request", "graph", "1",
				new RunPayload(1, "application.input", "{}"));
		RunSpecCodec codec = new RunSpecCodec();
		RunSnapshot admitted = store.submit(codec.decodeRun(codec.encodeRun(spec)), authenticated);
		RunLease lease = store.acquire(spec.namespace(), spec.runId(), UUID.randomUUID(), Duration.ofSeconds(30));
		return store.commit(spec.namespace(), spec.runId(), lease,
				new RunCommit(admitted.revision(), RunState.RUNNING, null, List.of(), List.of(), List.of()));
	}

}
