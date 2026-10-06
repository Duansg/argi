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

import io.github.agentic.ai.graph.RunnableConfig;
import io.github.agentic.ai.graph.StateGraph;
import io.github.agentic.ai.graph.serializer.StateSerializer;
import io.github.agentic.ai.graph.serializer.check_point.CheckPointSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import reactor.core.publisher.Flux;

import static io.github.agentic.ai.graph.StateGraph.END;
import static io.github.agentic.ai.graph.StateGraph.START;

/**
 * Per-subscription revision owner for a {@link VersionedCheckpointSaver} namespace.
 */
public final class VersionedCheckpointScope {

	private static final Object CONTEXT_KEY = new Object();

	private final VersionedCheckpointSaver saver;

	private final String namespace;

	private final CheckPointSerializer checkpointSerializer;

	private CheckpointSnapshot currentSnapshot;

	private final CheckpointSnapshot preTurnSnapshot;

	private boolean ownMutation;

	private long lastOwnRevision;

	private CheckpointConflictException terminalConflict;

	private VersionedCheckpointScope(VersionedCheckpointSaver saver, RunnableConfig config,
			StateSerializer serializer) {
		this.saver = Objects.requireNonNull(saver, "saver cannot be null");
		this.namespace = saver.checkpointThreadId(Objects.requireNonNull(config, "config cannot be null"));
		this.checkpointSerializer = new CheckPointSerializer(
				Objects.requireNonNull(serializer, "serializer cannot be null"));
		CheckpointSnapshot initialSnapshot = cloneSnapshotUnchecked(saver.getVersioned(config));
		this.currentSnapshot = initialSnapshot;
		this.preTurnSnapshot = cloneSnapshotUnchecked(initialSnapshot);
		this.lastOwnRevision = currentSnapshot.revision();
	}

	public static <T> Flux<T> withScope(VersionedCheckpointSaver saver, RunnableConfig config,
			Function<VersionedCheckpointScope, Flux<T>> operation) {
		return withScope(saver, config, StateGraph.DEFAULT_JACKSON_SERIALIZER, operation);
	}

	public static <T> Flux<T> withScope(VersionedCheckpointSaver saver, RunnableConfig config,
			StateSerializer serializer, Function<VersionedCheckpointScope, Flux<T>> operation) {
		Objects.requireNonNull(saver, "saver cannot be null");
		Objects.requireNonNull(config, "config cannot be null");
		Objects.requireNonNull(serializer, "serializer cannot be null");
		Objects.requireNonNull(operation, "operation cannot be null");
		return Flux.deferContextual(context -> {
			ScopeKey key = new ScopeKey(saver, saver.checkpointThreadId(config));
			Map<ScopeKey, VersionedCheckpointScope> inherited = context.getOrDefault(CONTEXT_KEY, Map.of());
			VersionedCheckpointScope existing = inherited.get(key);
			if (existing != null) {
				return Flux.defer(() -> operation.apply(existing));
			}
			VersionedCheckpointScope scope = new VersionedCheckpointScope(saver, config, serializer);
			Map<ScopeKey, VersionedCheckpointScope> scopes = new HashMap<>(inherited);
			scopes.put(key, scope);
			return Flux.defer(() -> operation.apply(scope)).contextWrite(current -> current.put(CONTEXT_KEY, scopes));
		});
	}

	public synchronized CheckpointSnapshot snapshot() {
		return cloneSnapshotUnchecked(currentSnapshot);
	}

	public synchronized CheckpointSnapshot snapshot(RunnableConfig config) throws Exception {
		validateNamespace(config);
		failIfTerminated();
		if (config.checkPointId().isEmpty() || currentSnapshot.checkpoint()
			.map(Checkpoint::getId)
			.filter(config.checkPointId().get()::equals)
			.isPresent()) {
			return cloneSnapshot(currentSnapshot);
		}
		CheckpointSnapshot selected = cloneSnapshot(saver.getVersioned(config));
		if (selected.revision() != currentSnapshot.revision()) {
			throw rememberConflict(new CheckpointConflictException(namespace, currentSnapshot.revision(),
					selected.revision()));
		}
		return cloneSnapshot(selected);
	}

	public synchronized CheckpointSnapshot preTurnSnapshot() {
		return cloneSnapshotUnchecked(preTurnSnapshot);
	}

	public synchronized boolean hasOwnMutation() {
		return ownMutation;
	}

	public synchronized RunnableConfig put(RunnableConfig config, Checkpoint checkpoint) throws Exception {
		validateNamespace(config);
		failIfTerminated();
		Checkpoint ownedCheckpoint = cloneCheckpoint(checkpoint);
		long expectedRevision = currentSnapshot.revision();
		long nextRevision = nextRevision(expectedRevision);
		RunnableConfig updated;
		try {
			updated = saver.putIfVersion(config, ownedCheckpoint, expectedRevision);
		}
		catch (CheckpointConflictException ex) {
			throw rememberConflict(ex);
		}
		currentSnapshot = new CheckpointSnapshot(Optional.of(ownedCheckpoint), nextRevision);
		ownMutation = true;
		lastOwnRevision = nextRevision;
		return updated;
	}

	public synchronized BaseCheckpointSaver.Tag release(RunnableConfig config) throws Exception {
		validateNamespace(config);
		failIfTerminated();
		long expectedRevision = currentSnapshot.revision();
		long nextRevision = nextRevision(expectedRevision);
		BaseCheckpointSaver.Tag tag;
		try {
			tag = saver.releaseIfVersion(config, expectedRevision);
		}
		catch (CheckpointConflictException ex) {
			throw rememberConflict(ex);
		}
		currentSnapshot = new CheckpointSnapshot(Optional.empty(), nextRevision);
		ownMutation = true;
		lastOwnRevision = nextRevision;
		return tag;
	}

	public synchronized void rewind(RunnableConfig config) throws Exception {
		validateNamespace(config);
		failIfTerminated();
		if (!ownMutation) {
			return;
		}
		try {
			long nextRevision = nextRevision(lastOwnRevision);
			Checkpoint rewindCheckpoint = preTurnSnapshot.checkpoint()
				.orElseGet(() -> Checkpoint.builder().state(Map.of()).nodeId(START).nextNodeId(END).build());
			Checkpoint ownedCheckpoint = cloneCheckpoint(rewindCheckpoint);
			saver.putIfVersion(config, ownedCheckpoint, lastOwnRevision);
			currentSnapshot = new CheckpointSnapshot(Optional.of(ownedCheckpoint), nextRevision);
			lastOwnRevision = nextRevision;
			ownMutation = false;
		}
		catch (CheckpointConflictException ex) {
			rememberConflict(ex);
			// A newer owner has already moved the namespace; never reload or blind-rewind it.
		}
	}

	public void validate(VersionedCheckpointSaver expectedSaver, RunnableConfig config) {
		if (saver != expectedSaver) {
			throw new IllegalArgumentException("scope belongs to a different checkpoint saver");
		}
		validateNamespace(config);
	}

	private void validateNamespace(RunnableConfig config) {
		Objects.requireNonNull(config, "config cannot be null");
		String requestedNamespace = saver.checkpointThreadId(config);
		if (!namespace.equals(requestedNamespace)) {
			throw new IllegalArgumentException("scope belongs to checkpoint namespace '%s', not '%s'"
				.formatted(namespace, requestedNamespace));
		}
	}

	private long nextRevision(long revision) {
		return Math.addExact(revision, 1L);
	}

	private void failIfTerminated() throws CheckpointConflictException {
		if (terminalConflict != null) {
			throw terminalConflict;
		}
	}

	private CheckpointConflictException rememberConflict(CheckpointConflictException conflict) {
		if (terminalConflict == null) {
			terminalConflict = conflict;
		}
		return terminalConflict;
	}

	private CheckpointSnapshot cloneSnapshot(CheckpointSnapshot snapshot) throws IOException, ClassNotFoundException {
		Objects.requireNonNull(snapshot, "snapshot cannot be null");
		Optional<Checkpoint> checkpoint = snapshot.checkpoint().isPresent()
				? Optional.of(cloneCheckpoint(snapshot.checkpoint().orElseThrow())) : Optional.empty();
		return new CheckpointSnapshot(checkpoint, snapshot.revision());
	}

	private CheckpointSnapshot cloneSnapshotUnchecked(CheckpointSnapshot snapshot) {
		try {
			return cloneSnapshot(snapshot);
		}
		catch (IOException | ClassNotFoundException ex) {
			throw new IllegalStateException("Failed to clone checkpoint snapshot", ex);
		}
	}

	private Checkpoint cloneCheckpoint(Checkpoint checkpoint) throws IOException, ClassNotFoundException {
		Objects.requireNonNull(checkpoint, "checkpoint cannot be null");
		return checkpointSerializer.bytesToObject(checkpointSerializer.objectToBytes(checkpoint));
	}

	private record ScopeKey(VersionedCheckpointSaver saver, String namespace) {

		private ScopeKey {
			Objects.requireNonNull(saver, "saver cannot be null");
			Objects.requireNonNull(namespace, "namespace cannot be null");
		}

		@Override
		public boolean equals(Object other) {
			return other instanceof ScopeKey key && saver == key.saver && namespace.equals(key.namespace);
		}

		@Override
		public int hashCode() {
			return 31 * System.identityHashCode(saver) + namespace.hashCode();
		}

	}

}
