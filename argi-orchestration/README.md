# ARGI Orchestration

This directory groups the orchestration engines while keeping their responsibilities separate.

- `argi-loop-core` executes stateful graphs with nodes, edges, state, streaming,
  checkpoints, and stores.
- `argi-workflow-core` defines versioned declarative workflows independently of
  the graph engine.
- `argi-agent-framework` provides the current high-level Agent and multi-agent
  orchestration facade.

The published artifact identifiers remain unchanged. `argi-loop-core` keeps its
existing graph package names in this structural migration; package migration is
tracked separately from the module layout change.
