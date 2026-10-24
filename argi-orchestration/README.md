# ARGI Orchestration

This directory groups the orchestration engines while keeping their responsibilities separate.

- `argi-workflow-graph-core` executes stateful graphs with nodes, edges, state, streaming,
  checkpoints, and stores.
- `argi-workflow-core` defines versioned declarative workflows independently of
  the graph engine.
- `argi-agent-framework` provides the current high-level Agent and multi-agent
  orchestration facade.

The graph engine artifact is named `argi-workflow-graph-core`, and its public graph
packages are rooted at `io.github.agentic.ai.workflow.graph`. The independent Agent
contracts are rooted at `io.github.agentic.ai.agent`, while the graph-backed
orchestration framework uses `io.github.agentic.ai.agent.orchestration`.
