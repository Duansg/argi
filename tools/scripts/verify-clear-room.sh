#!/usr/bin/env bash
#
# Copyright 2024-2026 the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
readonly REPO_ROOT

fail() {
	echo "Clear Room gate failed: $*" >&2
	exit 1
}

[[ -d "${REPO_ROOT}/argi-orchestration/argi-workflow-graph-core" ]] || fail "argi-workflow-graph-core is missing"
[[ ! -e "${REPO_ROOT}/argi-graph-core" ]] || fail "legacy argi-graph-core directory still exists"

grep -q '<module>argi-orchestration/argi-workflow-graph-core</module>' "${REPO_ROOT}/pom.xml" \
	|| fail "root reactor does not declare argi-workflow-graph-core"
grep -q '<artifactId>argi-workflow-graph-core</artifactId>' "${REPO_ROOT}/argi-orchestration/argi-workflow-graph-core/pom.xml" \
	|| fail "Workflow Graph Core artifact id is missing"

for module in argi-agent/argi-agent-api argi-agent/argi-agent-core \
	argi-orchestration/argi-workflow-core argi-runtime/argi-runtime-api \
	argi-runtime/argi-runtime-core; do
	if grep -n 'argi-workflow-graph-core' "${REPO_ROOT}/${module}/pom.xml"; then
		fail "${module} must not depend on Workflow Graph Core"
	fi
done

legacy_production_refs="$(git -C "${REPO_ROOT}" grep -l 'argi-graph-core' -- \
	. \
	':(exclude)AGENTS.md' \
	':(exclude)docs/**' \
	':(exclude)argi-orchestration/argi-workflow-graph-core/README.md' \
	':(exclude)argi-orchestration/argi-workflow-graph-core/src/test/**' \
	':(exclude)benchmark/**' \
	':(exclude)example/**' \
	':(exclude)tools/scripts/verify-clear-room.sh' \
	':(exclude)**/target/**' \
	':(exclude)**/.flattened-pom.xml' || true)"
[[ -z "${legacy_production_refs}" ]] || fail "legacy artifact references remain:${legacy_production_refs}"

git -C "${REPO_ROOT}" grep -q 'GRAPH("graph")' -- argi-runtime/argi-runtime-api/src/main/java \
	|| fail "DefinitionKind.GRAPH wire name is not preserved"
git -C "${REPO_ROOT}" grep -q -E 'graphId|graphVersion' -- argi-runtime/argi-runtime-core/src/main/java \
	|| fail "graph definition identifiers are not documented in runtime code"
git -C "${REPO_ROOT}" grep -q -E 'ARGI_CHAT_MESSAGE|LegacyChatRecordSupport' \
	-- argi-orchestration/argi-workflow-graph-core/src/main \
	|| fail "Loop serializer compatibility boundary is missing"

for document in clear-room-baseline.md checkpoint-compatibility.md api-boundaries.md; do
	[[ -f "${REPO_ROOT}/docs/architecture/${document}" ]] \
		|| fail "architecture document is missing: ${document}"
done

if [[ -d "${REPO_ROOT}/benchmark" ]]; then
	if grep -R -n --include='*.xml' --exclude-dir=target 'argi-graph-core' "${REPO_ROOT}/benchmark"; then
		fail "benchmark submodule still references argi-graph-core"
	fi
fi
if [[ -d "${REPO_ROOT}/example" ]]; then
	if grep -R -n --include='*.xml' --exclude-dir=target 'argi-graph-core' "${REPO_ROOT}/example"; then
		fail "example submodule still references argi-graph-core"
	fi
fi

echo "Clear Room ownership, artifact, wire-name, and consumer-boundary gates passed"
