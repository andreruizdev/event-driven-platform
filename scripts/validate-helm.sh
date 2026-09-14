#!/usr/bin/env bash
set -e

echo "Running Infrastructure Security Scanner..."

HELM_TEMPLATES_DIR="deploy/helm/event-driven-lab/templates"

for file in "$HELM_TEMPLATES_DIR"/*.yaml; do
    if grep -qE "^kind:[[:space:]]*(Deployment|StatefulSet)" "$file"; then
        if ! grep -q "securityContext:" "$file"; then
            echo "=========================================================================="
            echo "❌ GOVERNANCE FAILURE: Infrastructure Security Violation!"
            echo "File '$(basename $file)' does not define a 'securityContext'."
            echo "Rule #5 (AGENTS.md): Default to the most restrictive settings possible."
            echo "=========================================================================="
            exit 1
        fi
        if ! grep -qE "(runAsNonRoot|allowPrivilegeEscalation|readOnlyRootFilesystem|capabilities)" "$file"; then
            echo "=========================================================================="
            echo "⚠️  GOVERNANCE WARNING: Incomplete securityContext constraints in '$(basename $file)'."
            echo "Ensure containers explicitly specify runAsNonRoot, allowPrivilegeEscalation, or capabilities."
            echo "=========================================================================="
        fi
    fi
done

echo "✅ All Helm charts passed security validation."
exit 0
