#!/bin/bash
# elastic-blast-janitor-azure.sh — Clean up stale ElasticBLAST AKS resources
#
# Finds and deletes AKS clusters that have been idle beyond the TTL.
# Designed to run as a CronJob or Azure Function to prevent cost leaks.
#
# Environment variables:
#   ELB_JANITOR_RESOURCE_GROUP - Azure resource group to scan
#   ELB_JANITOR_TTL_HOURS      - Maximum idle hours before cleanup (default: 24)
#   ELB_JANITOR_DRY_RUN        - Set to "true" for dry run mode
#   ELB_JANITOR_TAG_KEY         - Tag key to identify ElasticBLAST clusters (default: "project")
#   ELB_JANITOR_TAG_VALUE       - Tag value to match (default: "elastic-blast")

set -o pipefail

TTL_HOURS=${ELB_JANITOR_TTL_HOURS:-24}
DRY_RUN=${ELB_JANITOR_DRY_RUN:-false}
RESOURCE_GROUP=${ELB_JANITOR_RESOURCE_GROUP:?Resource group is required}
TAG_KEY=${ELB_JANITOR_TAG_KEY:-project}
TAG_VALUE=${ELB_JANITOR_TAG_VALUE:-elastic-blast}

echo "ElasticBLAST Azure Janitor"
echo "Resource group: $RESOURCE_GROUP"
echo "TTL: ${TTL_HOURS}h, Dry run: $DRY_RUN"
echo "Tag filter: ${TAG_KEY}=${TAG_VALUE}"

# Get AKS clusters with matching tags
clusters=$(az aks list \
    --resource-group "$RESOURCE_GROUP" \
    --query "[?tags.${TAG_KEY}=='${TAG_VALUE}'].{name:name, created:timeCreated}" \
-o json 2>/dev/null)

if [ -z "$clusters" ] || [ "$clusters" = "[]" ]; then
    echo "No ElasticBLAST clusters found"
    exit 0
fi

now=$(date +%s)
cutoff=$((now - TTL_HOURS * 3600))

echo "$clusters" | jq -r '.[] | "\(.name) \(.created)"' | while read -r name created; do
    created_epoch=$(date -d "$created" +%s 2>/dev/null || echo 0)
    
    if [ "$created_epoch" -lt "$cutoff" ]; then
        age_hours=$(( (now - created_epoch) / 3600 ))
        echo "STALE: $name (age: ${age_hours}h, TTL: ${TTL_HOURS}h)"
        
        if [ "$DRY_RUN" = "true" ]; then
            echo "  [DRY RUN] Would delete cluster: $name"
        else
            echo "  Deleting cluster: $name"
            az aks delete \
            --resource-group "$RESOURCE_GROUP" \
            --name "$name" \
            --yes --no-wait
            echo "  Deletion initiated for: $name"
        fi
    else
        age_hours=$(( (now - created_epoch) / 3600 ))
        echo "OK: $name (age: ${age_hours}h, TTL: ${TTL_HOURS}h)"
    fi
done

echo "Janitor run complete"
