#!/usr/bin/env bash
#
# Deploy the Azure Function proxy to multiple regions.
# Prerequisites: Azure CLI installed and logged in (az login).
#
# Usage:
#   ./deploy_azure_proxies.sh                  # deploy all regions
#   ./deploy_azure_proxies.sh westeurope       # deploy one region
#
# After deployment, the script prints the function URLs + keys.
# Copy them into azure_proxies.json (created automatically).
#
set -euo pipefail

RESOURCE_GROUP="njuskalo-proxy-rg"
STORAGE_PREFIX="njuskproxysa"   # max 24 chars after appending region abbreviation
APP_PREFIX="njusk-proxy"
PLAN_PREFIX="njusk-proxy-plan"
PYTHON_VERSION="3.11"
FUNC_DIR="$(cd "$(dirname "$0")/azure_proxy" && pwd)"

# Azure regions close to Croatia / spread across Europe
ALL_REGIONS=(
    "westeurope"          # Netherlands
    "northeurope"         # Ireland
    "germanywestcentral"  # Frankfurt
    "francecentral"       # Paris
    "uksouth"             # London
    "swedencentral"       # Sweden
    "polandcentral"       # Warsaw
    "italynorth"          # Milan
)

# If a specific region is passed, use only that
if [[ $# -ge 1 ]]; then
    REGIONS=("$1")
else
    REGIONS=("${ALL_REGIONS[@]}")
fi

echo "=== Creating resource group: $RESOURCE_GROUP ==="
az group create --name "$RESOURCE_GROUP" --location westeurope --output none 2>/dev/null || true

PROXY_CONFIG="[]"

for REGION in "${REGIONS[@]}"; do
    # Abbreviate region for storage account name (must be lowercase, no hyphens, max 24 chars)
    REGION_SHORT=$(echo "$REGION" | tr -d '-' | cut -c1-8)
    STORAGE_ACCOUNT="${STORAGE_PREFIX}${REGION_SHORT}"
    APP_NAME="${APP_PREFIX}-${REGION}"
    PLAN_NAME="${PLAN_PREFIX}-${REGION}"

    echo ""
    echo "=== Deploying to $REGION ==="

    # 1. Storage account
    echo "  Creating storage account: $STORAGE_ACCOUNT"
    az storage account create \
        --name "$STORAGE_ACCOUNT" \
        --resource-group "$RESOURCE_GROUP" \
        --location "$REGION" \
        --sku Standard_LRS \
        --output none 2>/dev/null || true

    # 2. Function app (consumption plan, Python)
    echo "  Creating function app: $APP_NAME"
    az functionapp create \
        --name "$APP_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --storage-account "$STORAGE_ACCOUNT" \
        --consumption-plan-location "$REGION" \
        --runtime python \
        --runtime-version "$PYTHON_VERSION" \
        --functions-version 4 \
        --os-type Linux \
        --output none 2>/dev/null || true

    # 3. Deploy code
    echo "  Deploying function code..."
    pushd "$FUNC_DIR" > /dev/null
    func azure functionapp publish "$APP_NAME" --python --no-build 2>/dev/null || \
        az functionapp deployment source config-zip \
            --resource-group "$RESOURCE_GROUP" \
            --name "$APP_NAME" \
            --src <(cd "$FUNC_DIR" && zip -r - . -x '*.pyc' '__pycache__/*') \
            --output none
    popd > /dev/null

    # 4. Get the function key
    echo "  Retrieving function key..."
    FUNC_KEY=$(az functionapp keys list \
        --name "$APP_NAME" \
        --resource-group "$RESOURCE_GROUP" \
        --query "functionKeys.default" \
        --output tsv 2>/dev/null || echo "")

    if [[ -z "$FUNC_KEY" ]]; then
        # Try the master key as fallback
        FUNC_KEY=$(az functionapp keys list \
            --name "$APP_NAME" \
            --resource-group "$RESOURCE_GROUP" \
            --query "masterKey" \
            --output tsv 2>/dev/null || echo "RETRIEVE_MANUALLY")
    fi

    FUNC_URL="https://${APP_NAME}.azurewebsites.net/api/fetch?code=${FUNC_KEY}"
    echo "  URL: $FUNC_URL"

    # Build JSON array entry
    PROXY_CONFIG=$(echo "$PROXY_CONFIG" | python3 -c "
import sys, json
arr = json.load(sys.stdin)
arr.append({'region': '$REGION', 'url': 'https://${APP_NAME}.azurewebsites.net/api/fetch', 'key': '$FUNC_KEY'})
print(json.dumps(arr, indent=2))
")

done

# Write config file
CONFIG_FILE="$(dirname "$0")/azure_proxies.json"
echo "$PROXY_CONFIG" > "$CONFIG_FILE"
echo ""
echo "=== Deployment complete ==="
echo "Proxy config written to: $CONFIG_FILE"
echo "Regions deployed: ${REGIONS[*]}"
