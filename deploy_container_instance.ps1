#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Deploy LWS CloudPipe v2 to Azure Container Instance
    
.DESCRIPTION
    This script builds the Docker image, pushes it to Azure Container Registry,
    and deploys it to Azure Container Instance for pipeline execution.
    
.PARAMETER ImageTag
    Tag for the Docker image (default: current date-time)
    
.PARAMETER ResourceGroup
    Azure Resource Group name (default: from settings)
    
.PARAMETER Location
    Azure region (default: westus2)
    
.EXAMPLE
    .\deploy_container_instance.ps1
    .\deploy_container_instance.ps1 -ImageTag "v2-emoji-fix" -ResourceGroup "lws-data-pipeline-rg"
#>

param(
    [string]$ImageTag = "",
    [string]$ResourceGroup = "lws-data-pipeline-rg",
    [string]$Location = "westus2"
)

# Set error action preference
$ErrorActionPreference = "Stop"

# Load settings
$settingsPath = "settings.json"
if (-not (Test-Path $settingsPath)) {
    Write-Error "settings.json not found. Please ensure the file exists."
    exit 1
}

$settings = Get-Content $settingsPath | ConvertFrom-Json

# Azure Container Registry details
$acrName = "lwsdatapipeline"
$acrLoginServer = "$acrName.azurecr.io"
$imageName = "lws-data-pipeline"

# Generate image tag if not provided
if ([string]::IsNullOrEmpty($ImageTag)) {
    $ImageTag = "aci-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
}

$fullImageName = "$acrLoginServer/$imageName`:$ImageTag"

Write-Host "=== LWS CloudPipe v2 Azure Deployment ===" -ForegroundColor Green
Write-Host "Image: $fullImageName" -ForegroundColor Yellow
Write-Host "Resource Group: $ResourceGroup" -ForegroundColor Yellow
Write-Host "Location: $Location" -ForegroundColor Yellow
Write-Host "Timestamp: $(Get-Date)" -ForegroundColor Yellow
Write-Host ""

# Step 1: Login to Azure Container Registry
Write-Host "Step 1: Logging into Azure Container Registry..." -ForegroundColor Cyan
try {
    az acr login --name $acrName
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to login to Azure Container Registry"
    }
    Write-Host "✓ Successfully logged into ACR" -ForegroundColor Green
} catch {
    Write-Error "Failed to login to Azure Container Registry: $_"
    exit 1
}

# Step 2: Build Docker Image
Write-Host "Step 2: Building Docker image..." -ForegroundColor Cyan
try {
    Write-Host "Building image: $fullImageName"
    docker build -t $fullImageName .
    if ($LASTEXITCODE -ne 0) {
        throw "Docker build failed"
    }
    Write-Host "✓ Docker image built successfully" -ForegroundColor Green
} catch {
    Write-Error "Failed to build Docker image: $_"
    exit 1
}

# Step 3: Push Image to Azure Container Registry
Write-Host "Step 3: Pushing image to Azure Container Registry..." -ForegroundColor Cyan
try {
    docker push $fullImageName
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to push image to ACR"
    }
    Write-Host "✓ Image pushed successfully to ACR" -ForegroundColor Green
} catch {
    Write-Error "Failed to push image to Azure Container Registry: $_"
    exit 1
}

# Step 4: Create or Update Container Instance
Write-Host "Step 4: Deploying to Azure Container Instance..." -ForegroundColor Cyan

# Container instance name
$containerInstanceName = "lws-pipeline-run-$(Get-Date -Format 'yyyyMMdd-HHmmss')"

# Environment variables for the container
$envVars = @(
    "AZURE_TENANT_ID=$($settings.AZURE_TENANT_ID)",
    "AZURE_CLIENT_ID=$($settings.AZURE_CLIENT_ID)", 
    "AZURE_CLIENT_SECRET=$($settings.AZURE_CLIENT_SECRET)",
    "AZURE_STORAGE_CONNECTION_STRING=$($settings.AZURE_STORAGE_CONNECTION_STRING)",
    "SNOWFLAKE_ACCOUNT=$($settings.SNOWFLAKE_ACCOUNT)",
    "SNOWFLAKE_USER=$($settings.SNOWFLAKE_USER)",
    "SNOWFLAKE_WAREHOUSE=$($settings.SNOWFLAKE_WAREHOUSE)",
    "SNOWFLAKE_DATABASE=$($settings.SNOWFLAKE_DATABASE)",
    "BLOB_CONTAINER=$($settings.BLOB_CONTAINER)",
    "SHAREPOINT_SITE_ID=$($settings.SHAREPOINT_SITE_ID)",
    "SHAREPOINT_CLIENT_ID=$($settings.SHAREPOINT_CLIENT_ID)",
    "SHAREPOINT_CLIENT_SECRET=$($settings.SHAREPOINT_CLIENT_SECRET)",
    "SHAREPOINT_SCOOP_DRIVE_ID=$($settings.SHAREPOINT_SCOOP_DRIVE_ID)",
    "SHAREPOINT_SCOOP_FOLDER_ID=$($settings.SHAREPOINT_SCOOP_FOLDER_ID)",
    "EXCEL_FILENAME=$($settings.EXCEL_FILENAME)"
)

# Join environment variables for az command
$envVarsString = $envVars -join " "

try {
    # Check if resource group exists, create if not
    $rgExists = az group exists --name $ResourceGroup
    if ($rgExists -eq "false") {
        Write-Host "Creating resource group: $ResourceGroup" -ForegroundColor Yellow
        az group create --name $ResourceGroup --location $Location
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to create resource group"
        }
    }

    # Create container instance
    Write-Host "Creating container instance: $containerInstanceName"
    
    # Build the az command with proper escaping
    $aciArgs = @(
        "container", "create",
        "--resource-group", $ResourceGroup,
        "--name", $containerInstanceName,
        "--image", $fullImageName,
        "--cpu", "1",
        "--memory", "2",
        "--restart-policy", "Never",
        "--registry-login-server", $acrLoginServer,
        "--registry-username", $settings.AZURE_CLIENT_ID,
        "--registry-password", $settings.AZURE_CLIENT_SECRET
    )
    
    # Add environment variables
    foreach ($envVar in $envVars) {
        $aciArgs += "--environment-variables"
        $aciArgs += $envVar
    }
    
    # Execute the command
    az @aciArgs
    
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create container instance"
    }
    
    Write-Host "✓ Container instance created successfully" -ForegroundColor Green
    
    # Get container instance details
    $containerDetails = az container show --resource-group $ResourceGroup --name $containerInstanceName | ConvertFrom-Json
    
    Write-Host ""
    Write-Host "=== Deployment Summary ===" -ForegroundColor Green
    Write-Host "Container Instance: $containerInstanceName" -ForegroundColor Yellow
    Write-Host "Image: $fullImageName" -ForegroundColor Yellow
    Write-Host "Resource Group: $ResourceGroup" -ForegroundColor Yellow
    Write-Host "Location: $Location" -ForegroundColor Yellow
    Write-Host "Status: $($containerDetails.provisioningState)" -ForegroundColor Yellow
    
    # Log deployment details
    $logEntry = @{
        timestamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ss.ffffff"
        operation = "azure_container_instance_deployment"
        status = "success"
        details = "Created Azure Container Instance with orchestrate_pipeline.py as main function"
        container_name = $containerInstanceName
        image = $fullImageName
        resource_group = $ResourceGroup
        location = $Location
        provisioning_state = $containerDetails.provisioningState
    }
    
    $logPath = "logs/deployment_log.json"
    $existingLogs = @()
    if (Test-Path $logPath) {
        $existingLogs = Get-Content $logPath | ConvertFrom-Json
    }
    $existingLogs += $logEntry
    $existingLogs | ConvertTo-Json -Depth 10 | Set-Content $logPath
    
    Write-Host ""
    Write-Host "✓ Deployment completed successfully!" -ForegroundColor Green
    Write-Host "Container instance will start automatically and run the pipeline." -ForegroundColor Cyan
    
} catch {
    Write-Error "Failed to deploy container instance: $_"
    exit 1
}

Write-Host ""
Write-Host "=== Next Steps ===" -ForegroundColor Cyan
Write-Host "1. Monitor the container instance: az container logs --resource-group $ResourceGroup --name $containerInstanceName" -ForegroundColor White
Write-Host "2. Check container status: az container show --resource-group $ResourceGroup --name $containerInstanceName" -ForegroundColor White
Write-Host "3. View logs in real-time: az container logs --resource-group $ResourceGroup --name $containerInstanceName --follow" -ForegroundColor White 