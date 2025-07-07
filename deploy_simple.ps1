# Simple Azure Container Deployment Script
param(
    [string]$ImageTag = "v2-emoji-fix"
)

# Azure Container Registry details
$acrName = "lwscloudpipe"
$acrLoginServer = "$acrName.azurecr.io"
$imageName = "lws-data-pipeline"
$fullImageName = $acrLoginServer + "/" + $imageName + ":" + $ImageTag

Write-Host "=== LWS CloudPipe v2 Azure Deployment ===" -ForegroundColor Green
Write-Host "Image: $fullImageName" -ForegroundColor Yellow
Write-Host "Timestamp: $(Get-Date)" -ForegroundColor Yellow
Write-Host ""

# Step 1: Login to Azure Container Registry
Write-Host "Step 1: Logging into Azure Container Registry..." -ForegroundColor Cyan
az acr login --name $acrName
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to login to Azure Container Registry"
    exit 1
}
Write-Host "✓ Successfully logged into ACR" -ForegroundColor Green

# Step 2: Build Docker Image
Write-Host "Step 2: Building Docker image..." -ForegroundColor Cyan
Write-Host "Building image: $fullImageName"
docker build -t $fullImageName .
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker build failed"
    exit 1
}
Write-Host "✓ Docker image built successfully" -ForegroundColor Green

# Step 3: Push Image to Azure Container Registry
Write-Host "Step 3: Pushing image to Azure Container Registry..." -ForegroundColor Cyan
docker push $fullImageName
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to push image to ACR"
    exit 1
}
Write-Host "✓ Image pushed successfully to ACR" -ForegroundColor Green

Write-Host ""
Write-Host "=== Deployment Summary ===" -ForegroundColor Green
Write-Host "Image: $fullImageName" -ForegroundColor Yellow
Write-Host "Status: Successfully built and pushed" -ForegroundColor Yellow
Write-Host ""
Write-Host "✓ Image is now available in Azure Container Registry!" -ForegroundColor Green
Write-Host "You can now deploy it to Azure Container Instance or Container App." -ForegroundColor Cyan 