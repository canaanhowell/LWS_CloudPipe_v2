# Azure Container Instance Setup for LWS CloudPipe

This guide explains how to deploy and run your `orchestrate_pipeline.py` pipeline using **Azure Container Instances (ACI)** for on-demand, cost-effective execution—no always-on web server required.

---

## Switching to Azure Container Instances While Reusing Existing Resources

You can move from Azure Container Apps (or other compute) to Azure Container Instances **without recreating your existing Azure resources**. Here’s what you can reuse:

- **Azure Container Registry (ACR):** Continue pushing your Docker images to your existing registry (e.g., `lwsdatapipeline.azurecr.io`). ACI will pull images from here.
- **Resource Group:** Use your current resource group (e.g., `lws-data-rg`) to organize all related resources.
- **Storage Accounts, Databases, and Other Services:** Your ACI container can access the same Azure Storage, Blob containers, databases (Snowflake, etc.), and other services, as long as you provide the correct credentials/config (via environment variables or files).
- **Networking (VNET/Subnet):** If you use a VNET or subnet for secure access, you can deploy ACI into the same network.
- **Monitoring (Application Insights, Log Analytics):** You can configure ACI to send logs/metrics to existing monitoring resources if needed.

**What changes?**
- The only new resource is the container instance itself, which is created each time you run your pipeline. You pay only for the time it runs.
- You do **not** need to create a new registry, storage account, or resource group—just reuse your existing ones.

**Summary Table:**

| Resource Type         | Reusable with ACI? | Notes                                  |
|-----------------------|-------------------|----------------------------------------|
| Container Registry    | Yes               | Use same ACR for images                |
| Resource Group        | Yes               | Use same group for all resources       |
| Storage Accounts      | Yes               | Use same storage, update config as needed |
| Databases             | Yes               | Use same, provide credentials          |
| VNET/Subnet           | Yes (optional)    | For private networking                 |
| Monitoring            | Yes (optional)    | For logs/metrics                       |

---

## 1. Build Your Docker Image

```sh
docker build -t lwsdatapipeline.azurecr.io/lws-data-pipeline:aci-<version> .
```
- Replace `<version>` with a unique tag (e.g., `aci-20250707-1`).

---

## 2. Push the Image to Azure Container Registry

```sh
docker push lwsdatapipeline.azurecr.io/lws-data-pipeline:aci-<version>
```

---

## 3. Run the Container in Azure Container Instances

```sh
az container create \
  --resource-group lws-data-rg \
  --name lws-pipeline-run-<version> \
  --image lwsdatapipeline.azurecr.io/lws-data-pipeline:aci-<version> \
  --registry-login-server lwsdatapipeline.azurecr.io \
  --registry-username <ACR_USERNAME> \
  --registry-password <ACR_PASSWORD> \
  --cpu 2 \
  --memory 4 \
  --restart-policy Never \
  --environment-variables <KEY1>=<VALUE1> <KEY2>=<VALUE2> ...
```
- Replace `<version>` with your tag.
- Set environment variables as needed for your pipeline (e.g., credentials, config).
- Use `--restart-policy Never` so the container stops after the script finishes.

---

## 4. Monitor the Container Run

```sh
az container show --resource-group lws-data-rg --name lws-pipeline-run-<version> --output table
az container logs --resource-group lws-data-rg --name lws-pipeline-run-<version>
```
- Check status and view logs to confirm successful execution.

---

## 5. Clean Up (Optional)

After the run completes, delete the container instance to avoid charges:

```sh
az container delete --resource-group lws-data-rg --name lws-pipeline-run-<version> --yes
```

---

## 6. (Optional) Automate with a Script or Logic App
- You can automate this process with a shell script, Azure Logic App, or GitHub Actions for scheduled or event-driven runs.

---

## Notes
- **No web server needed:** The container runs your script and exits.
- **Cost-effective:** You pay only for the run time.
- **No persistent endpoint:** Trigger runs via CLI, automation, or Azure tools.
- **Logs:** All output is available via `az container logs`.

---

**For more details:**
- [Azure Container Instances Documentation](https://docs.microsoft.com/en-us/azure/container-instances/) 