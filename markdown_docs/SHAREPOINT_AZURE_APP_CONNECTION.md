# SharePoint Connection via Azure Registered App

This document explains how this project connects to SharePoint Online using a registered Azure AD application. Use this as a reference for any scripts or automations that need to access SharePoint resources securely.

---

## 1. **Azure App Registration**
- **Register an app** in Azure Active Directory (Azure Portal).
- **Grant API permissions** (Microsoft Graph > Delegated or Application permissions as needed, e.g., `Sites.Read.All`, `Files.Read.All`).
- **Create a client secret** for the app.
- **Record the following values:**
  - **Tenant ID**
  - **Client ID**
  - **Client Secret**

## 2. **Environment Variables**
Set these in `local.settings.json` or your environment:
- `AZURE_TENANT_ID`: Your Azure AD tenant ID
- `azure_client_id`: The app's client ID
- `azure_client_secret`: The app's client secret
- `AZURE_STORAGE_CONNECTION_STRING`: For Azure Blob Storage access
- `STORAGE_CONTAINER`: Name of the blob container (e.g., `pbi25`)

## 3. **Authentication Flow**
- Uses **Client Credentials Flow** via `msal` or `azure-identity`:
  ```python
  from azure.identity import ClientSecretCredential
  credential = ClientSecretCredential(
      tenant_id=os.environ['AZURE_TENANT_ID'],
      client_id=os.environ['azure_client_id'],
      client_secret=os.environ['azure_client_secret']
  )
  token = credential.get_token('https://graph.microsoft.com/.default')
  access_token = token.token
  ```
- This token is used in the `Authorization` header for Microsoft Graph API requests.

## 4. **Accessing SharePoint Files**
- Use Microsoft Graph API endpoints to:
  - List sites and drives
  - Find files/folders by path
  - Download files (Excel, etc.)
- Example request:
  ```python
  headers = {'Authorization': f'Bearer {access_token}'}
  url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/path/to/file.xlsx:/content'
  response = requests.get(url, headers=headers)
  ```

## 5. **Extending for Future Use Cases**
- This pattern can be reused for:
  - Uploading files to SharePoint
  - Listing folders/files
  - Automating document workflows
  - Integrating with Teams, OneDrive, etc.
- Just update the Graph API endpoints and permissions as needed.

## 6. **Security Notes**
- **Never commit secrets** to source control.
- Rotate client secrets regularly.
- Use least-privilege permissions for the app.

---

**To use this document in a new session, just ask the AI to refer to `markdown_docs/SHAREPOINT_AZURE_APP_CONNECTION.md`.**

_This is a living document—add more details and examples as your SharePoint automation grows!_ 