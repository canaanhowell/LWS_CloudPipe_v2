# Google Analytics Data API (GA4) Setup Guide

This guide will help you set up the Google Analytics Data API (GA4) script (`main/google.py`) to retrieve analytics data and upload it to Azure Blob Storage.

## Prerequisites

1. **Google Cloud Console Account**
2. **Google Analytics 4 (GA4) Property** (not Universal Analytics)
3. **Python 3.10+** with virtual environment
4. **Azure Storage Account** (already configured)

## Step 1: Set Up Google Cloud Console

### 1.1 Create a Google Cloud Project
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the **Google Analytics Data API (GA4)**

### 1.2 Create OAuth 2.0 Credentials
1. In Google Cloud Console, go to **APIs & Services** > **Credentials**
2. Click **Create Credentials** > **OAuth 2.0 Client IDs**
3. Choose **Desktop application** as the application type
4. Download the JSON file and rename it to `client_secrets.json`
5. Place `client_secrets.json` in your project root directory

### 1.3 Get Your Google Analytics Property ID
1. Go to [Google Analytics](https://analytics.google.com/)
2. Navigate to **Admin** > **Property Settings**
3. Copy the **Property ID** (format: `123456789`)
4. **Important**: Make sure you're using a GA4 property, not Universal Analytics

## Step 2: Configure Environment Variables

Add the following to your `local.settings.json` file:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "your_azure_connection_string",
    "GOOGLE_ANALYTICS_PROPERTY_ID": "your_property_id_here",
    "GOOGLE_CLIENT_SECRETS_FILE": "client_secrets.json"
  }
}
```

## Step 3: Install Dependencies

The required packages are already in `requirements.txt`. Install them:

```bash
pip install -r requirements.txt
```

## Step 4: First-Time Authentication

When you run the script for the first time:

1. Run the script: `python main/google.py`
2. A browser window will open for Google OAuth authentication
3. Sign in with your Google account and grant permissions
4. The script will save authentication tokens in `token.pickle`

## Step 5: Run the Script

```bash
python main/google.py
```

## Features

### 📊 **Default Analytics Data (GA4)**
The script retrieves the following metrics by default:
- **Sessions** - Total number of sessions
- **Total Users** - Total number of users
- **Screen Page Views** - Total number of page views
- **Bounce Rate** - Session bounce rate

### 📅 **Default Dimensions (GA4)**
- **Date** - Date of the session
- **Source** - Traffic source
- **Medium** - Traffic medium

### 📈 **Date Range**
- Default: Last 30 days
- Customizable via parameters

## GA4 vs Universal Analytics

### Key Differences:
- **Property ID** instead of View ID
- **Different metric names** (e.g., `screenPageViews` instead of `pageviews`)
- **Enhanced data model** with events and parameters
- **Better privacy controls** and data retention

## Customization

### Custom Metrics and Dimensions

You can modify the script to retrieve different data:

```python
# Custom GA4 metrics
metrics = ['sessions', 'totalUsers', 'screenPageViews', 'averageSessionDuration']

# Custom GA4 dimensions
dimensions = ['date', 'country', 'city', 'deviceCategory']

# Get data with custom parameters
df = ga_api.get_analytics_data(
    start_date='2024-01-01',
    end_date='2024-12-31',
    metrics=metrics,
    dimensions=dimensions
)
```

### Available GA4 Metrics
- `sessions` - Total sessions
- `totalUsers` - Total users
- `activeUsers` - Active users
- `screenPageViews` - Screen page views
- `bounceRate` - Bounce rate
- `averageSessionDuration` - Average session duration
- `sessionsPerUser` - Sessions per user
- `eventCount` - Event count
- `conversions` - Conversions
- `totalRevenue` - Total revenue
- `purchaseToViewRate` - Purchase to view rate
- `checkouts` - Checkouts
- `addToCarts` - Add to carts
- `cartToViewRate` - Cart to view rate
- `ecommercePurchases` - E-commerce purchases

### Available GA4 Dimensions
- `date`, `dateHour`, `dateHourMinute`, `year`, `month`, `week`
- `dayOfWeek`, `dayOfYear`, `hour`, `minute`
- `source`, `medium`, `campaign`, `term`, `content`
- `country`, `region`, `city`, `continent`, `subContinent`
- `deviceCategory`, `deviceBrand`, `deviceModel`, `operatingSystem`
- `browser`, `browserVersion`, `platform`, `platformVersion`
- `pagePath`, `pageTitle`, `pageReferrer`, `pageLocation`
- `eventName`, `eventParameterName`, `eventParameterValue`
- `userPropertyName`, `userPropertyValue`

## Output

### 📁 **Files Generated**
- `google_analytics_ga4_YYYYMMDD_HHMMSS.csv` - Uploaded to Azure Blob Storage
- `token.pickle` - Authentication tokens (auto-generated)

### 📊 **Data Format**
The script outputs a CSV file with:
- Clean column names (GA4 format)
- Structured data ready for Power BI
- Timestamped filenames for version control

## Troubleshooting

### Common Issues

1. **"Client secrets file not found"**
   - Ensure `client_secrets.json` is in the project root
   - Check the file path in environment variables

2. **"No property ID provided"**
   - Add `GOOGLE_ANALYTICS_PROPERTY_ID` to your environment variables
   - Verify the property ID is correct
   - Make sure you're using a GA4 property, not Universal Analytics

3. **"Authentication errors"**
   - Delete `token.pickle` and re-authenticate
   - Check that your Google account has access to the Analytics property

4. **"API quota exceeded"**
   - Google Analytics Data API has daily quotas
   - Consider reducing the date range or frequency of requests

5. **"Property not found"**
   - Verify you're using a GA4 property ID
   - Check that the property exists and you have access

### Error Messages

- **"Not authenticated"** - Run authentication first
- **"No data retrieved"** - Check property ID and date range
- **"Azure connection string not found"** - Verify Azure configuration
- **"Property not found"** - Verify GA4 property ID

## Security Notes

- Keep `client_secrets.json` secure and don't commit to version control
- The `token.pickle` file contains sensitive authentication data
- Use environment variables for sensitive configuration

## Integration with Power BI

The generated CSV files are automatically uploaded to Azure Blob Storage in the `pbi25` container, making them available for Power BI data refresh.

## Migration from Universal Analytics

If you're migrating from Universal Analytics to GA4:

1. **Property ID**: Use GA4 property ID, not UA view ID
2. **Metrics**: Update metric names to GA4 format
3. **Data Structure**: GA4 has a different data model
4. **Historical Data**: GA4 data starts from when you created the property

## Support

For issues with:
- **Google Analytics Data API**: Check [GA4 API Documentation](https://developers.google.com/analytics/devguides/reporting/data/v1)
- **Authentication**: Review [Google OAuth 2.0 Documentation](https://developers.google.com/identity/protocols/oauth2)
- **Azure Storage**: Check Azure Storage documentation
- **GA4 Migration**: Review [GA4 Migration Guide](https://support.google.com/analytics/answer/10089681) 