# LWS CloudPipe v2

A comprehensive data pipeline solution for cloud-based data processing and integration.

## Overview

LWS CloudPipe v2 is a modern data pipeline framework designed to handle data integration, transformation, and delivery across multiple cloud platforms including Azure, PowerBI, and Snowflake.

## Features

- **Multi-Cloud Integration**: Support for Azure, PowerBI, Snowflake, and Google Analytics
- **Docker Containerization**: Easy deployment with Docker and Docker Compose
- **Configuration Management**: Secure handling of credentials and connection strings
- **Logging and Monitoring**: Comprehensive logging and progress tracking
- **Modular Architecture**: Extensible pipeline scripts and helper utilities

## Project Structure

```
LWS_CloudPipe_v2/
├── config_files/          # Configuration and credential files
├── data/                  # Data storage and processing
│   └── csv/              # CSV data files
├── docker-compose*.yml    # Docker Compose configurations
├── Dockerfile            # Docker container definition
├── helper_scripts/       # Utility scripts and tools
│   ├── Tests/           # Test scripts
│   └── Utils/           # Utility modules
├── logs/                 # Log files and progress tracking
├── markdown_docs/        # Documentation
└── pipeline_scripts/     # Main pipeline execution scripts
```

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Access to Azure, PowerBI, and Snowflake services

## Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd LWS_CloudPipe_v2
   ```

2. **Configure your environment**
   - Copy configuration templates from `config_files/`
   - Update with your service credentials
   - Ensure sensitive files are properly secured

3. **Run with Docker**
   ```bash
   # Development environment
   docker-compose -f docker-compose.dev.yml up

   # Production environment
   docker-compose -f docker-compose.prod.yml up

   # Azure-specific deployment
   docker-compose -f docker-compose.azure.yml up
   ```

## Configuration

### Azure Setup
- Configure Azure Storage credentials
- Set up Logic Apps for automated workflows
- Configure Azure AD authentication

### PowerBI Integration
- Set up PowerBI workspace connections
- Configure data refresh schedules
- Manage dataset permissions

### Snowflake Connection
- Configure Snowflake account and warehouse
- Set up user roles and permissions
- Manage private key authentication

## Documentation

- [Connections Guide](markdown_docs/connections.md)
- [PowerBI Setup Guide](markdown_docs/POWERBI_CONNECTION_GUIDE.md)
- [Google Analytics Setup](markdown_docs/GOOGLE_ANALYTICS_SETUP.md)
- [SharePoint Azure App Connection](markdown_docs/SHAREPOINT_AZURE_APP_CONNECTION.md)

## Security

⚠️ **Important Security Notes:**
- Never commit sensitive configuration files to version control
- Use environment variables for secrets in production
- Regularly rotate access tokens and keys
- Follow the principle of least privilege for service accounts

## Development

### Adding New Integrations
1. Create new pipeline scripts in `pipeline_scripts/`
2. Add configuration templates in `config_files/`
3. Update documentation in `markdown_docs/`
4. Add tests in `helper_scripts/Tests/`

### Testing
```bash
# Run tests
python -m pytest helper_scripts/Tests/

# Run specific test suite
python -m pytest helper_scripts/Tests/test_integration.py
```

## Logging

The pipeline uses structured logging with JSON format. Logs are stored in:
- `logs/log.json` - Main application logs
- `logs/progress.md` - Human-readable progress tracking

## Troubleshooting

Common issues and solutions:

1. **Connection Timeouts**: Check network connectivity and firewall settings
2. **Authentication Errors**: Verify credentials and token expiration
3. **Docker Issues**: Ensure Docker daemon is running and ports are available

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

[Add your license information here]

## Support

For support and questions:
- Check the documentation in `markdown_docs/`
- Review logs in `logs/`
- Create an issue in the repository

---

**Version**: 2.0  
**Last Updated**: [Current Date] 