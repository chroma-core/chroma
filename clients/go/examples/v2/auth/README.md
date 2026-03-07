# Authentication Examples

This directory contains practical examples of different authentication methods for the Chroma Go v2 API.

## Available Examples

- **basic** - Basic authentication with username and password
- **bearer** - Bearer token authentication using Authorization header
- **x-chroma-token** - Token authentication using X-Chroma-Token header
- **custom-headers** - Custom headers for advanced authentication scenarios
- **cloud** - Chroma Cloud authentication with API key

## Running the Examples

To see all available examples:
```bash
go run main.go
```

### Prerequisites

1. For self-hosted examples, ensure Chroma is running locally:

```bash
docker run -p 8000:8000 chromadb/chroma
```

2. Set up authentication on your Chroma server as needed.

### Basic Authentication

```bash
# Set required environment variables
export CHROMA_AUTH_USERNAME="admin"
export CHROMA_AUTH_PASSWORD="your-password"
export CHROMA_URL="http://localhost:8000"  # optional, defaults to localhost:8000

# Run the example
go run main.go -example=basic
```

### Bearer Token

```bash
# Set required environment variables
export CHROMA_AUTH_TOKEN="your-bearer-token"
export CHROMA_URL="http://localhost:8000"  # optional

# Run the example
go run main.go -example=bearer
```

### X-Chroma-Token

```bash
# Set required environment variables
export CHROMA_AUTH_TOKEN="your-chroma-token"
export CHROMA_URL="http://localhost:8000"  # optional

# Run the example
go run main.go -example=x-chroma-token
```

### Custom Headers

```bash
# Set at least one of these
export AUTH_TOKEN="your-bearer-token"
export API_KEY="your-api-key"
export CHROMA_URL="http://localhost:8000"  # optional

# Run the example
go run main.go -example=custom-headers
```

### Chroma Cloud

```bash
# Set required environment variables
export CHROMA_CLOUD_API_KEY="your-api-key"
export CHROMA_CLOUD_TENANT="your-tenant"      # optional, defaults shown
export CHROMA_CLOUD_DATABASE="your-database"  # optional, defaults shown

# Run the example
go run main.go -example=cloud
```

## Environment Variables

| Variable              | Description                     | Required | Used By                            |
|-----------------------|---------------------------------|----------|------------------------------------|
| CHROMA_URL            | Chroma server URL               | No       | All examples (except cloud)        |
| CHROMA_AUTH_USERNAME  | Basic auth username             | Yes      | basic_auth.go                      |
| CHROMA_AUTH_PASSWORD  | Basic auth password             | Yes      | basic_auth.go                      |
| CHROMA_AUTH_TOKEN     | Authentication token            | Yes      | bearer_token.go, x_chroma_token.go |
| AUTH_TOKEN            | Bearer token for custom headers | One of   | custom_headers.go                  |
| API_KEY               | API key for custom headers      | One of   | custom_headers.go                  |
| CHROMA_CLOUD_API_KEY  | Chroma Cloud API key            | Yes      | chroma_cloud.go                    |
| CHROMA_CLOUD_TENANT   | Chroma Cloud tenant             | No       | chroma_cloud.go                    |
| CHROMA_CLOUD_DATABASE | Chroma Cloud database           | No       | chroma_cloud.go                    |

## Security Best Practices

1. **Never hardcode credentials** - All examples require environment variables for sensitive data
2. **Use secure storage** - Store credentials in secure vaults or secret managers in production
3. **Rotate credentials regularly** - Update tokens and passwords periodically
4. **Use HTTPS in production** - Always use encrypted connections for production deployments
5. **Validate environment** - Examples will fail with clear error messages if required variables are missing