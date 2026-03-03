# MCP Gateway — Setup Guide

The MCP Gateway (`mcp-gateway`) authenticates Claude Connector requests via OAuth 2.1 and proxies them to backend `bintrail-mcp` instances based on the authenticated tenant.

## Architecture

```
Claude (web/desktop/mobile)
         │
         │  https://mcp.dbtrail.com/mcp
         ▼
┌─────────────────────┐
│    Route 53         │  mcp.dbtrail.com → ALB
└────────┬────────────┘
         ▼
┌─────────────────────┐
│    ALB (HTTPS)      │  ACM cert, TLS termination, idle timeout 120s
└────────┬────────────┘
         ▼
┌─────────────────────┐
│   mcp-gateway       │  OAuth + CORS + Origin validation + reverse proxy
│   (ECS or EC2)      │  Stateless — all state in DynamoDB
└────────┬────────────┘
         │
   ┌─────┼──────────────┐
   ▼     ▼              ▼
 EC2-A  EC2-B     EC2-shared       (bintrail-mcp instances)
 (paid) (paid)    (free tier)
```

## AWS Infrastructure Setup

### 1. Route 53 — DNS Record

Create an A record (alias) for `mcp.dbtrail.com` pointing to the ALB:

```bash
# Using the AWS CLI:
aws route53 change-resource-record-sets \
  --hosted-zone-id <ZONE_ID> \
  --change-batch '{
    "Changes": [{
      "Action": "UPSERT",
      "ResourceRecordSet": {
        "Name": "mcp.dbtrail.com",
        "Type": "A",
        "AliasTarget": {
          "HostedZoneId": "<ALB_HOSTED_ZONE_ID>",
          "DNSName": "<ALB_DNS_NAME>",
          "EvaluateTargetHealth": true
        }
      }
    }]
  }'
```

### 2. ACM — TLS Certificate

Request a certificate for `mcp.dbtrail.com` (or use an existing `*.dbtrail.com` wildcard):

```bash
aws acm request-certificate \
  --domain-name mcp.dbtrail.com \
  --validation-method DNS \
  --region us-east-1
```

Add the CNAME validation record to Route 53 and wait for validation:

```bash
aws acm describe-certificate --certificate-arn <CERT_ARN> --query 'Certificate.DomainValidationOptions'
```

### 3. ALB — Application Load Balancer

Create an ALB with HTTPS listener. Key settings:

- **Idle timeout: 120 seconds** (SSE connections are long-lived)
- **HTTPS listener on port 443** with the ACM certificate
- **Target group** pointing to the gateway service (port 8443)
- **Health check path:** `GET /health`

```bash
# Create target group
aws elbv2 create-target-group \
  --name mcp-gateway-tg \
  --protocol HTTP \
  --port 8443 \
  --vpc-id <VPC_ID> \
  --target-type ip \
  --health-check-path /health \
  --health-check-interval-seconds 30

# Create ALB
aws elbv2 create-load-balancer \
  --name mcp-gateway-alb \
  --subnets <SUBNET_1> <SUBNET_2> \
  --security-groups <SG_ID> \
  --scheme internet-facing

# Set idle timeout to 120s for SSE streaming
aws elbv2 modify-load-balancer-attributes \
  --load-balancer-arn <ALB_ARN> \
  --attributes Key=idle_timeout.timeout_seconds,Value=120

# Create HTTPS listener
aws elbv2 create-listener \
  --load-balancer-arn <ALB_ARN> \
  --protocol HTTPS \
  --port 443 \
  --certificates CertificateArn=<CERT_ARN> \
  --default-actions Type=forward,TargetGroupArn=<TG_ARN>
```

**Security group:** Allow inbound 443 from `0.0.0.0/0` (public). Allow outbound to the gateway on port 8443.

### 4. DynamoDB Tables

Create the OAuth and tenant tables. All tables use on-demand capacity (pay-per-request):

```bash
# OAuth clients (DCR)
aws dynamodb create-table \
  --table-name bintrail-oauth-clients \
  --attribute-definitions AttributeName=client_id,AttributeType=S \
  --key-schema AttributeName=client_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# Authorization codes (short-lived, TTL-enabled)
aws dynamodb create-table \
  --table-name bintrail-oauth-codes \
  --attribute-definitions AttributeName=code,AttributeType=S \
  --key-schema AttributeName=code,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb update-time-to-live \
  --table-name bintrail-oauth-codes \
  --time-to-live-specification Enabled=true,AttributeName=expires_at

# Access tokens (TTL-enabled)
aws dynamodb create-table \
  --table-name bintrail-oauth-tokens \
  --attribute-definitions AttributeName=access_token_hash,AttributeType=S \
  --key-schema AttributeName=access_token_hash,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb update-time-to-live \
  --table-name bintrail-oauth-tokens \
  --time-to-live-specification Enabled=true,AttributeName=expires_at

# Refresh tokens (TTL-enabled)
aws dynamodb create-table \
  --table-name bintrail-oauth-refresh \
  --attribute-definitions AttributeName=refresh_token_hash,AttributeType=S \
  --key-schema AttributeName=refresh_token_hash,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb update-time-to-live \
  --table-name bintrail-oauth-refresh \
  --time-to-live-specification Enabled=true,AttributeName=expires_at

# Tenant → backend mapping
aws dynamodb create-table \
  --table-name bintrail-tenants \
  --attribute-definitions AttributeName=tenant_id,AttributeType=S \
  --key-schema AttributeName=tenant_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

Seed the tenants table with your customer data:

```bash
aws dynamodb put-item \
  --table-name bintrail-tenants \
  --item '{
    "tenant_id": {"S": "acme-corp"},
    "tier": {"S": "paid"},
    "backend_url": {"S": "http://10.0.1.5:8080/mcp"},
    "index_dsn": {"S": "user:pass@tcp(10.0.1.5:3306)/bintrail"},
    "status": {"S": "active"}
  }'
```

### 5. Deploy the Gateway

Build and deploy the gateway binary:

```bash
make build-gateway
```

Run it on ECS Fargate or EC2:

```bash
./mcp-gateway \
  --addr :8443 \
  --issuer https://mcp.dbtrail.com \
  --allowed-origins "https://claude.ai,https://claude.com" \
  --table-prefix bintrail-oauth \
  --backend-url http://localhost:8080   # fallback for tenants without a dedicated backend
```

The gateway needs AWS credentials with DynamoDB access. On ECS/EC2, use an IAM role with this policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem"
    ],
    "Resource": [
      "arn:aws:dynamodb:*:*:table/bintrail-oauth-*",
      "arn:aws:dynamodb:*:*:table/bintrail-tenants"
    ]
  }]
}
```

### 6. Deploy Backend Instances

Each backend runs `bintrail-mcp` with its own index DSN:

**Dedicated (paid tier):**
```bash
BINTRAIL_INDEX_DSN="user:pass@tcp(localhost:3306)/bintrail" \
  ./bintrail-mcp --http :8080
```

**Shared (free tier) — multi-tenant mode:**
```bash
./bintrail-mcp --http :8080 --tenant-dsns /etc/bintrail/tenant-dsns.json
```

Where `tenant-dsns.json` maps tenant IDs to DSNs:
```json
{
  "startup-x": "user:pass@tcp(localhost:3306)/bintrail_startup_x",
  "devshop": "user:pass@tcp(localhost:3306)/bintrail_devshop"
}
```

The gateway sends `X-Bintrail-Tenant: <tenant_id>` on every proxied request. The shared backend uses this to resolve the correct DSN.

## Adding as a Claude Connector

Once deployed, users add Bintrail to Claude in one step:

1. Go to **Settings > Connectors** in claude.ai
2. Click **Add custom connector**
3. Enter URL: `https://mcp.dbtrail.com/mcp`
4. Claude auto-discovers OAuth metadata, registers itself (DCR), and initiates the authorization flow
5. User enters their Bintrail tenant ID on the authorization page
6. Claude receives tokens and can now use `query`, `recover`, and `status` tools

## Testing

### Local testing with MCP Inspector

```bash
# Start a backend
BINTRAIL_INDEX_DSN="..." ./bintrail-mcp --http :8080

# Start the gateway (pointing to local backend)
./mcp-gateway --addr :8443 --backend-url http://localhost:8080 --issuer http://localhost:8443

# Test with MCP Inspector
npx @modelcontextprotocol/inspector http://localhost:8443/mcp
```

### Verify OAuth flow

```bash
# 1. Check metadata discovery
curl http://localhost:8443/.well-known/oauth-authorization-server | jq .

# 2. Register a client (DCR)
curl -X POST http://localhost:8443/oauth/register \
  -H 'Content-Type: application/json' \
  -d '{"client_name":"test","redirect_uris":["http://localhost:3000/callback"]}'

# 3. Health check
curl http://localhost:8443/health
```

### Verify CORS

```bash
curl -i -X OPTIONS http://localhost:8443/mcp \
  -H 'Origin: https://claude.ai' \
  -H 'Access-Control-Request-Method: POST'
# Should return 204 with Access-Control-Allow-Origin: https://claude.ai
```

## Deprecating proxy.py

Once the connector is deployed, `proxy.py` becomes a legacy fallback for users who prefer local JSON config or need offline access. Document both methods:

1. **Recommended:** Settings > Connectors → `https://mcp.dbtrail.com/mcp`
2. **Fallback:** Local `proxy.py` via `claude_desktop_config.json` (see `docs/mcp-server.md`)
