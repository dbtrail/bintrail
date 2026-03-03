# MCP Gateway — Connect Claude to Bintrail

The MCP Gateway lets you connect Claude (the AI) directly to your bintrail index. After a one-time setup, you can ask Claude questions like:

> "What got deleted from the orders table in the last hour?"

...and Claude will query your binlog index and answer, without you touching the CLI at all.

This works from claude.ai in a browser, Claude Desktop on your laptop, and Claude mobile. No local binaries needed on the client side.

---

## What This Does (The Big Picture)

Without the gateway, you need `bintrail-mcp` running locally alongside Claude Code. The gateway removes that requirement by putting everything behind a public URL with authentication:

```
You, in Claude (web/desktop/mobile)
    │
    │  "What happened to user 42 today?"
    ▼
Claude calls https://mcp.dbtrail.com/mcp
    │
    │  OAuth token (proves who you are)
    ▼
┌──────────────┐
│  mcp-gateway │  Checks your token, finds your tenant, picks your backend
└──────┬───────┘
       ▼
┌──────────────┐
│ bintrail-mcp │  Queries your MySQL binlog index, returns results
└──────────────┘
       ▼
Claude reads the results and explains them to you in plain English
```

The gateway handles:
- **Authentication** — OAuth 2.1 so only authorized users can access your data
- **Tenant routing** — each customer's queries go to the right backend and database
- **CORS** — so Claude's browser client (claude.ai) can talk to the gateway

---

## Two Ways to Set This Up

### Option A: Quick Local Test (5 minutes, no AWS)

This runs everything on your machine. Good for trying it out.

**Step 1 — Start a backend:**

```sh
# You need an existing bintrail index. If you don't have one, follow the quickstart first.
BINTRAIL_INDEX_DSN='root:secret@tcp(127.0.0.1:3306)/binlog_index' \
  ./bintrail-mcp --http :8080
```

**Step 2 — Start the gateway:**

```sh
./mcp-gateway \
  --addr :8443 \
  --issuer http://localhost:8443 \
  --backend-url http://localhost:8080 \
  --table-prefix bintrail-oauth
```

This will fail with a DynamoDB error because there's no AWS credentials. For local testing, you have two options:

1. **Use DynamoDB Local** (Docker):
   ```sh
   docker run -p 8000:8000 amazon/dynamodb-local
   ```
   Then set `AWS_ENDPOINT_URL=http://localhost:8000` and create the tables (see [DynamoDB Tables](#3-dynamodb-tables) below).

2. **Use a real AWS account** — just make sure `aws configure` works and create the tables in your account.

**Step 3 — Verify it works:**

```sh
# Should return JSON with authorization_endpoint, token_endpoint, etc.
curl http://localhost:8443/.well-known/oauth-authorization-server | jq .

# Should return {"status":"ok","version":"dev"}
curl http://localhost:8443/health
```

**Step 4 — Test with MCP Inspector:**

```sh
npx @modelcontextprotocol/inspector http://localhost:8443/mcp
```

The inspector will walk through the OAuth flow and let you call tools interactively.

---

### Option B: Production Deployment on AWS

This is the full setup for running the gateway as a public service.

#### What You Need

- An AWS account
- A domain name (the guide uses `mcp.dbtrail.com` — replace with yours)
- One or more `bintrail-mcp` backends already running (each with its own MySQL index)

#### Architecture

```
Internet (claude.ai, Claude Desktop, Claude mobile)
         │
         │  HTTPS
         ▼
┌─────────────────────┐
│    Route 53         │  mcp.dbtrail.com → ALB
└────────┬────────────┘
         ▼
┌─────────────────────┐
│    ALB (HTTPS)      │  TLS termination, idle timeout 120s
└────────┬────────────┘
         ▼
┌─────────────────────┐
│   mcp-gateway       │  Stateless. All state lives in DynamoDB.
│   (ECS or EC2)      │  Handles OAuth + CORS + reverse proxy.
└────────┬────────────┘
         │
   ┌─────┼──────────────┐
   ▼     ▼              ▼
 EC2-A  EC2-B     EC2-shared       ← bintrail-mcp instances
 (paid) (paid)    (free tier)         each with its own MySQL index
```

The gateway is stateless — you can run multiple copies behind the ALB. All OAuth state (clients, tokens, etc.) lives in DynamoDB.

---

### 1. DNS (Route 53)

Point your domain at the ALB. Create an A record (alias type) for `mcp.dbtrail.com`:

```sh
aws route53 change-resource-record-sets \
  --hosted-zone-id <YOUR_ZONE_ID> \
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

**How to find these values:**
- `YOUR_ZONE_ID` — go to Route 53 > Hosted zones > click your domain > the Zone ID is at the top
- `ALB_HOSTED_ZONE_ID` and `ALB_DNS_NAME` — you'll get these after creating the ALB in step 3

If you use the AWS Console instead: Route 53 > Hosted zones > your domain > Create record > toggle "Alias" on > select your ALB from the dropdown.

### 2. TLS Certificate (ACM)

You need an HTTPS certificate. AWS gives you one for free:

```sh
aws acm request-certificate \
  --domain-name mcp.dbtrail.com \
  --validation-method DNS \
  --region us-east-1
```

This prints a certificate ARN. Now validate it — ACM gives you a CNAME record to add to DNS:

```sh
# See what CNAME record ACM wants you to add:
aws acm describe-certificate \
  --certificate-arn <CERT_ARN> \
  --query 'Certificate.DomainValidationOptions'
```

Add that CNAME record to Route 53. Wait a few minutes. The certificate status will change from `PENDING_VALIDATION` to `ISSUED`.

**Already have a `*.dbtrail.com` wildcard cert?** Skip this step and use that ARN.

### 3. Load Balancer (ALB)

The ALB terminates TLS and forwards traffic to the gateway. Three commands:

```sh
# 1. Create a target group (where the ALB sends traffic)
aws elbv2 create-target-group \
  --name mcp-gateway-tg \
  --protocol HTTP \
  --port 8443 \
  --vpc-id <YOUR_VPC_ID> \
  --target-type ip \
  --health-check-path /health \
  --health-check-interval-seconds 30

# 2. Create the ALB itself
aws elbv2 create-load-balancer \
  --name mcp-gateway-alb \
  --subnets <SUBNET_1> <SUBNET_2> \
  --security-groups <SG_ID> \
  --scheme internet-facing

# 3. IMPORTANT: Set idle timeout to 120 seconds.
#    Default is 60s, which is too short for SSE streaming connections.
aws elbv2 modify-load-balancer-attributes \
  --load-balancer-arn <ALB_ARN> \
  --attributes Key=idle_timeout.timeout_seconds,Value=120

# 4. Create the HTTPS listener (ties the ALB to your certificate)
aws elbv2 create-listener \
  --load-balancer-arn <ALB_ARN> \
  --protocol HTTPS \
  --port 443 \
  --certificates CertificateArn=<CERT_ARN> \
  --default-actions Type=forward,TargetGroupArn=<TG_ARN>
```

**Security group rules:**
- Inbound: allow port 443 from `0.0.0.0/0` (the internet)
- Outbound: allow traffic to the gateway on port 8443

**How to find VPC and subnet IDs:** `aws ec2 describe-vpcs` and `aws ec2 describe-subnets --filters "Name=vpc-id,Values=<VPC_ID>"`. Pick two subnets in different availability zones.

### 4. DynamoDB Tables

The gateway stores OAuth clients, tokens, and tenant configs in DynamoDB. Create 6 tables — all using on-demand billing (you pay per request, no capacity planning):

```sh
# OAuth clients (created when Claude registers itself)
aws dynamodb create-table \
  --table-name bintrail-oauth-clients \
  --attribute-definitions AttributeName=client_id,AttributeType=S \
  --key-schema AttributeName=client_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

# Authorization codes (short-lived, auto-deleted by TTL)
aws dynamodb create-table \
  --table-name bintrail-oauth-codes \
  --attribute-definitions AttributeName=code,AttributeType=S \
  --key-schema AttributeName=code,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb update-time-to-live \
  --table-name bintrail-oauth-codes \
  --time-to-live-specification Enabled=true,AttributeName=expires_at

# Access tokens (auto-deleted by TTL)
aws dynamodb create-table \
  --table-name bintrail-oauth-tokens \
  --attribute-definitions AttributeName=access_token_hash,AttributeType=S \
  --key-schema AttributeName=access_token_hash,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb update-time-to-live \
  --table-name bintrail-oauth-tokens \
  --time-to-live-specification Enabled=true,AttributeName=expires_at

# Refresh tokens (auto-deleted by TTL)
aws dynamodb create-table \
  --table-name bintrail-oauth-refresh \
  --attribute-definitions AttributeName=refresh_token_hash,AttributeType=S \
  --key-schema AttributeName=refresh_token_hash,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb update-time-to-live \
  --table-name bintrail-oauth-refresh \
  --time-to-live-specification Enabled=true,AttributeName=expires_at

# Tenants — maps each customer to their backend
aws dynamodb create-table \
  --table-name bintrail-oauth-tenants \
  --attribute-definitions AttributeName=tenant_id,AttributeType=S \
  --key-schema AttributeName=tenant_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

**Why TTL?** Authorization codes expire in 10 minutes, access tokens in 1 hour, refresh tokens in 30 days. DynamoDB TTL automatically deletes expired rows so the tables don't grow forever. You don't need to run any cleanup jobs.

**Why are tokens hashed?** The gateway stores `SHA-256(token)` instead of the raw token. If someone reads your DynamoDB table, they can't use the hashes to impersonate users.

### 5. Add Your Tenants

Each customer who will use the gateway needs a row in the tenants table. A tenant needs:

- `tenant_id` — whatever identifier the customer knows (e.g. their company name)
- `backend_url` — where their `bintrail-mcp` instance is running
- `status` — must be `active` for the tenant to work

```sh
# Paid customer with a dedicated backend:
aws dynamodb put-item \
  --table-name bintrail-oauth-tenants \
  --item '{
    "tenant_id": {"S": "acme-corp"},
    "tier": {"S": "paid"},
    "backend_url": {"S": "http://10.0.1.5:8080/mcp"},
    "index_dsn": {"S": "user:pass@tcp(10.0.1.5:3306)/bintrail"},
    "status": {"S": "active"}
  }'

# Free-tier customer on the shared backend (no backend_url — uses the default):
aws dynamodb put-item \
  --table-name bintrail-oauth-tenants \
  --item '{
    "tenant_id": {"S": "startup-x"},
    "tier": {"S": "free"},
    "status": {"S": "active"}
  }'
```

Tenants without a `backend_url` use the gateway's `--backend-url` default. This is how you put multiple free-tier customers on one shared `bintrail-mcp` instance.

### 6. Start the Backend(s)

Each backend runs `bintrail-mcp` in HTTP mode.

**Dedicated backend (one customer per instance):**

```sh
BINTRAIL_INDEX_DSN="user:pass@tcp(localhost:3306)/bintrail" \
  ./bintrail-mcp --http :8080
```

**Shared backend (multiple free-tier customers on one instance):**

```sh
./bintrail-mcp --http :8080 --tenant-dsns /etc/bintrail/tenant-dsns.json
```

Where `tenant-dsns.json` is a simple JSON object mapping tenant IDs to MySQL DSNs:

```json
{
  "startup-x": "user:pass@tcp(localhost:3306)/bintrail_startup_x",
  "devshop": "user:pass@tcp(localhost:3306)/bintrail_devshop"
}
```

The gateway sends an `X-Bintrail-Tenant` header on every request. The shared backend reads this header to pick the right database.

### 7. Start the Gateway

Build it:

```sh
make build-gateway
```

Run it:

```sh
./mcp-gateway \
  --addr :8443 \
  --issuer https://mcp.dbtrail.com \
  --allowed-origins "https://claude.ai,https://claude.com" \
  --table-prefix bintrail-oauth \
  --backend-url http://10.0.1.100:8080
```

| Flag | What it does | Example |
|---|---|---|
| `--addr` | Port the gateway listens on | `:8443` |
| `--issuer` | Your public URL (used in OAuth metadata) | `https://mcp.dbtrail.com` |
| `--allowed-origins` | Which browser origins can call the gateway | `https://claude.ai,https://claude.com` |
| `--table-prefix` | DynamoDB table name prefix | `bintrail-oauth` (tables will be `bintrail-oauth-clients`, `bintrail-oauth-tokens`, etc.) |
| `--backend-url` | Default backend for tenants without their own | `http://10.0.1.100:8080` |

**IAM permissions** — the gateway needs to read/write DynamoDB. Attach this policy to the ECS task role or EC2 instance role:

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
      "arn:aws:dynamodb:*:*:table/bintrail-oauth-*"
    ]
  }]
}
```

### 8. Verify Everything Works

```sh
# 1. OAuth metadata discovery (should return JSON with all the endpoints)
curl https://mcp.dbtrail.com/.well-known/oauth-authorization-server | jq .

# 2. Health check
curl https://mcp.dbtrail.com/health
# → {"status":"ok","version":"v1.2.3"}

# 3. Register a test client
curl -X POST https://mcp.dbtrail.com/oauth/register \
  -H 'Content-Type: application/json' \
  -d '{"client_name":"test","redirect_uris":["http://localhost:3000/callback"]}'
# → {"client_id":"...","client_secret":"...","client_name":"test",...}

# 4. CORS preflight (should return 204 with CORS headers)
curl -i -X OPTIONS https://mcp.dbtrail.com/mcp \
  -H 'Origin: https://claude.ai' \
  -H 'Access-Control-Request-Method: POST'
# → Access-Control-Allow-Origin: https://claude.ai
```

If all four work, you're good.

---

## Adding Bintrail to Claude

This is the easy part. Once the gateway is running:

1. Go to **claude.ai** > **Settings** > **Connectors**
2. Click **Add custom connector**
3. Enter the URL: `https://mcp.dbtrail.com/mcp`
4. Claude auto-discovers the OAuth endpoints, registers itself, and opens a login page
5. Enter your **tenant ID** (e.g. `acme-corp`) on the authorization page
6. Done. Claude now has access to `query`, `recover`, and `status` tools.

From now on, you can ask Claude things like:
- "What changed in the users table in the last 24 hours?"
- "Show me all DELETEs on the orders table since Monday"
- "Generate SQL to undo the last 5 updates to order 12345"

---

## How the OAuth Flow Works

You don't need to understand this to use the gateway, but here's what happens under the hood when Claude connects:

```
1. Claude discovers endpoints
   GET /.well-known/oauth-authorization-server
   ← {authorization_endpoint, token_endpoint, registration_endpoint, ...}

2. Claude registers itself (one-time, per device)
   POST /oauth/register
   → {client_name: "Claude", redirect_uris: ["https://claude.ai/..."]}
   ← {client_id: "abc", client_secret: "xyz"}

3. Claude opens the authorization page in your browser
   GET /oauth/authorize?client_id=abc&redirect_uri=...&code_challenge=...&code_challenge_method=S256
   ← HTML page: "Enter your Bintrail tenant ID"

4. You submit your tenant ID
   POST /oauth/authorize
   → {tenant_id: "acme-corp", ...}
   ← 302 redirect to Claude with an authorization code

5. Claude exchanges the code for tokens
   POST /oauth/token
   → {grant_type: "authorization_code", code: "...", code_verifier: "...", client_secret: "..."}
   ← {access_token: "...", refresh_token: "...", expires_in: 3600}

6. Claude calls tools using the access token
   POST /mcp (Authorization: Bearer <access_token>)
   → gateway validates token → resolves tenant → proxies to backend → returns results

7. When the access token expires (1 hour), Claude refreshes it
   POST /oauth/token
   → {grant_type: "refresh_token", refresh_token: "...", client_secret: "..."}
   ← {access_token: "new-token", refresh_token: "new-refresh-token", ...}
```

PKCE (S256) prevents authorization code interception. Refresh tokens are single-use (rotated on every refresh). Access tokens are stored as SHA-256 hashes in DynamoDB — even if someone reads the database, they can't forge tokens.

---

## Troubleshooting

### "Gateway returns 502 Bad Gateway"

The gateway can't reach the backend. Check:
- Is `bintrail-mcp --http :8080` actually running on the backend machine?
- Can the gateway machine reach it? `curl http://<backend-ip>:8080/mcp` from the gateway
- Does the tenant row in DynamoDB have the correct `backend_url`?

### "OAuth flow shows 'unknown or inactive tenant'"

The tenant ID you entered doesn't exist in the tenants table, or its `status` is not `active`. Check:
```sh
aws dynamodb get-item \
  --table-name bintrail-oauth-tenants \
  --key '{"tenant_id":{"S":"your-tenant-id"}}'
```

### "CORS error in Claude web"

The `--allowed-origins` flag doesn't include the origin Claude is calling from. Make sure you have both:
```
--allowed-origins "https://claude.ai,https://claude.com"
```

### "Tokens stop working after gateway restart"

Access tokens survive a gateway restart (they're in DynamoDB, not memory). But if you see auth failures after a restart, it might be a clock skew issue — check that the gateway machine's clock is accurate (`ntpdate` or `chronyc`).

### "I added a tenant but queries return errors"

The `index_dsn` field in the tenants table is informational — the gateway doesn't use it directly. What matters is:
- **Dedicated backends:** The `backend_url` points to a `bintrail-mcp` instance that has `BINTRAIL_INDEX_DSN` set to the correct DSN.
- **Shared backends:** The `tenant-dsns.json` file on the shared backend has an entry for this tenant.

---

## Still Using proxy.py?

The gateway replaces the need for `proxy.py`. But if you prefer running everything locally (no cloud, no OAuth), `proxy.py` still works:

| | Gateway (recommended) | proxy.py (fallback) |
|---|---|---|
| Setup | Add URL in Claude Settings > Connectors | Edit `claude_desktop_config.json` |
| Works from | claude.ai, Claude Desktop, Claude mobile | Claude Desktop only |
| Auth | OAuth 2.1 (automatic) | None (trusts local user) |
| Requires | AWS infrastructure | Python 3.7+ on your machine |

For proxy.py setup, see [MCP Server docs](./mcp-server.md#proxypy-bridging-claude-desktop-to-remote-http).
