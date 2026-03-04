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

### 1. TLS Certificate (ACM)

You need an HTTPS certificate for the ALB. AWS gives you one for free:

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

Add that CNAME record to Route 53 (or your DNS provider). Wait a few minutes. The certificate status will change from `PENDING_VALIDATION` to `ISSUED`.

**Already have a `*.dbtrail.com` wildcard cert?** Skip this step and use that ARN.

### 2. Load Balancer (ALB)

The ALB terminates TLS and forwards traffic to the gateway. You can set it up via the AWS Console or CLI.

#### Step 1: Find your VPC and subnets

You need your VPC ID and at least two public subnets in different availability zones. The ALB requires subnets in multiple AZs for high availability.

```sh
# Find your VPC ID
aws ec2 describe-vpcs --query 'Vpcs[*].{ID:VpcId,CIDR:CidrBlock,Default:IsDefault}' --output table

# List public subnets in your VPC (pick two in different AZs)
aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=<YOUR_VPC_ID>" \
  --query 'Subnets[*].{ID:SubnetId,AZ:AvailabilityZone,CIDR:CidrBlock,Public:MapPublicIpOnLaunch}' \
  --output table
```

Pick two subnets where `Public` is `True` and the `AZ` values differ (e.g. `us-east-1a` and `us-east-1b`). Write down both subnet IDs — you'll need them in Step 4.

#### Step 2: Create a security group for the ALB

The ALB needs its own security group that allows inbound HTTPS (443) from the internet and outbound traffic to the gateway on port 8443.

```sh
# Create the security group
aws ec2 create-security-group \
  --group-name mcp-gateway-alb-sg \
  --description "ALB for MCP Gateway - allows inbound HTTPS" \
  --vpc-id <YOUR_VPC_ID>
# → returns a GroupId like sg-0abc1234def56789

# Allow inbound HTTPS (port 443) from anywhere
aws ec2 authorize-security-group-ingress \
  --group-id <SG_ID> \
  --protocol tcp \
  --port 443 \
  --cidr 0.0.0.0/0

# Allow outbound traffic to the gateway on port 8443
# (the default outbound rule allows all traffic — if you've restricted it, add this)
aws ec2 authorize-security-group-egress \
  --group-id <SG_ID> \
  --protocol tcp \
  --port 8443 \
  --cidr 0.0.0.0/0
```

**Also update the gateway's security group**: the EC2 instance (or ECS task) running `mcp-gateway` must allow inbound traffic on port 8443 from the ALB security group:

```sh
aws ec2 authorize-security-group-ingress \
  --group-id <GATEWAY_INSTANCE_SG_ID> \
  --protocol tcp \
  --port 8443 \
  --source-group <ALB_SG_ID>
```

#### Step 3: Create a target group

The target group tells the ALB where to send traffic and how to health-check the gateway.

```sh
aws elbv2 create-target-group \
  --name mcp-gateway-tg \
  --protocol HTTP \
  --port 8443 \
  --vpc-id <YOUR_VPC_ID> \
  --target-type ip \
  --health-check-path /health \
  --health-check-interval-seconds 30 \
  --healthy-threshold-count 2 \
  --unhealthy-threshold-count 3
# → returns a TargetGroupArn — save this
```

| Setting | Value | Why |
|---|---|---|
| Protocol | HTTP | ALB terminates TLS; traffic between ALB and gateway is plaintext within the VPC |
| Port | 8443 | The gateway's default listen port (`--addr :8443`) |
| Target type | `ip` | Works for both EC2 and ECS/Fargate. Use `instance` if you prefer instance-ID-based targets |
| Health check | `/health` | The gateway returns `{"status":"ok"}` on this path |

Now register the gateway instance(s) as targets:

```sh
# For an EC2 instance — use the private IP (not the public IP)
aws elbv2 register-targets \
  --target-group-arn <TG_ARN> \
  --targets Id=<GATEWAY_PRIVATE_IP>,Port=8443

# To find the private IP of your EC2 instance:
aws ec2 describe-instances \
  --instance-ids <INSTANCE_ID> \
  --query 'Reservations[0].Instances[0].PrivateIpAddress' \
  --output text
```

#### Step 4: Create the ALB

```sh
aws elbv2 create-load-balancer \
  --name mcp-gateway-alb \
  --subnets <SUBNET_1> <SUBNET_2> \
  --security-groups <ALB_SG_ID> \
  --scheme internet-facing \
  --type application
# → returns LoadBalancerArn and DNSName — save both
```

The `DNSName` (e.g. `mcp-gateway-alb-123456789.us-east-1.elb.amazonaws.com`) is what you'll point your domain at in the DNS step. The `LoadBalancerArn` is needed for the next commands.

#### Step 5: Set the idle timeout to 120 seconds

This is **critical** for MCP. The MCP protocol uses SSE (Server-Sent Events) streaming, and tool calls can take several seconds. The default ALB timeout is 60 seconds, which cuts off long-running requests.

```sh
aws elbv2 modify-load-balancer-attributes \
  --load-balancer-arn <ALB_ARN> \
  --attributes Key=idle_timeout.timeout_seconds,Value=120
```

#### Step 6: Create the HTTPS listener

The listener ties the ALB to your TLS certificate (from Step 1) and routes traffic to the target group.

```sh
aws elbv2 create-listener \
  --load-balancer-arn <ALB_ARN> \
  --protocol HTTPS \
  --port 443 \
  --certificates CertificateArn=<CERT_ARN> \
  --default-actions Type=forward,TargetGroupArn=<TG_ARN> \
  --ssl-policy ELBSecurityPolicy-TLS13-1-2-2021-06
```

The `--ssl-policy` enables TLS 1.3 and 1.2. This is the recommended policy for new ALBs — it disables older protocols like TLS 1.0 and 1.1.

Optionally, add an HTTP listener that redirects to HTTPS (so `http://mcp.dbtrail.com` → `https://mcp.dbtrail.com`):

```sh
aws elbv2 create-listener \
  --load-balancer-arn <ALB_ARN> \
  --protocol HTTP \
  --port 80 \
  --default-actions 'Type=redirect,RedirectConfig={Protocol=HTTPS,Port=443,StatusCode=HTTP_301}'
```

If you add the HTTP redirect, also open port 80 on the ALB security group:

```sh
aws ec2 authorize-security-group-ingress \
  --group-id <ALB_SG_ID> \
  --protocol tcp \
  --port 80 \
  --cidr 0.0.0.0/0
```

#### Step 7: Verify the ALB is working

```sh
# Check ALB state (should say "active" after a minute or two)
aws elbv2 describe-load-balancers \
  --names mcp-gateway-alb \
  --query 'LoadBalancers[0].{State:State.Code,DNS:DNSName}' \
  --output table

# Check target health (should say "healthy" once the gateway is running)
aws elbv2 describe-target-health \
  --target-group-arn <TG_ARN>

# Hit the health endpoint through the ALB (use the ALB DNS name)
curl https://<ALB_DNS_NAME>/health
```

If `describe-target-health` shows `unhealthy`, check:
- The gateway is running (`./mcp-gateway --addr :8443 ...`)
- The gateway's security group allows inbound 8443 from the ALB's security group
- The gateway responds to health checks: `curl http://<GATEWAY_PRIVATE_IP>:8443/health`

#### Doing this from the AWS Console instead

If you prefer clicking over typing:

1. **EC2 > Security Groups > Create security group** — name it `mcp-gateway-alb-sg`, add inbound rule for HTTPS (443) from `0.0.0.0/0`
2. **EC2 > Target Groups > Create target group** — choose "IP addresses", protocol HTTP, port 8443, health check path `/health`, select your VPC, then register the gateway's private IP
3. **EC2 > Load Balancers > Create Load Balancer** — choose "Application Load Balancer", internet-facing, select two public subnets in different AZs, attach the security group from step 1
4. **Add listener** — HTTPS on port 443, select your ACM certificate, forward to the target group from step 2
5. **Load balancer attributes** — edit and set "Idle timeout" to `120` seconds
6. **Done** — copy the DNS name from the load balancer details page for the Route 53 step

### 3. DNS (Route 53)

Now that the ALB is created, point your domain at it. You need two values from the ALB:

```sh
# Get the ALB's DNS name and hosted zone ID
aws elbv2 describe-load-balancers \
  --names mcp-gateway-alb \
  --query 'LoadBalancers[0].{DNSName:DNSName,HostedZoneId:CanonicalHostedZoneId}' \
  --output table
```

Create an A record (alias type) for `mcp.dbtrail.com`:

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

**How to find `YOUR_ZONE_ID`:** Route 53 > Hosted zones > click your domain > the Zone ID is at the top.

If you use the AWS Console instead: Route 53 > Hosted zones > your domain > Create record > toggle "Alias" on > select your ALB from the dropdown.

Verify DNS is working:

```sh
# Should resolve to the ALB's IP addresses (may take a few minutes to propagate)
dig mcp.dbtrail.com

# Should return the gateway health response
curl https://mcp.dbtrail.com/health
```

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

You have two ways to add tenants: the **Admin API** (recommended) or **raw DynamoDB** commands.

#### Option 1: Admin API (recommended)

The gateway has a built-in REST API for managing tenants. You need to start the gateway with `--admin-token` to enable it:

```sh
./mcp-gateway \
  --addr :8443 \
  --admin-token my-secret-admin-token \
  ...other flags...
```

Then use curl to manage tenants:

```sh
# Create a paid tenant with a dedicated backend:
curl -X POST http://localhost:8443/admin/tenants \
  -H 'Authorization: Bearer my-secret-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "acme-corp",
    "tier": "paid",
    "backend_url": "http://10.0.1.5:8080/mcp",
    "index_dsn": "user:pass@tcp(10.0.1.5:3306)/bintrail",
    "status": "active"
  }'
# → 201 Created + JSON body with the tenant

# Create a free-tier tenant on the shared backend (no backend_url):
curl -X POST http://localhost:8443/admin/tenants \
  -H 'Authorization: Bearer my-secret-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "startup-x",
    "tier": "free"
  }'
# → 201 Created (status defaults to "active" if omitted)

# List all tenants:
curl http://localhost:8443/admin/tenants \
  -H 'Authorization: Bearer my-secret-admin-token'
# → JSON array of all tenants

# Get one tenant:
curl http://localhost:8443/admin/tenants/acme-corp \
  -H 'Authorization: Bearer my-secret-admin-token'
# → JSON object for acme-corp

# Update a tenant (e.g. upgrade to paid, change backend):
curl -X PUT http://localhost:8443/admin/tenants/startup-x \
  -H 'Authorization: Bearer my-secret-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "tier": "paid",
    "backend_url": "http://10.0.1.10:8080/mcp",
    "status": "active"
  }'
# → 200 OK

# Suspend a tenant (they'll get 403 Forbidden on all requests):
curl -X PUT http://localhost:8443/admin/tenants/bad-actor \
  -H 'Authorization: Bearer my-secret-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"status": "suspended"}'

# Delete a tenant permanently:
curl -X DELETE http://localhost:8443/admin/tenants/old-customer \
  -H 'Authorization: Bearer my-secret-admin-token'
# → 204 No Content
```

**Important:** The `--admin-token` flag is required to enable admin endpoints. Without it, `/admin/*` routes return 404. Use a long, random string in production.

#### Option 2: Raw DynamoDB (no admin API needed)

If you prefer managing tenants directly in DynamoDB:

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

| Flag | What it does | Default | Example |
|---|---|---|---|
| `--addr` | Port the gateway listens on | `:8443` | `:8443` |
| `--issuer` | Your public URL (used in OAuth metadata) | `https://mcp.dbtrail.com` | `https://mcp.dbtrail.com` |
| `--allowed-origins` | Which browser origins can call the gateway | `https://claude.ai,https://claude.com` | `https://claude.ai,https://claude.com` |
| `--table-prefix` | DynamoDB table name prefix | `bintrail-oauth` | `bintrail-oauth` |
| `--backend-url` | Default backend for tenants without their own | (none) | `http://10.0.1.100:8080` |
| `--admin-token` | Bearer token for admin API (`/admin/*` endpoints) | (disabled) | `my-secret-token` |
| `--health-interval` | How often to check if backends are alive | `30s` | `30s`, `1m`, `0` (disable) |
| `--health-threshold` | Consecutive failures before marking a backend down | `3` | `3` |

**IAM permissions** — the gateway needs to read/write DynamoDB. Attach this policy to the ECS task role or EC2 instance role:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem",
      "dynamodb:Scan"
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

# 2. Health check (now includes backend health status)
curl https://mcp.dbtrail.com/health | jq .
# → {"status":"ok","version":"v1.2.3","backends":{"http://10.0.1.100:8080":true}}

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

## How Tenant Routing Works (The Full Picture)

This section explains exactly what happens when a request arrives, from start to finish. You don't need to understand this to use the gateway, but it helps when debugging.

### The Request Journey

```
1. Claude sends:  POST /mcp  (Authorization: Bearer <token>)
                       │
2. OriginMiddleware:   Is the Origin header from an allowed site?
                       │  (claude.ai ✓, evil-site.com ✗, no Origin ✓)
                       │
3. CORSMiddleware:     Add Access-Control-Allow-Origin headers for browsers.
                       │
4. AuthMiddleware:     Validate the Bearer token.
                       │
                       ├── SHA-256(token) → look up in DynamoDB tokens table
                       ├── Token expired? → 401 Unauthorized
                       ├── Token valid → get tenant_id from the token record
                       ├── Look up tenant in DynamoDB tenants table
                       └── Tenant active? → inject into request context
                                           Tenant suspended? → 403 Forbidden
                       │
5. ReverseProxy:       Route to the right backend.
                       │
                       ├── Does the tenant have a backend_url? Use it.
                       │   (e.g. paid tenant → http://10.0.1.5:8080/mcp)
                       │
                       ├── No backend_url? Use the --backend-url default.
                       │   (e.g. free tenant → http://10.0.1.100:8080/mcp)
                       │
                       ├── Is the backend healthy? (health checker says yes/no)
                       │   Unhealthy → 502 "backend is currently unhealthy"
                       │
                       ├── Add X-Bintrail-Tenant: <tenant_id> header
                       │   (so shared backends know which database to query)
                       │
                       ├── Strip the Authorization header
                       │   (backend doesn't need the OAuth token)
                       │
                       └── Forward the request → return the response
```

### Paid vs Free Tier Routing

**Paid tenants** get their own dedicated `bintrail-mcp` instance:
- Their tenant row has `backend_url` set to their instance's address
- The gateway routes directly to that address
- No `X-Bintrail-Tenant` header needed (though it's still sent)
- Complete isolation — their backend only serves their data

**Free tenants** share a single `bintrail-mcp` instance:
- Their tenant row has no `backend_url` (empty)
- The gateway uses the `--backend-url` default
- The `X-Bintrail-Tenant` header tells the shared backend which tenant this is
- The shared backend looks up the tenant's DSN from its `--tenant-dsns` JSON file
- Each free tenant gets a separate MySQL database — data is isolated even though the backend is shared

```
                     Paid tenants               Free tenants
                     ───────────               ────────────
Token → tenant_id    "acme-corp"               "startup-x"       "devshop"
                         │                          │                 │
DynamoDB lookup      backend_url:               backend_url:      backend_url:
                     10.0.1.5:8080              (empty)           (empty)
                         │                          │                 │
Route to             ┌───┘                     ┌────┘─────────────────┘
                     ▼                         ▼
               ┌──────────────┐          ┌──────────────────────┐
               │ bintrail-mcp │          │ bintrail-mcp (shared)│
               │ (dedicated)  │          │ --tenant-dsns file:  │
               │              │          │  startup-x → db1     │
               │ DSN: acme's  │          │  devshop   → db2     │
               │   database   │          │                      │
               └──────────────┘          └──────────────────────┘
```

---

## Backend Health Monitoring

The gateway continuously checks whether backends are alive by pinging their `/health` endpoint. If a backend stops responding, the gateway returns 502 immediately instead of waiting for the connection to time out.

### How It Works

1. Every `--health-interval` (default: 30 seconds), the gateway sends `GET /health` to every known backend
2. If a backend fails `--health-threshold` (default: 3) consecutive checks, it's marked **unhealthy**
3. Requests to unhealthy backends get an immediate **502 Bad Gateway** with the message "backend is currently unhealthy"
4. As soon as a health check passes again, the backend is marked **healthy** and starts receiving traffic

### What Gets Checked

- The `--backend-url` default backend (if set)
- Any backend that a tenant routes to (registered on first request)
- Health endpoint: the backend's URL with the path replaced by `/health` (e.g. `http://host:8080/mcp` → `http://host:8080/health`)

### Disabling Health Checks

Set `--health-interval 0` to disable. All backends are assumed healthy and the proxy always tries to connect (the old behavior).

### Checking Backend Status

The gateway's own `/health` endpoint includes backend health:

```sh
curl http://localhost:8443/health | jq .
```

```json
{
  "status": "ok",
  "version": "dev",
  "backends": {
    "http://10.0.1.5:8080/mcp": true,
    "http://10.0.1.100:8080/mcp": false
  }
}
```

`true` = healthy, `false` = unhealthy. Use this in your monitoring/alerting.

---

## Troubleshooting

### "Gateway returns 502 Bad Gateway"

The gateway can't reach the backend. Check:
- Is `bintrail-mcp --http :8080` actually running on the backend machine?
- Can the gateway machine reach it? `curl http://<backend-ip>:8080/health` from the gateway
- Does the tenant row in DynamoDB have the correct `backend_url`?
- Check the gateway's own health endpoint for backend status: `curl http://localhost:8443/health | jq .backends`

### "502 with 'backend is currently unhealthy'"

The health checker has marked this backend as down after consecutive failed health checks. This means:
- The backend was unreachable for at least `--health-threshold` (default 3) consecutive checks
- Fix the backend, and it will automatically recover on the next successful health check
- For a quick check: `curl http://<backend-ip>:8080/health` — if this fails, the backend is genuinely down

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
