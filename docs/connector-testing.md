# Claude Connector — Integration Testing & Hardening Checklist

This is a step-by-step guide for manually testing the Bintrail Claude Connector before GA. Follow each section in order. Every step has exact commands or click paths — no assumed knowledge.

---

## Prerequisites

Before you start, you need:

- [ ] The MCP gateway running at `https://mcp.dbtrail.com` (or your domain)
- [ ] At least one `bintrail-mcp` backend running with indexed data
- [ ] Two test tenants provisioned (e.g. `tenant-a` and `tenant-b`) — see [gateway docs](./mcp-gateway.md#5-add-your-tenants)
- [ ] A Claude Pro/Team account (for claude.ai and mobile testing)
- [ ] Claude Desktop installed (macOS or Windows)
- [ ] A phone with Claude mobile (iOS or Android)
- [ ] Node.js installed (for MCP Inspector)

---

## 1. Claude.ai (Web Browser)

### 1.1 Add the Connector

1. Open **https://claude.ai** in your browser
2. Click your profile icon (bottom-left) → **Settings**
3. Click **Integrations** in the left sidebar (may also be called **Connectors** depending on your version)
4. Click **Add custom integration** (or **Add custom connector**)
5. In the URL field, enter: `https://mcp.dbtrail.com/mcp`
6. Click **Add** (or **Save**)

**What should happen:** Claude discovers the OAuth endpoints automatically (via `/.well-known/oauth-authorization-server`), opens a browser tab to the Bintrail authorization page.

**If it fails:**
- Check that `https://mcp.dbtrail.com/.well-known/oauth-authorization-server` returns JSON in your browser
- Check the gateway logs for errors

### 1.2 Complete the OAuth Flow

1. On the Bintrail authorization page, enter your **tenant ID** (e.g. `tenant-a`)
2. Click **Authorize**

**What should happen:** The page redirects back to Claude. The connector shows as "Connected" in Settings.

**If it fails:**
- "unknown or inactive tenant" → your tenant ID doesn't exist or its status isn't `active`. Check:
  ```sh
  curl https://mcp.dbtrail.com/admin/tenants/tenant-a \
    -H 'Authorization: Bearer YOUR_ADMIN_TOKEN'
  ```
- "redirect_uri not registered" → this is a bug in the gateway's DCR flow. Check gateway logs.

### 1.3 Test the `query` Tool

1. Open a new conversation in Claude
2. Type: **"What tables are indexed in my binlog index?"**
3. Claude should call the `status` tool and list your tables

4. Then try: **"Show me all DELETEs from the last 24 hours"**
5. Claude should call the `query` tool with `event_type: DELETE` and `since` set to 24 hours ago

**What to verify:**
- [ ] Claude shows the tool call (you can expand it to see parameters)
- [ ] Results come back with actual data from your index
- [ ] No "backend unavailable" or "token expired" errors

### 1.4 Test the `recover` Tool

1. In the same conversation, type: **"Generate SQL to undo those deletes"**
2. Claude should call the `recover` tool with matching filters

**What to verify:**
- [ ] Claude returns a SQL script with `BEGIN`/`COMMIT` wrapper
- [ ] The SQL contains `INSERT INTO` statements (reversing the DELETEs)

### 1.5 Test the `status` Tool

1. Type: **"Show me the status of my binlog index"**

**What to verify:**
- [ ] Shows indexed files, partitions, and event counts
- [ ] Numbers match what you'd see from `bintrail status` on the CLI

### 1.6 Test Token Refresh (Long Session)

This tests that sessions survive beyond the 1-hour access token TTL.

1. Open a conversation and verify tools work (make any query)
2. Note the time
3. **Wait 65 minutes** (or at least until the access token expires — TTL is 1 hour)
4. In the same conversation, make another query: **"Show me the latest events"**

**What should happen:** Claude silently refreshes the access token using the refresh token. The query works without re-authenticating.

**If it fails:**
- Claude shows an error about invalid/expired token → the refresh flow isn't working
- Check gateway logs for `consume refresh token` errors
- Verify refresh tokens are stored in DynamoDB: `aws dynamodb scan --table-name bintrail-oauth-refresh`

### 1.7 Test Data Isolation (Two Tenants)

This verifies that tenant A cannot see tenant B's data.

1. **In browser 1** (or incognito): Connect with `tenant-a`
2. **In browser 2** (or different incognito): Connect with `tenant-b`
3. In browser 1, query a table that exists in tenant-a's index
4. In browser 2, query the same table name

**What to verify:**
- [ ] Each browser sees only its own tenant's data
- [ ] tenant-a cannot see tenant-b's rows and vice versa

---

## 2. Claude Desktop

### 2.1 Add the Connector

1. Open **Claude Desktop**
2. Click your profile icon → **Settings**
3. Go to **Integrations** (or **Connectors**)
4. Click **Add custom integration**
5. Enter URL: `https://mcp.dbtrail.com/mcp`
6. Complete the OAuth flow (same as web — enter your tenant ID)

> **Important:** Add the connector through the UI, NOT by editing `claude_desktop_config.json`. The JSON config method is the old `proxy.py` approach. The connector UI handles OAuth automatically.

### 2.2 Verify Tools Work

1. Open a new conversation
2. Ask: **"What's in my binlog index?"**

**What to verify:**
- [ ] Tools are listed and callable
- [ ] Results come back correctly

### 2.3 Verify SSE Streaming

1. Ask a query that returns many rows: **"Show me the last 50 events in table format"**

**What to verify:**
- [ ] Response appears incrementally (not all at once after a long wait)
- [ ] If the response takes more than a few seconds, you should see partial text appearing
- [ ] No timeout errors

**If streaming appears buffered (all at once):**
- This might be an ALB idle timeout issue. Check that the ALB has `idle_timeout.timeout_seconds=120`:
  ```sh
  aws elbv2 describe-load-balancer-attributes --load-balancer-arn YOUR_ALB_ARN \
    | jq '.Attributes[] | select(.Key == "idle_timeout.timeout_seconds")'
  ```

---

## 3. Claude Mobile (iOS/Android)

### 3.1 Sync from Web

Connectors added on claude.ai should sync to mobile automatically.

1. Open the Claude app on your phone
2. Go to **Settings** → check that the Bintrail connector appears

**If it doesn't appear:**
- Force-close and reopen the app
- Make sure you're logged into the same account as the web version
- Try pulling down to refresh on the settings page

### 3.2 Verify Tools Work

1. Open a new conversation on mobile
2. Ask: **"Show me the status of my binlog index"**

**What to verify:**
- [ ] The `status` tool is called successfully
- [ ] Results render correctly on the small screen

---

## 4. MCP Inspector

The MCP Inspector is a developer tool that tests the raw MCP protocol without Claude.

### 4.1 Run the Inspector

```sh
npx @modelcontextprotocol/inspector https://mcp.dbtrail.com/mcp
```

This opens a browser UI.

### 4.2 Test the OAuth Flow

1. The Inspector should detect the OAuth requirement and show a "Connect" button
2. Click **Connect**
3. Complete the OAuth flow (enter your tenant ID)
4. You should see "Connected" status

### 4.3 Test Protocol Messages

Once connected, in the Inspector UI:

**Test `initialize`:**
1. This happens automatically on connect
2. Verify the response shows `serverInfo` with the bintrail server name and version

**Test `tools/list`:**
1. Click **Tools** in the sidebar
2. You should see three tools: `query`, `recover`, `status`

**Test `tools/call`:**
1. Click on the `status` tool
2. Click **Call** (no parameters needed, unless your server requires `index_dsn`)
3. Verify the response contains indexed files and partition info

4. Click on the `query` tool
5. Fill in parameters: `schema` = your schema, `table` = your table
6. Click **Call**
7. Verify results come back

---

## 5. Rate Limiting Verification

The gateway now rate-limits OAuth endpoints to prevent abuse. Verify the limits work.

### 5.1 Test `/oauth/register` Rate Limit

Default: 10 requests/minute per IP.

```sh
# Send 11 register requests rapidly. The 11th should get 429.
for i in $(seq 1 11); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST https://mcp.dbtrail.com/oauth/register \
    -H 'Content-Type: application/json' \
    -d '{"client_name":"test-'$i'","redirect_uris":["http://localhost/cb"]}')
  echo "Request $i: HTTP $STATUS"
done
```

**Expected:** Requests 1–10 return `201`. Request 11 returns `429` with a `Retry-After` header.

### 5.2 Test `/oauth/token` Rate Limit

Default: 30 requests/minute per IP.

```sh
# Send 31 token requests. All will fail (bad grant), but only the 31st should be 429.
for i in $(seq 1 31); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST https://mcp.dbtrail.com/oauth/token \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -d 'grant_type=authorization_code&code=fake&client_id=fake&client_secret=fake&code_verifier=fake')
  echo "Request $i: HTTP $STATUS"
done
```

**Expected:** Requests 1–30 return `400` (bad grant). Request 31 returns `429`.

### 5.3 Test `/oauth/authorize` POST Rate Limit

Default: 20 requests/minute per IP.

```sh
for i in $(seq 1 21); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST https://mcp.dbtrail.com/oauth/authorize \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    -d 'client_id=fake&redirect_uri=http://localhost&code_challenge=test&tenant_id=fake')
  echo "Request $i: HTTP $STATUS"
done
```

**Expected:** Requests 1–20 return `400` or `401`. Request 21 returns `429`.

### 5.4 Verify Normal Usage Isn't Affected

After the rate limit window resets (wait 1 minute), verify that a normal OAuth flow still works:

```sh
# Should succeed (201)
curl -X POST https://mcp.dbtrail.com/oauth/register \
  -H 'Content-Type: application/json' \
  -d '{"client_name":"normal-test","redirect_uris":["http://localhost/cb"]}'
```

---

## 6. Request Logging & Correlation IDs

The gateway now logs every request with a unique `X-Request-Id`.

### 6.1 Verify Correlation IDs in Responses

```sh
curl -v https://mcp.dbtrail.com/health 2>&1 | grep -i x-request-id
```

**Expected:** Response includes `X-Request-Id: <uuid>` header.

### 6.2 Verify Client-Provided IDs Are Preserved

```sh
curl -v -H 'X-Request-Id: my-custom-trace-123' \
  https://mcp.dbtrail.com/health 2>&1 | grep -i x-request-id
```

**Expected:** Response header is `X-Request-Id: my-custom-trace-123` (preserved, not overwritten).

### 6.3 Verify Structured Logs

Check the gateway's log output (stderr, or wherever your log aggregator reads from):

```sh
# If running locally:
./mcp-gateway --addr :8443 ... 2>&1 | jq 'select(.msg == "request")'
```

**Expected log fields:**
```json
{
  "msg": "request",
  "method": "GET",
  "path": "/health",
  "status": 200,
  "duration_ms": 1,
  "client_ip": "203.0.113.50",
  "request_id": "abc123-..."
}
```

---

## 7. Security Review Checklist

These are manual verification steps. Check each item and mark it off.

### 7.1 Tokens Are Stored as SHA-256 Hashes

```sh
# Look at the raw DynamoDB tokens table. Values should be hex hashes, not raw tokens.
aws dynamodb scan --table-name bintrail-oauth-tokens --limit 3 \
  | jq '.Items[].access_token_hash.S'
```

**Expected:** 64-character hex strings like `"a1b2c3d4e5..."`  — NOT the raw Bearer token value.

### 7.2 Authorization Codes Are Single-Use

1. Register a test client:
   ```sh
   CLIENT=$(curl -s -X POST https://mcp.dbtrail.com/oauth/register \
     -H 'Content-Type: application/json' \
     -d '{"client_name":"security-test","redirect_uris":["http://localhost:9999/cb"]}')
   CLIENT_ID=$(echo $CLIENT | jq -r .client_id)
   CLIENT_SECRET=$(echo $CLIENT | jq -r .client_secret)
   echo "client_id=$CLIENT_ID"
   echo "client_secret=$CLIENT_SECRET"
   ```

2. Start the authorization flow manually:
   ```sh
   # Generate a PKCE verifier and challenge
   VERIFIER=$(openssl rand -hex 32)
   CHALLENGE=$(echo -n $VERIFIER | openssl dgst -sha256 -binary | openssl base64 -A | tr '+/' '-_' | tr -d '=')
   echo "verifier=$VERIFIER"
   echo "challenge=$CHALLENGE"

   # Open this URL in a browser:
   echo "https://mcp.dbtrail.com/oauth/authorize?client_id=$CLIENT_ID&redirect_uri=http://localhost:9999/cb&code_challenge=$CHALLENGE&code_challenge_method=S256&state=test123"
   ```

3. Submit the form with a valid tenant ID. The redirect will fail (localhost:9999 isn't running), but grab the `code` from the URL bar:
   ```
   http://localhost:9999/cb?code=THE_CODE_HERE&state=test123
   ```

4. Exchange the code for tokens:
   ```sh
   CODE="THE_CODE_HERE"
   curl -X POST https://mcp.dbtrail.com/oauth/token \
     -H 'Content-Type: application/x-www-form-urlencoded' \
     -d "grant_type=authorization_code&code=$CODE&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&code_verifier=$VERIFIER&redirect_uri=http://localhost:9999/cb"
   ```
   **Expected:** Returns access_token and refresh_token.

5. Try to use the same code again:
   ```sh
   curl -X POST https://mcp.dbtrail.com/oauth/token \
     -H 'Content-Type: application/x-www-form-urlencoded' \
     -d "grant_type=authorization_code&code=$CODE&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&code_verifier=$VERIFIER&redirect_uri=http://localhost:9999/cb"
   ```
   **Expected:** Returns `{"error":"invalid_grant","error_description":"invalid or expired authorization code"}`.

### 7.3 Authorization Codes Expire After 10 Minutes

1. Get a new authorization code (repeat steps 2–3 above with a new PKCE challenge)
2. **Wait 11 minutes**
3. Try to exchange the code

**Expected:** Same `invalid_grant` error.

### 7.4 PKCE S256 Is Enforced

Try exchanging a code with a **wrong** verifier:

```sh
curl -X POST https://mcp.dbtrail.com/oauth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=authorization_code&code=$CODE&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&code_verifier=wrong-verifier&redirect_uri=http://localhost:9999/cb"
```

**Expected:** `{"error":"invalid_grant","error_description":"PKCE verification failed"}`.

### 7.5 Refresh Token Rotation (Old Tokens Invalidated)

1. Get tokens from a successful code exchange (step 4 above)
2. Save the `refresh_token` value
3. Use the refresh token to get new tokens:
   ```sh
   REFRESH_TOKEN="the_refresh_token"
   curl -X POST https://mcp.dbtrail.com/oauth/token \
     -H 'Content-Type: application/x-www-form-urlencoded' \
     -d "grant_type=refresh_token&refresh_token=$REFRESH_TOKEN&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET"
   ```
   **Expected:** Returns new access_token AND new refresh_token.

4. Try to use the **old** refresh token again:
   ```sh
   curl -X POST https://mcp.dbtrail.com/oauth/token \
     -H 'Content-Type: application/x-www-form-urlencoded' \
     -d "grant_type=refresh_token&refresh_token=$REFRESH_TOKEN&client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET"
   ```
   **Expected:** `{"error":"invalid_grant","error_description":"invalid or expired refresh token"}`.

### 7.6 Origin Header Validation

```sh
# Allowed origin — should work (200 or 4xx from the endpoint, but not 403 from origin check)
curl -H 'Origin: https://claude.ai' https://mcp.dbtrail.com/health

# Disallowed origin — should get 403 Forbidden
curl -s -o /dev/null -w "%{http_code}" \
  -H 'Origin: https://evil-site.com' https://mcp.dbtrail.com/health
```

**Expected:** Second request returns `403`.

### 7.7 CORS Headers Only Allow Claude Origins

```sh
curl -v -X OPTIONS https://mcp.dbtrail.com/mcp \
  -H 'Origin: https://claude.ai' \
  -H 'Access-Control-Request-Method: POST' 2>&1 | grep -i access-control
```

**Expected:** `Access-Control-Allow-Origin: https://claude.ai` (NOT `*`).

```sh
curl -v -X OPTIONS https://mcp.dbtrail.com/mcp \
  -H 'Origin: https://random-site.com' \
  -H 'Access-Control-Request-Method: POST' 2>&1 | grep -i access-control
```

**Expected:** No `Access-Control-Allow-Origin` header (origin not in allow list).

---

## 8. Monitoring Setup (AWS CloudWatch)

These steps set up monitoring and alerting. They require AWS Console or CLI access.

### 8.1 Enable ALB Access Logs

```sh
# Create an S3 bucket for ALB logs
aws s3 mb s3://bintrail-alb-logs-YOUR_ACCOUNT_ID

# Enable access logging on the ALB
aws elbv2 modify-load-balancer-attributes \
  --load-balancer-arn YOUR_ALB_ARN \
  --attributes \
    Key=access_logs.s3.enabled,Value=true \
    Key=access_logs.s3.bucket,Value=bintrail-alb-logs-YOUR_ACCOUNT_ID \
    Key=access_logs.s3.prefix,Value=mcp-gateway
```

> Note: The S3 bucket needs a specific bucket policy for ALB to write logs. See [AWS docs](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/enable-access-logging.html).

### 8.2 Create CloudWatch Metric Filters

If the gateway runs on ECS/EC2 with CloudWatch Logs agent, create metric filters on the log group:

**Auth failures (401/403):**

```sh
aws logs put-metric-filter \
  --log-group-name /ecs/mcp-gateway \
  --filter-name AuthFailures \
  --filter-pattern '{ $.status = 401 || $.status = 403 }' \
  --metric-transformations \
    metricName=AuthFailures,metricNamespace=Bintrail/Gateway,metricValue=1,defaultValue=0
```

**Proxy errors (502):**

```sh
aws logs put-metric-filter \
  --log-group-name /ecs/mcp-gateway \
  --filter-name ProxyErrors \
  --filter-transformations \
  --filter-pattern '{ $.status = 502 }' \
  --metric-transformations \
    metricName=ProxyErrors,metricNamespace=Bintrail/Gateway,metricValue=1,defaultValue=0
```

**Rate limit hits (429):**

```sh
aws logs put-metric-filter \
  --log-group-name /ecs/mcp-gateway \
  --filter-name RateLimitHits \
  --filter-pattern '{ $.status = 429 }' \
  --metric-transformations \
    metricName=RateLimitHits,metricNamespace=Bintrail/Gateway,metricValue=1,defaultValue=0
```

**Request latency:**

```sh
aws logs put-metric-filter \
  --log-group-name /ecs/mcp-gateway \
  --filter-name RequestLatency \
  --filter-pattern '{ $.msg = "request" }' \
  --metric-transformations \
    metricName=RequestLatencyMs,metricNamespace=Bintrail/Gateway,metricValue=$.duration_ms,defaultValue=0
```

### 8.3 Create CloudWatch Alarms

**Alarm: High auth failure rate (potential token leak)**

```sh
aws cloudwatch put-metric-alarm \
  --alarm-name mcp-gateway-auth-failures \
  --alarm-description "High rate of 401/403 responses — possible token leak or brute force" \
  --namespace Bintrail/Gateway \
  --metric-name AuthFailures \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 50 \
  --comparison-operator GreaterThanThreshold \
  --alarm-actions YOUR_SNS_TOPIC_ARN \
  --treat-missing-data notBreaching
```

**Alarm: High proxy error rate (backend issues)**

```sh
aws cloudwatch put-metric-alarm \
  --alarm-name mcp-gateway-proxy-errors \
  --alarm-description "High rate of 502 responses — backend may be down" \
  --namespace Bintrail/Gateway \
  --metric-name ProxyErrors \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 1 \
  --threshold 10 \
  --comparison-operator GreaterThanThreshold \
  --alarm-actions YOUR_SNS_TOPIC_ARN \
  --treat-missing-data notBreaching
```

**Alarm: High rate limit hits (potential abuse)**

```sh
aws cloudwatch put-metric-alarm \
  --alarm-name mcp-gateway-rate-limits \
  --alarm-description "High rate of 429 responses — possible abuse" \
  --namespace Bintrail/Gateway \
  --metric-name RateLimitHits \
  --statistic Sum \
  --period 300 \
  --evaluation-periods 2 \
  --threshold 100 \
  --comparison-operator GreaterThanThreshold \
  --alarm-actions YOUR_SNS_TOPIC_ARN \
  --treat-missing-data notBreaching
```

### 8.4 Create an SNS Topic for Alerts

If you don't have one yet:

```sh
# Create the topic
aws sns create-topic --name bintrail-gateway-alerts
# → returns TopicArn — use this in the alarm-actions above

# Subscribe your email
aws sns subscribe \
  --topic-arn YOUR_SNS_TOPIC_ARN \
  --protocol email \
  --notification-endpoint you@example.com

# Confirm the subscription by clicking the link in the email
```

### 8.5 Verify Monitoring Works

1. Trigger a few auth failures:
   ```sh
   for i in $(seq 1 5); do
     curl -s https://mcp.dbtrail.com/mcp -H 'Authorization: Bearer invalid-token'
   done
   ```

2. Check CloudWatch metrics (allow 5 minutes for data to appear):
   ```sh
   aws cloudwatch get-metric-statistics \
     --namespace Bintrail/Gateway \
     --metric-name AuthFailures \
     --start-time $(date -u -v-10M +%Y-%m-%dT%H:%M:%S) \
     --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
     --period 300 \
     --statistics Sum
   ```

---

## 9. Documentation Checklist

Verify the docs are complete and accurate:

- [ ] `docs/mcp-server.md` documents both the connector method (recommended) and the proxy method (legacy fallback)
- [ ] `docs/mcp-gateway.md` has complete deployment instructions
- [ ] `README.md` mentions the Claude Connector in the MCP Server section
- [ ] `proxy.py` docstring/comments mention it's an optional legacy fallback

---

## Summary: All Checks

### Integration Testing
- [ ] Claude.ai: Add connector → OAuth flow → query/recover/status tools work
- [ ] Claude.ai: Token refresh (wait >1 hour, query again)
- [ ] Claude.ai: Two-tenant data isolation
- [ ] Claude Desktop: Add connector via UI → tools work → SSE streaming works
- [ ] Claude Mobile: Connector syncs from web → tools work
- [ ] MCP Inspector: Full protocol works (initialize → tools/list → tools/call)

### Rate Limiting
- [ ] `/oauth/register` — 429 after 10 requests/min
- [ ] `/oauth/token` — 429 after 30 requests/min
- [ ] `POST /oauth/authorize` — 429 after 20 requests/min
- [ ] Normal usage unaffected after window resets

### Monitoring
- [ ] CloudWatch metric filters created (auth failures, proxy errors, rate limits, latency)
- [ ] CloudWatch alarms created with SNS notifications
- [ ] Request logs include correlation IDs (`request_id` field)
- [ ] `X-Request-Id` header present in all responses

### Security
- [ ] Tokens stored as SHA-256 hashes (not plaintext)
- [ ] Authorization codes are single-use
- [ ] Authorization codes expire after 10 minutes
- [ ] PKCE S256 enforced on all code exchanges
- [ ] Refresh token rotation (old tokens invalidated)
- [ ] Origin header validation blocks unknown origins
- [ ] CORS headers only allow `claude.ai` and `claude.com`
