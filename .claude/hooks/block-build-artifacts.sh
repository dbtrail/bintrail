#!/bin/bash
INPUT=$(cat)
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty' 2>/dev/null)
if [[ "$FILE" =~ /(bintrail|bintrail-mcp|cover\.out)$ ]]; then
  jq -n '{
    "hookSpecificOutput": {
      "hookEventName": "PreToolUse",
      "permissionDecision": "deny",
      "permissionDecisionReason": "Blocked: do not edit build artifacts (bintrail, bintrail-mcp, cover.out)"
    }
  }'
fi
