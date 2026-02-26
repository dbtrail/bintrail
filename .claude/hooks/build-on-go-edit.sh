#!/bin/bash
INPUT=$(cat)
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty' 2>/dev/null)
if [[ "$FILE" == *.go ]]; then
  cd /Users/dani/bintrail && go build ./... && go vet ./...
fi
