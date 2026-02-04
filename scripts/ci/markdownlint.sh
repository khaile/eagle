#!/usr/bin/env bash

set -euo pipefail

# Parse arguments
MODE="check"
if [ $# -gt 0 ]; then
  case "$1" in
    --check)
      MODE="check"
      ;;
    --fix)
      MODE="fix"
      ;;
    *)
      echo "Usage: $0 [--check|--fix]"
      echo "  --check  Check markdown files for issues (default)"
      echo "  --fix    Automatically fix markdown issues"
      exit 1
      ;;
  esac
fi

# Check if markdownlint is installed
if ! command -v markdownlint &> /dev/null; then
  echo "❌ markdownlint command not found"
  echo "💡 Install it using: npm install -g markdownlint-cli"
  exit 1
fi

# Files to ignore (in addition to .gitignore)

if [ "$MODE" = "fix" ]; then
  echo "🔧 Fixing markdown files..."
  markdownlint '**/*.md' --ignore-path .gitignore --ignore "AGENTS.md" --fix
  echo "✅ Markdown files have been fixed"
else
  echo "🔍 Checking markdown files..."
  if markdownlint '**/*.md' --ignore-path .gitignore --ignore "$IGNORE_FILES"; then
    echo "✅ All markdown files are properly formatted"
  else
    echo "❌ Markdown linting failed"
    echo "💡 Run '$0 --fix' to auto-fix issues"
    exit 1
  fi
fi
