#!/bin/bash
# Configure Core Dump settings for debugging crashes
# Usage: sudo ./coredump-config.sh

set -euo pipefail

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit 1
fi

# Directory to store core dumps
CORE_DIR="/var/crash/eagle"
# USER_NAME=${SUDO_USER:-$USER}

echo "Configuring core dump environment..."

# Create crash directory
if [ ! -d "$CORE_DIR" ]; then
    echo "Creating crash directory: $CORE_DIR"
    mkdir -p "$CORE_DIR"
fi

# Set permissions (allow anyone to write, or restricted to user?)
# Usually sticky bit or owned by user.
chmod 777 "$CORE_DIR"

# Enable unlimited core dumps
echo "Setting ulimit -c unlimited..."
ulimit -c unlimited

# Configure kernel core pattern
# %e: executable filename
# %p: pid
# %t: time of dump (UNIX timestamp)
# %u: uid
PATTERN="$CORE_DIR/core.%e.%p.%t"

echo "Setting core_pattern to: $PATTERN"
echo "$PATTERN" > /proc/sys/kernel/core_pattern

# Verify settings
echo ""
echo "Configuration verified:"
echo "  Core Pattern: $(cat /proc/sys/kernel/core_pattern)"
echo "  Crash Dir: $CORE_DIR"
echo "  Ulimit: $(ulimit -c)"

echo ""
echo "To test configuration, you can run:"
echo "  sleep 100 & kill -SIGSEGV \$!"
