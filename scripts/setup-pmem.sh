#!/bin/bash
# Setup Persistent Memory (PMEM) or emulate it
# Usage: sudo ./setup-pmem.sh [size_in_gb]

set -euo pipefail

MOUNT_POINT="/mnt/pmem"
PMEM_DEV="/dev/pmem0"
USER_NAME=${SUDO_USER:-$USER}
SIZE_GB=${1:-2}

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit 1
fi

echo "Setting up PMEM environment..."

# Create mount point
if [ ! -d "$MOUNT_POINT" ]; then
    echo "Creating mount point $MOUNT_POINT..."
    mkdir -p "$MOUNT_POINT"
fi

# Check if actual PMEM device exists
if [ -b "$PMEM_DEV" ]; then
    echo "Found PMEM device: $PMEM_DEV"

    # Check if already mounted
    if mount | grep -q "$MOUNT_POINT"; then
        echo "PMEM already mounted at $MOUNT_POINT"
    else
        echo "Formatting and mounting PMEM device..."
        # Check if formatted (simple check)
        if ! blkid "$PMEM_DEV" > /dev/null; then
            echo "Formatting $PMEM_DEV..."
            mkfs.ext4 -F "$PMEM_DEV"
        fi

        mount -o dax "$PMEM_DEV" "$MOUNT_POINT"
        echo "Mounted $PMEM_DEV at $MOUNT_POINT with DAX enabled"
    fi
else
    echo "No PMEM device found ($PMEM_DEV)."
    echo "Setting up emulation using tmpfs (RAM)..."
    echo "WARNING: Data will be lost on reboot!"

    if mount | grep -q "$MOUNT_POINT"; then
        echo "Emulation already mounted at $MOUNT_POINT"
    else
        echo "Mounting tmpfs of size ${SIZE_GB}G..."
        mount -t tmpfs -o size="${SIZE_GB}G tmpfs $MOUNT_POINT"
    fi
fi

# Set permissions so the user can write to it
echo "Setting permissions for user: $USER_NAME"
chown -R "$USER_NAME" "$MOUNT_POINT"
chmod 755 "$MOUNT_POINT"

echo "PMEM setup complete."
echo "Location: $MOUNT_POINT"
df -h "$MOUNT_POINT"
