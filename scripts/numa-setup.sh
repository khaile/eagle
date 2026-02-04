#!/bin/bash
# Configure NUMA settings for optimal database performance
# Usage: sudo ./numa-setup.sh

set -euo pipefail

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit 1
fi

echo "Configuring NUMA settings..."

# 1. Disable Zone Reclaim Mode
# This prevents the kernel from aggressively reclaiming memory from specific zones,
# which can cause performance spikes in database workloads.
if [ -f /proc/sys/vm/zone_reclaim_mode ]; then
    CURRENT_VAL=$(cat /proc/sys/vm/zone_reclaim_mode)
    if [ "$CURRENT_VAL" != "0" ]; then
        echo "Disabling vm.zone_reclaim_mode (current: $CURRENT_VAL)..."
        echo 0 > /proc/sys/vm/zone_reclaim_mode
    else
        echo "vm.zone_reclaim_mode is already disabled."
    fi
else
    echo "vm.zone_reclaim_mode not found (skipping)"
fi

# 2. Check for THP (Transparent Huge Pages)
# Often recommended to disable or set to 'madvise' for low-latency DBs.
THP_PATH="/sys/kernel/mm/transparent_hugepage/enabled"
if [ -f "$THP_PATH" ]; then
    echo "Transparent Huge Pages status: $(cat $THP_PATH)"
    # We don't enforce disabling it here as it depends on specific workload tuning,
    # but printing it is useful.
    echo "Recommendation: Consider setting to 'madvise' or 'never' if latency spikes occur."
fi

# 3. Display Topology
if command -v numactl &> /dev/null; then
    echo ""
    echo "NUMA Hardware Topology:"
    numactl --hardware
    echo ""
    echo "To run the database bound to a specific node:"
    echo "  numactl --cpunodebind=0 --membind=0 ./target/release/eagle ..."
else
    echo "numactl tool not found. Install 'numactl' package for better diagnostics."
fi

echo "NUMA configuration check complete."
