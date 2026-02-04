# PMEM Setup Guide

This guide explains how to set up and configure Persistent Memory (PMEM) for use with EagleDB.

## Prerequisites

- Linux kernel version 4.2 or later
- PMEM-capable hardware
- ndctl and daxctl utilities installed
- Root access to the system

## Hardware Requirements

- Intel Optane DC Persistent Memory Module(s)
- Or NVDIMM-N devices
- Supported CPU with PMEM controller

## System Configuration

### 1. Check PMEM Hardware

```bash
# List PMEM Namespaces
ndctl list

# Check PMEM Regions
ndctl list -R
```

### 2. Configure PMEM Mode

Choose between:

- App Direct Mode (recommended)
- Memory Mode
- Mixed Mode

### 3. Create Namespaces

```bash
# Create a namespace in fsdax mode
ndctl create-namespace --mode=fsdax --map=dev --size=100G

# Verify creation
ndctl list -N
```

### 4. Mount PMEM

```bash
# Format the PMEM device
mkfs.ext4 /dev/pmem0

# Create mount point
mkdir -p /mnt/pmem0

# Mount with DAX option
mount -o dax /dev/pmem0 /mnt/pmem0
```

### 5. NUMA Configuration

For multi-socket systems:

```bash
# Check NUMA nodes
numactl --hardware

# Set NUMA policy
numactl --membind=0 --cpunodebind=0 ./eagledb
```

## EagleDB Configuration

Update your EagleDB configuration to use PMEM:

```toml
[pmem]
path = "/mnt/pmem0/eagledb"
size = "100G"
numa_node = 0

[store]
engine = "pmem"
```

## Verification

1. Check PMEM is properly mounted:

```bash
df -h /mnt/pmem0
```

1. Verify DAX is enabled:

```bash
mount | grep dax
```

1. Test PMEM performance:

```bash
fio --name=pmem-test --filename=/mnt/pmem0/test \
    --direct=1 --rw=randwrite --bs=4k --size=1G
```

## Troubleshooting

### Common Issues

  1. PMEM not detected
      - Check BIOS settings
      - Verify kernel support
      - Update firmware

  2. Performance issues
      - Verify NUMA configuration
      - Check filesystem mount options
      - Monitor memory mode settings

  3. Persistence problems
      - Verify power-loss protection
      - Check filesystem journal settings
      - Review application flush operations
