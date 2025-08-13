#!/bin/sh
# start-executor.sh
# Ensure permissive umask so the created UDS socket is world-writable (DEV use only).
# In production you should create a group for communication and set group permissions instead.
umask 000

# Ensure the socket directory exists
mkdir -p /tmp/executor_sock
# Remove stale socket (safe to ignore errors)
[ -S /tmp/executor_sock/executor.sock ] && rm -f /tmp/executor_sock/executor.sock

# Start uvicorn listening on a UDS path
exec uvicorn executor:app --uds /tmp/executor_sock/executor.sock --host 0.0.0.0 --log-level info

