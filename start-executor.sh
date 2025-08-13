#!/bin/sh
# start-executor.sh - ensure socket dir exists and has permissive perms, then start uvicorn on UDS

# make sure the socket dir exists (runs as whichever user the container starts as)
mkdir -p /tmp/executor_sock

# remove stale socket if present (ignore errors)
[ -S /tmp/executor_sock/executor.sock ] && rm -f /tmp/executor_sock/executor.sock

# make the directory world-writable (dev convenience)
chmod 0777 /tmp/executor_sock

# start uvicorn on the UDS
exec uvicorn executor:app --uds /tmp/executor_sock/executor.sock --host 0.0.0.0 --log-level info

