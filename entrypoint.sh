#!/bin/sh
set -e

VDR_GATEWAY="${VDR_GATEWAY:-192.168.1.224}"
VDR_SUBNET="${VDR_SUBNET:-10.10.1.0/24}"

echo "Configuring route to VDR virtual subnet (${VDR_SUBNET} via ${VDR_GATEWAY})..."
if ip route show "${VDR_SUBNET}" 2>/dev/null | grep -q .; then
  echo "Route ${VDR_SUBNET} already present."
else
  if ip route add "${VDR_SUBNET}" via "${VDR_GATEWAY}" 2>/dev/null; then
    echo "Route ${VDR_SUBNET} via ${VDR_GATEWAY} added."
  else
    echo "Warning: could not add route ${VDR_SUBNET} via ${VDR_GATEWAY} (NET_ADMIN required)."
  fi
fi

exec "$@"
