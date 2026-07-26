#!/bin/bash
SEVENZIP_VERSION=$1
MACHINE=$(uname -m)
case "$MACHINE" in
  x86_64        ) SEVENZIP_ARCH="x64" ;;
  x86           ) SEVENZIP_ARCH="x86" ;;
  armv6l|armv7l ) SEVENZIP_ARCH="arm" ;;
  aarch64       ) SEVENZIP_ARCH="arm64" ;;
esac
curl -SLs "https://7-zip.org/a/7z$SEVENZIP_VERSION-linux-$SEVENZIP_ARCH.tar.xz" | tar -xJ 7zzs
