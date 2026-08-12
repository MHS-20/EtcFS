#!/bin/sh
# postinstall.sh — shared by the etcfuse and etcfuse-meta packages.
#
# Reloads the systemd unit cache after a unit file lands under
# /usr/lib/systemd/system. Does not enable or start anything: both daemons
# need cluster-specific flags (etcd endpoints, node ID, device) filled in
# first — see docs/deployment/configuration.md — so an unconditional
# `systemctl enable --now` here would start a daemon with no config and
# fail on every install.
set -e

if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
    systemctl daemon-reload || true
fi
