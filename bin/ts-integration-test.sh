#!/usr/bin/env bash

set -e

cd clients/js
pnpm install
pnpm test
