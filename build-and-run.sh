#! /bin/bash

# Escape early if a command fails, e.g. if the build fails
set -e

go build
./goless
