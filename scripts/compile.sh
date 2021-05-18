#! /usr/bin/bash

set -e

rm -rf build/
mkdir build/
mkdir -p build/peer/filesystem/storage/2/files/
cp -R assets/* build/peer/filesystem/storage/2/files/

# shellcheck disable=SC2046
javac -d build/ $(find . | grep .java) &>/dev/null
