#!/bin/bash

cd /tmp
git clone --recurse-submodules --depth 1 --branch v1.19.0 -j$(nproc) \
    https://github.com/open-telemetry/opentelemetry-cpp.git

# Build
cd opentelemetry-cpp
mkdir build && cd build
cmake .. \
  -DCMAKE_INSTALL_PREFIX=$HOME/.local \
  -DCMAKE_PREFIX_PATH=$HOME/.local \
  -DBUILD_TESTING=OFF \
  -DWITH_OTLP_GRPC=ON \
  -DWITH_OTLP_HTTP=OFF \
  -DCMAKE_BUILD_TYPE=Release \
  -DWITH_ABSEIL=ON

make -j$(nproc)
make install

# Cleanup
cd /tmp && rm -rf opentelemetry-cpp