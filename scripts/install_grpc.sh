#!/bin/bash

sudo apt update
sudo apt install -y cmake \
	git \
	build-essential \
	autoconf \
    libtool \
    pkg-config \
	ninja-build \
	libssl-dev \
	gcc \
	g++ \
	make \
	cmake-curses-gui \
	wget \
	unzip \
	curl \
	gdb \
	cgdb

function install_mqtt_c_lib {
	TMP_FOLDER="/tmp/mosquitto"

	rm -rf $TMP_FOLDER
	mkdir -p $TMP_FOLDER
	pushd $TMP_FOLDER
	git clone https://github.com/eclipse/paho.mqtt.c.git
	cd paho.mqtt.c
	make -j$(nproc)
	sudo make uninstall | true  # clean up first
	sudo make install   # install mosquitto c lib
	popd
}

install_mqtt_c_lib

# Build and Install gRPC
export MY_INSTALL_DIR=$HOME/.local
mkdir -p $MY_INSTALL_DIR
export PATH="$MY_INSTALL_DIR/bin:$PATH"

pushd $HOME
rm -rf grpc
git clone --depth 1 --recurse-submodules -j$(nproc) https://github.com/USC-NSL-DDB/grpc.git

cd grpc
mkdir -p cmake/build
cd cmake/build
cmake -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DgRPC_INSTALL=ON \
  -DgRPC_BUILD_TESTS=OFF \
  -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
  -DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
  ../..
cmake --build . --target install -- -j$(nproc)
popd