# Installation Steps

## Install_Build_Pre_Requisite

### Update Package Manager

Update the OS package manager

#### ubuntu

```bash
sudo apt-get update -qqy
```

#### rhel

```bash
sudo yum  update -qqy
```

### Install With Package Manager

Install the following packages using the package manager (or any other way you prefer), wget, curl, git, openssl, libssl-dev, pkg-config, clang.

#### ubuntu

```bash
sudo apt-get install -qqy wget curl git openssl libssl-dev pkg-config clang
```

#### rhel

```bash
sudo yum install -qqy wget curl git openssl openssl-devel
```

### Install Build Tools

Install build-essential or its equivalent to the OS you are using

#### ubuntu

```bash
sudo apt-get install build-essential
```

#### rhel

##### 7

```bash
sudo yum groupinstall -yqq 'Development Tools'
sudo yum install -yqq centos-release-scl
sudo yum install -yqq devtoolset-9 llvm-toolset-7
export PATH=/opt/rh/devtoolset-9/root/usr/bin/:$PATH
export LD_LIBRARY_PATH=/opt/rh/llvm-toolset-7/root/usr/lib64:$LD_LIBRARY_PATH
export PATH=/opt/rh/llvm-toolset-7/root/usr/bin:$PATH
```

##### 8

```bash
sudo yum groupinstall -yqq 'Development Tools'
sudo yum install -yqq clang
```

### Install Rust

Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > install_rust.sh
sh install_rust.sh -y
export PATH=/home/meir/.cargo/bin:$PATH
```

#### MSRV (Minimally Supported Rust Version)
Currently the edition 2021 of Rust language is used and the MSRV is `1.67`.

## Build_Release

### Cargo Build

Build the project for release using Cargo Build

```bash
cargo build --release
```

## Build_Debug

### Cargo Build

Build the project for debug using Cargo Build

```bash
cargo build
```

## Install_Tests_Pre_Requisite

### Install RLTest

Install RLTest to run the python tests

```bash
python3 -m pip install RLTest
```

### Install Redis

Install Redis 7.0.3

```bash
mkdir -p redis
git clone https://github.com/redis/redis ./redis
cd redis
git checkout 7.0.3
make
sudo make install
```

## Run_Test

### Change Directory

Enter directory ./pytests

```bash
cd ./pytests
```

### Run Tests With RLTest

Run the tests using RLTest

```bash
python3 -m RLTest --parallelism 10
```
