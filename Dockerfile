FROM ubuntu:22.04

WORKDIR /build
ADD . /build

RUN apt-get update
RUN apt-get install -y git build-essential curl libssl-dev pkg-config clang wget
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > install_rust.sh
RUN sh install_rust.sh -y
RUN git clone https://github.com/redis/redis; cd redis; git checkout 7.0.3; make install
RUN $HOME/.cargo/bin/cargo build --release

CMD ["redis-server", "--protected-mode", "no", "--loadmodule", "./target/release/libredisgears.so", "./target/release/libredisgears_v8_plugin.so"]