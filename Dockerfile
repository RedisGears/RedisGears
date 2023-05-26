FROM ubuntu:kinetic

WORKDIR /build
ADD . /build

RUN apt-get update
RUN apt-get install -y git build-essential autoconf libtool curl libssl-dev pkg-config clang wget
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > install_rust.sh
RUN sh install_rust.sh -y
RUN git clone https://github.com/redis/redis; cd redis; git checkout 7.2-rc1; make install
RUN $HOME/.cargo/bin/cargo build --release

CMD ["redis-server", "--protected-mode", "no", "--loadmodule", "./target/release/libredisgears.so", "v8-plugin-path", "./target/release/libredisgears_v8_plugin.so"]
