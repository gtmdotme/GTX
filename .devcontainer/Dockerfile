# USAGE:
# Option 1: use vscode devcontainer
# Option 2: use docker commands
    # BUILD: $ docker build --tag gtx-base --platform linux/amd64 .
    # RUN: $ docker run -it --volume ~/gautamc/ubuntu:/root --name test --rm gtx-base
# NOTE: Currently, the image is built for amd64 architecture only.

# Base image
FROM --platform=linux/amd64 ubuntu:22.04

# Install basic packages
RUN echo "Updating and installing basic packages..." \
    && apt-get update \
    && apt-get install -y \
    build-essential \
    python3-dev \
    nano \
    git \
    curl \
    zip \
    unzip \
    tar \
    wget \
    && echo "Installed basic packages successfully!"
    
# Install cmake (from binary)
# ref: https://cmake.org/download/
RUN cd /tmp \
    && echo "Downloading cmake..." \
    && wget https://github.com/Kitware/CMake/releases/download/v3.30.2/cmake-3.30.2-linux-x86_64.tar.gz \
    && cd /usr \
    && echo "Extracting cmake..." \
    && tar --strip-components=1 -xzf /tmp/cmake-3.30.2-linux-x86_64.tar.gz \
    && echo "Cleaning up..." \
    && rm -rf /tmp/cmake-3.30.2-linux-x86_64.tar.gz \
    && echo "Installed cmake successfully!"
    
# Install oneTBB (from source)
# ref: https://github.com/oneapi-src/oneTBB/blob/master/INSTALL.md
RUN cd /tmp \
    && echo "Downloading oneTBB..." \
    && git clone https://github.com/oneapi-src/oneTBB.git \
    && cd oneTBB \
    && echo "Building oneTBB..." \
    && mkdir build && cd build \
    && cmake -DTBB_TEST=OFF .. \
    && cmake --build . \
    && echo "Installing oneTBB..." \
    && cmake --install . \
    && echo "Cleaning up..." \
    && rm -rf /tmp/oneTBB \
    && echo "Installed oneTBB successfully!"

# Install libtorch (from binary)
# ref: https://pytorch.org/cppdocs/installing.html
RUN cd /tmp \
    && echo "Downloading libtorch..." \
    && wget https://download.pytorch.org/libtorch/nightly/cpu/libtorch-shared-with-deps-latest.zip \
    && echo "Extracting libtorch..." \
    && unzip libtorch-shared-with-deps-latest.zip \
    && echo "Installing libtorch..." \
    && cd libtorch \
    && cp -r include/* /usr/local/include/ \
    && cp -r lib/* /usr/local/lib/ \
    && cp -r share/* /usr/local/share/ \
    && ldconfig \
    && echo "Cleaning up..." \
    && rm /tmp/libtorch-shared-with-deps-latest.zip \
    && rm -rf /tmp/libtorch \
    && echo "Installed libtorch successfully!"

# Install torch_scatter (from source)
# ref: https://github.com/rusty1s/pytorch_scatter
RUN cd /tmp \
    && echo "Downloading pytorch_scatter..." \
    && git clone https://github.com/rusty1s/pytorch_scatter \
    && echo "Building pytorch_scatter..." \
    && cd pytorch_scatter \
    && sed -i 's/set(CMAKE_CXX_STANDARD 14)/set(CMAKE_CXX_STANDARD 17)/' CMakeLists.txt \
    && mkdir build && cd build \
    && cmake .. \
    && make \
    && echo "Installing pytorch_scatter..." \
    && make install \
    && echo "Cleaning up..." \
    && rm -rf /tmp/pytorch_scatter \
    && echo "Installed pytorch_scatter successfully!"

# Install torch_sparse (from source)
# ref: https://github.com/rusty1s/pytorch_sparse
RUN cd /tmp \
    && echo "Downloading pytorch_sparse..." \
    && git clone --recurse-submodule https://github.com/rusty1s/pytorch_sparse \
    && echo "Building pytorch_sparse..." \
    && cd pytorch_sparse \
    && sed -i 's/set(CMAKE_CXX_STANDARD 14)/set(CMAKE_CXX_STANDARD 17)/' CMakeLists.txt \
    && mkdir build && cd build \
    && cmake .. \
    && make \
    && echo "Installing pytorch_sparse..." \
    && make install \
    && echo "Cleaning up..." \
    && rm -rf /tmp/pytorch_sparse \
    && echo "Installed pytorch_sparse successfully!"

# Build GTX (from source)
# ref: https://github.com/gtmdotme/GTX
RUN cd /tmp \
    && echo "Cloning GTX..." \
    && git clone --branch dev --single-branch --recurse-submodule https://github.com/gtmdotme/GTX \
    && cd GTX \
    && echo "Building GTX..." \
    && mkdir build && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Release .. \
    && make \
    && echo "Installing GTX..." \
    && make install \
    && ldconfig \
    && echo "Cleaning up..." \
    && rm -rf /tmp/GTX \
    && echo "Installed GTX successfully!"

# Define .bashrc
RUN echo 'PS1="\e[0;32m[\\u@\h]\e[0m\e[0;36m[\\w]\\$ \e[0m"' >> ~/.bashrc

WORKDIR /home
CMD ["/bin/bash"]
