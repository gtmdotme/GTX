# USAGE:
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
    
# # Build GTX (from source)
# # ref: https://github.com/gtmdotme/GTX
# RUN cd /root \
#     && echo "Cloning GTX..." \
#     && git clone --branch dev --single-branch --recurse-submodule https://github.com/gtmdotme/GTX \
#     && cd GTX \
#     && echo "Building GTX..." \
#     && mkdir build && cd build \
#     && cmake -DCMAKE_BUILD_TYPE=Release .. \
#     && echo "Installing GTX..." \
#     && make -j \
#     && echo "Built GTX successfully!"

# # Install libtorch
# # ref: https://pytorch.org/cppdocs/installing.html
# RUN cd /root \
#     && echo "Downloading libtorch..." \
#     && wget https://download.pytorch.org/libtorch/nightly/cpu/libtorch-shared-with-deps-latest.zip \
#     && echo "Extracting libtorch..." \
#     && unzip libtorch-shared-with-deps-latest.zip -d /usr/local \
#     && echo "Cleaning up..." \
#     && rm libtorch-shared-with-deps-latest.zip \
#     && echo "Extracted libtorch successfully!"

WORKDIR /root
CMD ["/bin/bash"]
