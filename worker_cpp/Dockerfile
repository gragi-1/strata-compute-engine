FROM debian:trixie@sha256:9cc080028c43b27d2074d63a5f9caf7166d731494965616c1a6d2827a004585c AS build
ARG BUILD_JOBS=2
RUN apt-get update && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends g++ cmake make curl ca-certificates xz-utils \
    libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev protobuf-compiler \
    libcurl4-openssl-dev nlohmann-json3-dev && rm -rf /var/lib/apt/lists/*
# Docker archives use only uncompressed tar. Exclude XML parsers and unused compression stacks.
RUN curl --fail --location --silent --show-error \
      https://github.com/libarchive/libarchive/releases/download/v3.8.9/libarchive-3.8.9.tar.xz \
      --output /tmp/libarchive.tar.xz \
    && echo '888c934f9d95648ecb9163dc8e23ab80a476ecb81a8f1154704a227b5b676dde  /tmp/libarchive.tar.xz' | sha256sum -c - \
    && mkdir /archive-source && tar -xJf /tmp/libarchive.tar.xz -C /archive-source --strip-components=1 \
    && cmake -S /archive-source -B /archive-build -DCMAKE_INSTALL_PREFIX=/archive-install \
      -DBUILD_SHARED_LIBS=OFF -DENABLE_TEST=OFF -DENABLE_TAR=OFF -DENABLE_CPIO=OFF \
      -DENABLE_CAT=OFF -DENABLE_UNZIP=OFF -DENABLE_LIBXML2=OFF -DENABLE_EXPAT=OFF \
      -DENABLE_OPENSSL=OFF -DENABLE_LIBB2=OFF -DENABLE_LZ4=OFF -DENABLE_LZMA=OFF \
      -DENABLE_ZSTD=OFF -DENABLE_ZLIB=OFF -DENABLE_BZip2=OFF -DENABLE_ICONV=OFF \
      -DENABLE_ACL=OFF -DENABLE_XATTR=OFF \
    && cmake --build /archive-build -j${BUILD_JOBS} && cmake --install /archive-build
COPY worker_cpp /src/worker_cpp
COPY proto /src/proto
COPY workloads/wave_solver /src/workloads/wave_solver
RUN cmake -S /src/worker_cpp -B /build -DCMAKE_BUILD_TYPE=Release \
      -DLibArchive_INCLUDE_DIR=/archive-install/include -DLibArchive_LIBRARY=/archive-install/lib/libarchive.a \
    && cmake --build /build -j${BUILD_JOBS} && ctest --test-dir /build --output-on-failure
FROM debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a
RUN apt-get update && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends libgrpc++1.51t64 libprotobuf32t64 \
      libcurl4t64 && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/strata-worker-cpp /usr/local/bin/strata-worker-cpp
COPY --from=build /archive-source/COPYING /usr/local/share/strata/libarchive-COPYING
COPY worker_cpp/vendor-components.json /usr/local/share/strata/vendor-components.json
CMD ["strata-worker-cpp"]
