FROM debian:bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ cmake make \
    libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev protobuf-compiler \
    libcurl4-openssl-dev libarchive-dev nlohmann-json3-dev && rm -rf /var/lib/apt/lists/*
COPY worker_cpp /src/worker_cpp
COPY proto /src/proto
COPY workloads/wave_solver /src/workloads/wave_solver
RUN cmake -S /src/worker_cpp -B /build -DCMAKE_BUILD_TYPE=Release && cmake --build /build -j2 && ctest --test-dir /build --output-on-failure
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libgrpc++1.51 libprotobuf32 \
    libcurl4 libarchive13 && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/strata-worker-cpp /usr/local/bin/strata-worker-cpp
CMD ["strata-worker-cpp"]
