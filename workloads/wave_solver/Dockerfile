FROM debian:trixie@sha256:9cc080028c43b27d2074d63a5f9caf7166d731494965616c1a6d2827a004585c AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/*
COPY main.cpp /src/main.cpp
RUN g++ -O3 -static -std=c++20 -Wall -Wextra -Werror /src/main.cpp -o /solver
FROM debian:trixie-slim@sha256:a99cfc517144bc59b1978475ec53b46ecabec7e43635402ee5b77cc54cd1b20a
RUN apt-get update && apt-get upgrade --no-install-recommends -y \
    && rm -rf /var/lib/apt/lists/* && mkdir /output && chmod 1777 /output
COPY --from=build /solver /solver
USER 65534:65534
CMD ["/solver", "--output", "/output/wave.csv"]
