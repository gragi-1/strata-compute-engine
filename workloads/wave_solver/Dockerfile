FROM gcc:14 AS build
COPY main.cpp /src/main.cpp
RUN g++ -O3 -static -std=c++20 -Wall -Wextra -Werror /src/main.cpp -o /solver
FROM debian:bookworm-slim
RUN mkdir /output && chmod 1777 /output
COPY --from=build /solver /solver
USER 65534:65534
CMD ["/solver", "--output", "/output/wave.csv"]
