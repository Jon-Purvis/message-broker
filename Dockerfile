FROM debian:bookworm-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN make clean && make

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    zlib1g \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /app/broker /app/broker
RUN mkdir -p /app/data

EXPOSE 3490
CMD ["./broker"]