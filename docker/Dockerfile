# syntax=docker/dockerfile:1
#
# Stages:
#   toolchain - compiler, CMake and vcpkg at the commit pinned in vcpkg.json
#   deps      - third-party libraries only (cached until vcpkg.json changes)
#   build     - compiles TaskFlow and its tests with the "ci" CMake preset
#   test      - build tree + services-aware test command (used by `docker compose --profile test`)
#   runtime   - minimal non-root image containing only the taskflow binary and migrations

ARG UBUNTU_IMAGE=ubuntu:24.04

FROM ${UBUNTU_IMAGE} AS toolchain
ARG DEBIAN_FRONTEND=noninteractive
# Keep VCPKG_COMMIT identical to "builtin-baseline" in vcpkg.json (CI verifies this).
ARG VCPKG_COMMIT=4b308f5ccd45aed57f2977257bca304dfef43c99
RUN apt-get update && apt-get install -y --no-install-recommends \
        autoconf automake bison build-essential ca-certificates cmake curl flex git libtool \
        ninja-build perl pkg-config python3 tar unzip uuid-dev zip \
    && rm -rf /var/lib/apt/lists/*
RUN git init /opt/vcpkg \
    && git -C /opt/vcpkg remote add origin https://github.com/microsoft/vcpkg.git \
    && git -C /opt/vcpkg fetch --depth 1 origin ${VCPKG_COMMIT} \
    && git -C /opt/vcpkg checkout FETCH_HEAD \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics
ENV VCPKG_ROOT=/opt/vcpkg \
    VCPKG_DISABLE_METRICS=1

FROM toolchain AS deps
ARG VCPKG_TRIPLET=x64-linux
ARG VCPKG_MAX_CONCURRENCY=4
ENV VCPKG_MAX_CONCURRENCY=${VCPKG_MAX_CONCURRENCY}
WORKDIR /src
COPY vcpkg.json ./
RUN /opt/vcpkg/vcpkg install --triplet ${VCPKG_TRIPLET} --x-install-root=/opt/vcpkg_installed

FROM deps AS build
ARG VCPKG_TRIPLET=x64-linux
WORKDIR /src
COPY . .
RUN cmake --preset ci \
        -DVCPKG_TARGET_TRIPLET=${VCPKG_TRIPLET} \
        -DVCPKG_INSTALLED_DIR=/opt/vcpkg_installed \
        -DVCPKG_MANIFEST_INSTALL=OFF \
    && cmake --build --preset ci --parallel 4 \
    && strip build/ci/taskflow

FROM build AS test
# Needs PostgreSQL and Redis; see the "taskflow-test" service in docker-compose.yml.
CMD ["ctest", "--preset", "ci"]

FROM ${UBUNTU_IMAGE} AS runtime
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libuuid1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin taskflow
WORKDIR /app
COPY --from=build /src/build/ci/taskflow /app/taskflow
COPY migrations /app/migrations
ENV TASKFLOW_MIGRATIONS_DIR=/app/migrations
USER 10001
EXPOSE 8080
ENTRYPOINT ["/app/taskflow"]
