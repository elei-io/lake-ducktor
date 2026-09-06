FROM ubuntu:24.04 AS ducklake-builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       ca-certificates \
       cmake \
       git \
       ninja-build \
       python3 \
    && rm -rf /var/lib/apt/lists/*

ARG DUCKDB_EXTENSION_BUILD_VERSION=v1.5.5
ARG DUCKLAKE_BASE_COMMIT=d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4
ARG CROARING_VERSION=v4.5.0

RUN git clone --depth 1 --branch "${CROARING_VERSION}" \
       https://github.com/RoaringBitmap/CRoaring.git /tmp/croaring \
    && cmake -S /tmp/croaring -B /tmp/croaring/build \
       -DCMAKE_BUILD_TYPE=Release \
       -DENABLE_ROARING_TESTS=OFF \
    && cmake --build /tmp/croaring/build --target install --parallel \
    && rm -rf /tmp/croaring

WORKDIR /build

RUN git clone --filter=blob:none https://github.com/duckdb/ducklake.git \
    && cd ducklake \
    && git checkout "${DUCKLAKE_BASE_COMMIT}" \
    && git submodule update --init extension-ci-tools \
    && git clone --depth 1 --branch "${DUCKDB_EXTENSION_BUILD_VERSION}" \
       https://github.com/duckdb/duckdb.git duckdb

COPY vendor/ducklake/0001-external-hive-compaction.patch /tmp/

RUN cd ducklake \
    && git apply /tmp/0001-external-hive-compaction.patch \
    && DISABLE_EXTENSIONS_FOR_TEST=1 cmake -G Ninja \
       -DEXTENSION_STATIC_BUILD=1 \
       -DDUCKDB_EXTENSION_CONFIGS=/build/ducklake/extension_config.cmake \
       -DBUILD_EXTENSION_TEST_DEPS=default \
       -DCMAKE_BUILD_TYPE=Release \
       -S duckdb \
       -B build/release \
    && cmake --build build/release \
       --target ducklake_loadable_extension \
       --parallel 2


FROM ubuntu:24.04

COPY --from=ghcr.io/astral-sh/uv:0.9.30 /uv /uvx /bin/

ENV HOME=/home/lakeducktor \
    PATH=/opt/lakeducktor-venv/bin:$PATH \
    DUCKDB_ALLOW_UNSIGNED_EXTENSIONS=true \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON=3.14 \
    UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    UV_PROJECT_ENVIRONMENT=/opt/lakeducktor-venv

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock README.md LICENSE THIRD_PARTY_NOTICES.md ./
COPY vendor/ducklake/LICENSE vendor/ducklake/LICENSE
COPY src/ src/
COPY --from=ducklake-builder \
    /build/ducklake/build/release/extension/ducklake/ducklake.duckdb_extension \
    /opt/lakeducktor/ducklake.duckdb_extension

RUN useradd --create-home --uid 10001 lakeducktor \
    && uv sync --frozen --no-dev --no-editable --extra postgres \
    && python -c \
       "import duckdb; c = duckdb.connect(config={'allow_unsigned_extensions': 'true'}); c.install_extension('/opt/lakeducktor/ducklake.duckdb_extension'); [c.install_extension(name) for name in ('postgres', 'httpfs')]; c.load_extension('ducklake'); c.close()" \
    && chown -R lakeducktor:lakeducktor \
       /home/lakeducktor /opt/lakeducktor-venv \
    && rm -f /bin/uv /bin/uvx

USER 10001:10001

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/readyz', timeout=2).read()"]

ENTRYPOINT ["lakeducktor"]
CMD ["run"]
