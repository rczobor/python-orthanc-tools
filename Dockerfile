# The full image builds wheels, including any locked package that ships only
# as a source distribution. Its compilers and libraries stay out of the runtime.
FROM python:3.15@sha256:46837ed3105db0029b6d656816185c6258e3b39690532fb782428e0a5fd76182 AS build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /src
COPY requirements.lock requirements-build.lock ./
RUN python -m pip install --no-cache-dir --require-hashes -r requirements-build.lock \
    && python -m pip wheel --no-cache-dir --no-deps --no-build-isolation --require-hashes \
       -r requirements.lock --wheel-dir /wheels/runtime \
    && python -m pip wheel --no-cache-dir --no-deps --require-hashes \
       -r requirements-build.lock --wheel-dir /wheels/build

COPY orthanc_tools/ ./orthanc_tools/
COPY LICENSE.txt MANIFEST.in pyproject.toml README.md setup.cfg setup.py requirements.txt ./
RUN python -m build --no-isolation --wheel --outdir /wheels/runtime .

FROM python:3.15-slim@sha256:65a65fc7b30b06cb6df4930fd4650697cb23f4c2698ab81fc87e4b031bc36928

ARG VCS_REF=unknown
LABEL org.opencontainers.image.source="https://github.com/rczobor/python-orthanc-tools" \
      org.opencontainers.image.version="0.23.2" \
      org.opencontainers.image.revision="${VCS_REF}"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Only the locked pip is taken from the build tools: it replaces the base
# image's pip and its vendored urllib3/msgpack copies.
RUN --mount=type=bind,from=build,source=/wheels,target=/wheels \
    python -m pip install --no-cache-dir --no-index --no-deps /wheels/build/pip-*.whl /wheels/runtime/*.whl \
    && python -m pip check \
    && useradd --uid 10001 --user-group --no-create-home --shell /usr/sbin/nologin orthanc-tools

USER 10001
WORKDIR /
