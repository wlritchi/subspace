# syntax=docker/dockerfile:1
#
# Image for the subspace server, which the `notifications` Claude Code plugin's relay
# connects to (PR monitoring + agent messaging). The server is a PEP 723 uv script;
# its deps are baked from the committed lock (server/subspace-server.py.lock) so the
# running pod never reaches PyPI.
#
# CI builds and pushes releases (.forgejo/workflows/build.yaml). To build by hand,
# from the repository root:
#   DOCKER_BUILDKIT=1 docker build \
#     -t subspace:<tag> \
#     --build-arg VERSION=<version> \
#     --build-arg VCS_REF="$(git rev-parse --short HEAD)" \
#     --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
#     .

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build
WORKDIR /build
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1
# Resolve the script's pinned + hashed deps from its committed lock and bake them into an
# explicit venv (no project install — the server is a plain script, not a uv project).
# --frozen fails the build if the lock is missing/stale, keeping the image reproducible.
# The lock refers to ../protocol relative to the script, so run from server/ and copy
# the protocol package beside it. --no-editable installs it into the venv as a copy.
COPY protocol/ ./protocol/
COPY server/subspace-server.py server/subspace-server.py.lock ./server/
WORKDIR /build/server
RUN --mount=type=cache,target=/root/.cache/uv \
    uv export --script subspace-server.py --frozen --no-editable -o requirements.txt && \
    uv venv /opt/venv && \
    uv pip install --python /opt/venv -r requirements.txt

FROM python:3.12-slim-bookworm AS runtime
ARG VERSION=0.0.0
ARG VCS_REF=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="subspace" \
      org.opencontainers.image.description="Persistent WS server for the notifications Claude Code plugin (PR monitoring + agent messaging)" \
      org.opencontainers.image.source="https://github.com/wlritchi/subspace" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}"
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && \
    apt-get install -y --no-install-recommends tini && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
# Fixed uid/gid so a k8s PVC can be made writable via securityContext.fsGroup: 10001.
# The passwd entry exists for tools that getpwuid(), but the final USER is NUMERIC so a
# k8s runAsNonRoot:true admission check can verify non-root without a runAsUser override.
RUN groupadd -g 10001 app && useradd -u 10001 -g 10001 -M -s /usr/sbin/nologin app
COPY --from=build --chown=app:app /opt/venv /opt/venv
# Preserve the server/lib sibling layout — the server adds ../lib to sys.path at runtime.
COPY --chown=app:app server/subspace-server.py /app/subspace/server/subspace-server.py
COPY --chown=app:app lib/ /app/subspace/lib/
# /data is the SUBSPACE_DATA_DIR mount point; created + owned here for non-PVC runs.
# In k8s the mounted PVC needs securityContext.fsGroup: 10001 to be writable by `app`.
RUN install -d -o app -g app /data
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    SUBSPACE_WS_HOST=0.0.0.0 \
    SUBSPACE_WS_PORT=8137 \
    SUBSPACE_DATA_DIR=/data
USER 10001:10001
EXPOSE 8137
VOLUME ["/data"]
# Plain `python`: deps live in the venv, so the PEP723 header is inert (no runtime uv resolve).
ENTRYPOINT ["tini", "--"]
CMD ["python", "/app/subspace/server/subspace-server.py"]
