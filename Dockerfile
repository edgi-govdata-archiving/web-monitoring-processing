# Base Image  -----------------------------------------------------------------
FROM python:3.10.16-slim AS base
LABEL org.opencontainers.image.authors="enviroDGI@gmail.com"
LABEL maintainer="enviroDGI@gmail.com"


# Building Deps & Native Code -------------------------------------------------
FROM base AS build

ARG VERSION_NUMBER=1.0.0-dev

# Need build tools for some dependencies (Cchardet, Lxml)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ pkg-config libxml2-dev libxslt-dev libz-dev

# Set the working directory to /app
WORKDIR /app

# Copy the requirements.txt alone into the container at /app
# so that they can be cached more aggressively than the rest of the source.
ADD pyproject.toml /app
ADD README.md /app
RUN mkdir /app/web_monitoring

ENV SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION_NUMBER}
# Install any needed packages specified in requirements.txt
# RUN pip install --user --trusted-host pypi.python.org -r requirements.txt
RUN pip install --user --trusted-host pypi.python.org .

# Copy the rest of the source.
ADD . /app

# Install package.
RUN pip install --user '.[cchardet]'


# Deployable Image w/out Build-Only Dependencies ------------------------------
FROM base AS release

RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2 libxslt1.1 zlib1g

# Copy installed/built Python packages from build image.
COPY --from=build /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Set the working directory to /app
WORKDIR /app

# Copy the rest of the source. No need to reinstall here, since it will have
# been installed in the build layer above.
COPY --from=build /app /app

# NOTE: no built-in command; run something like `scripts/ia_healthcheck`
