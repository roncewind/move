# -----------------------------------------------------------------------------
# Stages
# -----------------------------------------------------------------------------

ARG IMAGE_GO_BUILDER=golang:1.20.1
ARG IMAGE_FINAL=senzing/senzingapi-runtime:3.4.0

# -----------------------------------------------------------------------------
# Stage: go_builder
# -----------------------------------------------------------------------------

FROM ${IMAGE_GO_BUILDER} as go_builder
ENV REFRESHED_AT 2023-02-23c
LABEL Name="roncewind/move" \
      Maintainer="dad@lynntribe.net" \
      Version="0.0.0"

# Build arguments.

ARG PROGRAM_NAME="move"
ARG BUILD_VERSION=0.0.0
ARG BUILD_ITERATION=0
ARG GO_PACKAGE_NAME="github.com/roncewind/move"

# Copy local files from the Git repository.

# COPY ./rootfs /
COPY . ${GOPATH}/src/${GO_PACKAGE_NAME}

# Build go program.

WORKDIR ${GOPATH}/src/${GO_PACKAGE_NAME}
RUN make build-scratch

# --- Test go program ---------------------------------------------------------

# Run unit tests.

# RUN go get github.com/jstemmer/go-junit-report \
#  && mkdir -p /output/go-junit-report \
#  && go test -v ${GO_PACKAGE_NAME}/... | go-junit-report > /output/go-junit-report/test-report.xml

# Copy binaries to /output.

RUN mkdir -p /output \
      && cp -R ${GOPATH}/src/${GO_PACKAGE_NAME}/target/*  /output/

# -----------------------------------------------------------------------------
# Stage: final
# -----------------------------------------------------------------------------

FROM scratch as final
ENV REFRESHED_AT 2023-02-24b
LABEL Name="roncewind/move" \
      Maintainer="dad@lynntribe.net" \
      Version="0.0.0"

# Copy files from prior step.

COPY --from=go_builder "/output/scratch/move" "/app/move"

# Runtime execution.

# WORKDIR /app
ENTRYPOINT ["/app/move"]

# FROM ${IMAGE_FINAL} as final
# ENV REFRESHED_AT 2023-02-23
# LABEL Name="roncewind/move" \
#       Maintainer="dad@lynntribe.net" \
#       Version="0.0.0"

# # Copy files from prior step.

# COPY --from=go_builder "/output/linux/move" "/app/move"

# # Runtime environment variables.

# ENV LD_LIBRARY_PATH=/opt/senzing/g2/lib/

# # Runtime execution.

# WORKDIR /app
# ENTRYPOINT ["/app/move"]