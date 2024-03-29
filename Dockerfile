# -----------------------------------------------------------------------------
# Stages
# -----------------------------------------------------------------------------

ARG IMAGE_GO_BUILDER=golang:1.20.4
ARG IMAGE_FINAL=senzing/senzingapi-runtime:latest

# -----------------------------------------------------------------------------
# Stage: go_builder
# -----------------------------------------------------------------------------

FROM ${IMAGE_GO_BUILDER} as go_builder
ENV REFRESHED_AT 2023-02-25
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

# Install Certificate
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates

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
ENV REFRESHED_AT 2023-02-25
LABEL Name="roncewind/move" \
      Maintainer="dad@lynntribe.net" \
      Version="0.0.0"

# Copy Certificate
COPY --from=go_builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy files from prior step.
COPY --from=go_builder "/output/scratch/move" "/app/move"

# Runtime execution.

# WORKDIR /app
ENTRYPOINT ["/app/move"]

