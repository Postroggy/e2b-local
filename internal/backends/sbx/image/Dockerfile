ARG GO_VERSION=1.26.5

FROM golang:${GO_VERSION}-alpine AS envd-build
ARG ENVD_REPOSITORY=https://github.com/e2b-dev/infra.git
ARG ENVD_REF
ARG TARGETARCH
RUN apk add --no-cache git
RUN test -n "${ENVD_REF}"
RUN git clone --depth 1 --filter=blob:none --sparse "${ENVD_REPOSITORY}" /src/infra
RUN git -C /src/infra checkout "${ENVD_REF}"
RUN git -C /src/infra sparse-checkout set packages/envd packages/shared
WORKDIR /src/infra/packages/envd
RUN GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -buildvcs=false -a -o /out/envd .

FROM golang:${GO_VERSION}-alpine AS helper-build
ARG TARGETARCH
WORKDIR /src/e2b-local
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -buildvcs=false -ldflags='-s -w' -o /out/sbx-init ./internal/backends/sbx/cmd/sbx-init
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -buildvcs=false -ldflags='-s -w' -o /out/sbx-tunnel ./internal/backends/sbx/cmd/sbx-tunnel

FROM alpine:3.22
ARG ENVD_REPOSITORY=https://github.com/e2b-dev/infra.git
ARG ENVD_REF
LABEL org.opencontainers.image.source="${ENVD_REPOSITORY}" \
      org.opencontainers.image.revision="${ENVD_REF}" \
      org.opencontainers.image.title="e2b-local sbx envd"
COPY --from=envd-build /out/envd /usr/local/bin/envd
COPY --from=helper-build /out/sbx-init /usr/local/bin/sbx-init
COPY --from=helper-build /out/sbx-tunnel /usr/local/bin/sbx-tunnel
RUN mkdir -p /var/log /tmp /home/oai/share
EXPOSE 49983
ENTRYPOINT ["/usr/local/bin/sbx-init"]
