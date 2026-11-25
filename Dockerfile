FROM golang:1.26 AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/pipeline ./cmd/pipeline \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/simulate ./cmd/simulate

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /usr/local/bin/
COPY rules.json /etc/pipeline/rules.json
ENV RULES_FILE=/etc/pipeline/rules.json
EXPOSE 8080
# Numeric, so Kubernetes can verify runAsNonRoot.
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/pipeline"]
