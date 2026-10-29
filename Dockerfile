FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/pipeline ./cmd/pipeline \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/simulate ./cmd/simulate

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /usr/local/bin/
COPY rules.json /etc/pipeline/rules.json
ENV RULES_FILE=/etc/pipeline/rules.json
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/usr/local/bin/pipeline"]
