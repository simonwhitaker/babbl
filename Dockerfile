# syntax=docker/dockerfile:1

FROM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies first, so a source-only change doesn't re-download them.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/babbl .

FROM alpine:3.22

# Alpine's wget serves the healthcheck; the shell is here for `exec`.
RUN adduser -D -H -u 10001 babbl \
    && mkdir -p /var/lib/babbl \
    && chown babbl:babbl /var/lib/babbl

COPY --from=build /out/babbl /usr/local/bin/babbl

USER babbl
ENV BABBL_DIR=/var/lib/babbl
VOLUME /var/lib/babbl
EXPOSE 8787

ENTRYPOINT ["babbl"]
# The default 127.0.0.1 would only be reachable from inside the container.
CMD ["serve", "--addr", "0.0.0.0:8787"]
