# Development

## Commands

| Task | Command |
| ---- | ------- |
| Lint | `test -z "$(gofmt -l .)" && go vet ./... && golangci-lint run` |
| Unit tests | `go test -race ./...` |
| Integration tests | `go test -race -tags=integration -timeout 40m ./...` |
| Generate the server code | `go generate ./...` |

The integration tests need Docker. They start PostgreSQL, Valkey, and the identity image. The first run pulls the images. Each integration test file has the build tag `integration`. The flag `-race` needs a C compiler (cgo). On a computer with no C compiler, run the tests without it. The CI runs them with it.

## Code generation

`internal/handlers/http/gen/audit.gen.go` comes from `api/openapi/Constellation.Audit.openapi.yaml` with `oapi-codegen`. The CI runs `go generate ./...` and fails if the work tree changes.

## Update the contract

1. Copy `generated/openapi/Constellation.Audit.openapi.yaml` and `services/audit/manifest.json` from a release tag of `constellation-contracts` into `api/openapi/`. The manifest file is `audit-manifest.json`.
2. Write the tag in `api/openapi/CONTRACTS_VERSION`.
3. Run `go generate ./...`.
4. Run the tests.

## Dockerfile

The build stage uses `golang:1.27.4-alpine`. The final stage uses `gcr.io/distroless/static-debian12:nonroot`. Both images are pinned by digest. The entry point is `/audit`. The container exposes the ports 8080 and 9090. The image workflow builds the image on each pull request and pushes it on a version tag.

## The fixture of the tests

The package `resources/test/testserver` starts one network with:

- one PostgreSQL container with the database `identity`, and one new database for each audit service,
- one Valkey container,
- the identity image `ghcr.io/techorionai/constellation-identity` with the tag `0.7.1`.

The identity container has the issuer `http://localhost:<port>` with a fixed host port. The test makes one P-256 key pair for each client at the start and writes the clients file. The rate limits of identity are off.

| Client ID | App | Roles |
| --------- | --- | ----- |
| `audit` | `audit` | None. The implicit role `identity:service` lets it publish its manifest |
| `reader-svc` | `reader` | `audit:reader` |
| `noroles-svc` | `noroles` | None |

Identity gives a token only for the scopes of the roles of the client. A client with no role cannot get a token with an `audit` scope.

The audit service runs in the test process, except in the image test. Its Valkey user is an ACL user with the key pattern `~audit:*` and the selectors of `streamsvalkey.AuditReaderSelectors()`, as in production.

### Producers

A test producer (`env.NewProducer(t, app)`) registers the app in `audit.apps` as admin, as the provisioner does in production. `env.NewUnregisteredProducer(t, app)` makes the same producer with no registration. The producer has its own Valkey ACL user with the selectors of the event streams and of an audit producer (`ACLSelectors` and `AuditProducerSelectors`). It writes entries with the audit writer of platform-go and adds them to the audit stream with the audit publisher of platform-go. It does not use the database relay of the outbox. `PublishRaw` adds a payload to the stream with no check, for entries that the writer refuses.

`env.ResetAuditStreams` deletes `audit.apps`, `audit.streams`, and the audit streams. Each service test calls it before it makes its producers, so the tests do not read the entries of each other.

`env.NewProxiedDB` gives a database behind a TCP proxy. A test stops and starts the proxy to stop and start the database.
