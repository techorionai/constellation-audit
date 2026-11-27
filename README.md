# constellation-audit

The audit service of the Constellation platform. It keeps an append-only audit trail for all services. Services write audit entries through their audit outbox and the Valkey streams `audit.stream:<app>`. The audit service reads the streams and stores each entry one time. It deletes entries after the retention period (60 days by default).

This version has the intake and the retention. The search, get, and export operations of the API give 501 `not_implemented`. The next version adds them.

## Docs

- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [Intake](docs/intake.md)
- [Retention](docs/retention.md)
- [Development](docs/development.md)

## Quick start

```sh
go test -race ./...
go test -race -tags=integration -timeout 40m ./...
docker build -t constellation-audit .
```

The API contract is in `constellation-contracts`. This repository has a copy of the OpenAPI file in `api/openapi/`. The file `api/openapi/CONTRACTS_VERSION` gives the release tag of the copy.

## License

MIT. See `LICENSE`.
