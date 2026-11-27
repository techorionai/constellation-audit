// Package openapi holds the copies of the files of constellation-contracts
// that the service reads at run time: the OpenAPI file, for the
// x-required-scopes values of the operations, and the permission manifest of
// the audit service.
package openapi

import "embed"

// Spec is the OpenAPI file of constellation-contracts.
//
//go:embed Constellation.Audit.openapi.yaml
var Spec []byte

// Files holds the permission manifest of the audit service as
// audit-manifest.json.
//
//go:embed audit-manifest.json
var Files embed.FS

// AuditManifestPath is the path of the manifest in Files.
const AuditManifestPath = "audit-manifest.json"
