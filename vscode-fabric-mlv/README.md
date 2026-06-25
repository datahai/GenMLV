# Fabric MLV Manager (VS Code Extension)

This extension helps author and orchestrate Materialized Lake View (MLV) deployment workflows for Microsoft Fabric.

## Commands

- Fabric MLV: Validate SQL Definitions
- Fabric MLV: Generate Deployment Plan
- Fabric MLV: Show Dependency Graph
- Fabric MLV: Trigger Fabric Deployment
- Fabric MLV: Refresh Explorer
- Fabric MLV: Apply Fabric API Profile
- Fabric MLV: Preview Fabric API Profile

## Explorer View

The extension contributes a Fabric MLV tree in the Explorer sidebar.

- Groups SQL definitions by folder order (for example 01, 02, 03)
- Lists schema.view definitions and opens files on click
- Shows obsolete metadata keys as cleanup candidates

## Fabric API Parsing

For APIs with nested response shapes, configure:

- `fabricMlv.fabricApi.runIdFieldPath` (default `runId`)
- `fabricMlv.fabricApi.statusFieldPath` (default `status`)
- `fabricMlv.fabricApi.successStates`
- `fabricMlv.fabricApi.failureStates`

Quick setup option:

- Run Fabric MLV: Apply Fabric API Profile and select one of the built-in presets.
- Run Fabric MLV: Preview Fabric API Profile to inspect exact settings before writing changes, then click Apply This Profile from the preview notification if desired.

Example:

```json
{
	"fabricMlv.fabricApi.runIdFieldPath": "data.run.id",
	"fabricMlv.fabricApi.statusFieldPath": "data.run.state",
	"fabricMlv.fabricApi.successStates": ["success", "completed"],
	"fabricMlv.fabricApi.failureStates": ["failed", "error", "cancelled"]
}
```

## How it maps to GenMLV

The extension mirrors the file-driven deployment model from `genmlv.py`:

- SQL definitions in a root folder (`fabricMlv.sqlRoot`)
- Schema and view name inferred from filename (`schema.view.sql`)
- Incremental planning using metadata timestamps (`fabricMlv.metadataFile`)
- Detection of obsolete MLVs from metadata drift

## Development

```bash
npm install
npm run compile
```

Press F5 in VS Code to launch an Extension Development Host.

## Packaging

```bash
npm run package
```

This creates a .vsix package for local installation and testing.
