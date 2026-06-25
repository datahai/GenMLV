# Fabric Integration Notes

This extension intentionally separates local planning from remote execution.

## Local features

- Validate naming and SQL shape
- Build deployment plan from SQL files and metadata file
- Identify candidate obsolete MLV keys from metadata

## Remote trigger model

The `Fabric MLV: Trigger Fabric Deployment` command calls an HTTP endpoint configured by:

- `fabricMlv.fabricApi.url`
- `fabricMlv.fabricApi.method`
- `fabricMlv.fabricApi.authToken`
- `fabricMlv.fabricApi.payloadJson`
- `fabricMlv.fabricApi.statusUrlTemplate`
- `fabricMlv.fabricApi.pollIntervalMs`
- `fabricMlv.fabricApi.maxPollAttempts`
- `fabricMlv.fabricApi.runIdFieldPath`
- `fabricMlv.fabricApi.statusFieldPath`
- `fabricMlv.fabricApi.successStates`
- `fabricMlv.fabricApi.failureStates`

Use this endpoint to trigger a Fabric notebook, pipeline, or service that runs the GenMLV logic in a Spark-enabled context.

## Trigger response conventions

Use `Fabric MLV: Apply Fabric API Profile` for quick configuration of common response shapes.
Use `Fabric MLV: Preview Fabric API Profile` to inspect exact setting values before applying, and click `Apply This Profile` in the notification to write them immediately.

Built-in presets include:

- Flat Default: `runId` and `status`
- Nested data.run: `data.run.id` and `data.run.state`
- Operation Pattern: `operationId` and `state.lifecycle`

For automatic polling, your trigger API should return JSON with one of these identifiers:

- `runId`
- `id`
- `operationId`

Or configure the exact nested location using `fabricMlv.fabricApi.runIdFieldPath`.

When `fabricMlv.fabricApi.statusUrlTemplate` is configured, the extension replaces `{runId}` and polls that endpoint.

Status payloads are considered terminal when one of these fields contains a final state:

- `status`
- `state`
- `result`

Or configure the exact nested location using `fabricMlv.fabricApi.statusFieldPath`.

Terminal values include: `succeeded`, `completed`, `failed`, `cancelled`, `canceled`.

## Why this split matters

`genmlv.py` depends on `spark.sql(...)` operations and Fabric Lakehouse paths, so actual CREATE, REPLACE, and DROP actions must run in Fabric runtime, not inside VS Code directly.
