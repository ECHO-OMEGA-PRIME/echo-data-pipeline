# Echo Data Pipeline

> ETL / data-pipeline orchestration Worker for the ECHO ecosystem (v1.0.0).
> Define pipelines, run them on demand or via a queue, and track runs and metrics
> — a Hono app on Cloudflare Workers.

Private to Echo Prime Technologies.

## What it does

A **pipeline** is a declared ETL definition. You **run** it (immediately or via the
**queue**), and each execution is tracked as a **run** with status and **metrics**.
Pipelines can be **paused** and **resumed**.

## API (auth: `X-Echo-API-Key`)

| Method | Route | Purpose |
|---|---|---|
| `GET`  | `/` , `/health` | Service info / liveness |
| `GET`  | `/stats` | Pipeline + run statistics |
| `GET`  | `/pipelines` | List pipelines |
| `POST` | `/pipelines` | Create a pipeline |
| `GET`  | `/pipelines/:id` | Get a pipeline |
| `DELETE` | `/pipelines/:id` | Delete a pipeline |
| `POST` | `/pipelines/:id/run` | Trigger a run |
| `POST` | `/pipelines/:id/pause` · `/resume` | Pause / resume a pipeline |
| `GET`  | `/pipelines/:id/runs` | Run history for a pipeline |
| `GET`  | `/pipelines/:id/metrics` | Pipeline metrics |
| `GET`  | `/runs/:id` | Get a run |
| `GET`  | `/queue` | Inspect the run queue |

## Develop

```bash
npm install
npx wrangler dev       # local Worker
npx wrangler deploy    # deploy
```

Bindings (queue, storage) and secrets are configured in `wrangler.toml` / the
Cloudflare dashboard — never commit them.

## License

Proprietary — © Echo Prime Technologies. All rights reserved.
