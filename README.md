# HLS Vegetation Indices (HLS-VI) Historical Orchestration



## Getting started

This project uses `uv` to manage dependencies and virtual environments. To install this, please visit the
uv [installation documentation](https://docs.astral.sh/uv/getting-started/installation/) for instructions.

### Testing

Run unit tests,
```
scripts/test
```

### Formatting and Linting

Run formatting,
```
scripts/format
```

Run linting,
```
scripts/lint
```

### Deployment

To deploy with CDK,
```
uv run cdk deploy
```

You may consider pointing UV to a different `.env` file, e.g.,
```
uv run --env-file .env.dev -- cdk deploy
```
or using an environment variable,
```
UV_ENV_FILE=.env.dev uv run cdk deploy
```
