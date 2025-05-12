# HLS Vegetation Indices (HLS-VI) Historical Orchestration

This repository orchestrates the historical backfill for the HLS-VI data product for the HLS archive.
See also:

* https://github.com/NASA-IMPACT/hls-vi
* https://github.com/NASA-IMPACT/hls-vi-historical

## Getting started

This project uses `uv` to manage dependencies and virtual environments. To install this, please visit the
uv [installation documentation](https://docs.astral.sh/uv/getting-started/installation/) for instructions.

### Dotenv

This project uses a `.env` ("dotenv") file to help manage application settings for the environments
we support (`dev` and `prod`).

You can use the `env.sample` as a starting point for populating settings, or use the
`scripts/bootstrap-dotenv.sh` script to populate your `.env.${STAGE}` from our Github
environment. To use this script you must have the Github CLI (`gh`) and `jq` installed.

For example,
```bash
$ STAGE=prod scripts/bootstrap-dotenv.sh
Dumping envvars for STAGE=prod to .env.prod
```

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
