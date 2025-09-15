# HLS Vegetation Indices (HLS-VI) Historical Orchestration

This repository orchestrates the historical backfill for the HLS-VI data product for the HLS archive. See also:

- https://github.com/NASA-IMPACT/hls-vi
- https://github.com/NASA-IMPACT/hls-vi-historical

## Getting started

This project uses `uv` to manage dependencies and virtual environments. To install this, please visit the uv
[installation documentation](https://docs.astral.sh/uv/getting-started/installation/) for instructions.

### Development

Install dependencies for resolving references in your favorite IDE:

```plain
uv sync --all-groups
```

### Testing

Run unit tests,

```plain
scripts/test
```

### Formatting and Linting

Run formatting,

```plain
scripts/format
```

Run linting,

```plain
scripts/lint
```

### Dotenv

This project uses a `.env` ("dotenv") file to help manage application settings for the environments we support (`dev`
and `prod`).

You can use the `env.sample` as a starting point for populating settings, or use the `scripts/bootstrap-dotenv.sh`
script to populate your `.env.${STAGE}` from our Github environment. To use this script you must have the Github CLI
(`gh`).

For example,

```bash
$ STAGE=prod scripts/bootstrap-dotenv.sh
Dumping envvars for STAGE=prod to .env.prod
```

### Deployment

We deploy this project to AWS using Python and CDK. First make sure that you've installed the deployment requirements,

```plain
uv sync --group deploy
```

Next, you probably want to bootstrap a "dotenv" file using the `scripts/bootstrap-dotenv.sh` script to gather deployment
configuration settings. Note that if you are using a "dotenv" file you will need to explicitly tell `uv` to load it
using a command line flag,

```plain
uv run --env-file .env.dev -- cdk diff
```

or using an environment variable,

```plain
UV_ENV_FILE=.env.dev uv run cdk diff
```

Viewing the CDK Cloudformation stack diff is a good first step to make sure your infrastructural changes are working
as intended. You probably should deploy only using the CI/CD pipelines configured for this repository, but for
completeness you could also deploy using,

```plain
UV_ENV_FILE=.env.dev uv run cdk deploy
```
