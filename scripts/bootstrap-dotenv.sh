#!/bin/bash

set -euo pipefail
if ! type gh >/dev/null 2>&1; then
    echo "The 'gh' command could not be found. See <https://cli.github.com/> for installation instructions."
    exit 1
fi

STAGE=${STAGE:-dev}
ENV_FILE=${ENV_FILE:-.env.${STAGE}}
echo "Dumping envvars for STAGE=${STAGE} to ${ENV_FILE}"

# Per the output from the 'gh help formatting' command, with respect to the --jq
# directive: "The `jq` utility does not need to be installed on the system to use
# this formatting directive."
gh variable list --env "${STAGE}" --json name,value --jq 'map([.name, .value] | join("=")) | .[]' >"$ENV_FILE"
