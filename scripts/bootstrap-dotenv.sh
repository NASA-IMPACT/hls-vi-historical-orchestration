#!/bin/bash

if ! command -v gh 2>&1 > /dev/null
then
    echo "The 'gh' command could not be found. Please install before using this script."
    exit 1
fi
if ! command -v jq 2>&1 > /dev/null
then
    echo "The 'jq' command could not be found. Please install before using this script."
    exit 1
fi

STAGE=${STAGE:-dev}
ENV_FILE=${ENV_FILE:-.env.${STAGE}}
echo "Dumping envvars for STAGE=${STAGE} to ${ENV_FILE}"


echo "STAGE=$STAGE" > "${ENV_FILE}"
gh variable list --env "${STAGE}" | awk -F '\t' '{ print $1 "=" $2 }' | grep -v STAGE >> "$ENV_FILE"
