#!/bin/bash
N=${1:-0}
echo $VCAP_SERVICES | jq -r ".[\"GAUSS DB\"][$N].credentials.uri"
