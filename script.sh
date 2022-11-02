#!/usr/bin/env bash
# script to transform output for pdokservicesplugin
attr_map=$(cat <<"EOF"
{
    "serviceMetadataId": "service_metadata_id",
    "serviceTitle": "service_title",
    "serviceAbstract": "service_abstract",
    "serviceUrl": "service_url",
    "datasetMetadataId": "dataset_metadata_id",
    "serviceProtocol": "service_type"
}
EOF
)

# first jq invocation to remap keys of output (assumes flat output of spider) 
# second jq invocation to convert service_type field
jq < test.json --argjson _lookup "$attr_map" \
    '.[] |= with_entries(if ($_lookup[.key] != null) then (.key = $_lookup[.key]) else (.) end)' | \
    jq '.[] |= with_entries(if .key == "service_type" then .value = (.value | split(":")[1] | ascii_downcase) else (.) end)'
