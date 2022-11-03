#!/usr/bin/env bash
# bash script to generate config for https://github.com/rduivenvoorde/pdokservicesplugin with ngr-services-spider
set -euo pipefail

output_file="$1"

spider_output=$(mktemp --suffix=.json -u)
cat <<EOF > /tmp/sorting-rules.json
[
  { "index": 0, "names": ["opentopo+"], "types": ["OGC:WMTS"] },
  { "index": 10, "names": ["^actueel_orthohr$"], "types": ["OGC:WMTS"] },
  { "index": 11, "names": ["^actueel_ortho25$"], "types": ["OGC:WMTS"] },
  { "index": 12, "names": ["^actueel_ortho25ir$"], "types": ["OGC:WMTS"] },
  { "index": 12, "names": ["lufolabels"], "types": ["OGC:WMTS"] },
  {
    "index": 20,
    "names": ["landgebied", "provinciegebied", "gemeentegebied"],
    "types": ["OGC:WFS"]
  },
  { "index": 30, "names": ["top+"], "types": ["OGC:WMTS"] },
  {
    "index": 32,
    "names": ["^standaard$", "^grijs$", "^pastel$", "^water$"],
    "types": ["OGC:WMTS"]
  },
  {
    "index": 34,
    "names": ["bgtstandaardv2", "bgtachtergrond"],
    "types": ["OGC:WMTS"]
  },
  { "index": 60, "names": ["ahn3+"], "types": ["OGC:WMTS"] }
]
EOF

docker run -v /tmp:/tmp ngr-spider layers -s /tmp/sorting-rules.json -m flat -p OGC:WMS,OGC:WFS,OGC:WCS,OGC:WMTS "$spider_output"

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
jq < "$spider_output" --argjson _lookup "$attr_map" \
    '.[] |= with_entries(if ($_lookup[.key] != null) then (.key = $_lookup[.key]) else (.) end)' | \
    jq '.[] |= with_entries(if .key == "service_type" then .value = (.value | split(":")[1] | ascii_downcase) else (.) end)' \
    > "$output_file"
echo "output written to ${output_file}"