#! /usr/bin/bash

ngr-spider layers -p OGC:WMS,OGC:WMTS -m datasets -n 10 --jq-filter '[{
    "themeName": "datasets",
    "datasets": [.datasets[] | {
        "datasetName": .title,
	      "infoUrl":  "https://nationaalgeoregister.nl/geonetwork/srv/dut/catalog.search#/metadata/\(.metadataId)",
        "services": [.services[] | {
            "type": .protocol | ascii_downcase | split(":")[1],
            "title": .title,
            "url": .url,
            "layers": [.layers[] | {
              "technicalName": .name,
              "name": .title,
              "legendUrl": .styles[0].legendUrl,
              "minResolution": .minscale,
              "maxResolution": .maxscale
            }]
        }]
    }]
}] | del(..|nulls)' themes.json