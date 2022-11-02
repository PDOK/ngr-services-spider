# NGR SPIDER

CLI (command line interface) application to retrieve list of services and datasets in a simple JSON format from [nationaalgeoregister.nl](https://nationaalgeoregister.nl/) (NGR), leveraging the NGR [CSW service](https://nationaalgeoregister.nl/geonetwork/srv/dut/csw?service=CSW&request=GetCapabilities).

To install from source run (from the root this repo):

```sh
python3 -m build
python3 -m pip install dist/ngr_spider-0.0.1-py3-none-any.whl 
```

This should install the cli tool `ngr-spider`:

```sh
$ ngr-spider
usage: ngr-spider [-h] subcommand ...

Generate list of PDOK services and/or service layers

positional arguments:
  subcommand
    services  Generate list of all PDOK services
    layers    Generate list of all PDOK layers

options:
  -h, --help  show this help message and exit
```

## Examples

1. To generate a list of layers per service per dataset run the following:

```sh
ngr-spider layers -p OGC:WMS,OGC:WMTS -m datasets pdok-services.json
```

## Run Type Check

Install package from source with dev dependencies:

```sh
python3 -m pip install -e ".[dev]"  
```

Then run mypy for type checking (from root):

```sh
python3 -m mypy ngr_spider 
```


## Sorting Rules Example

When running the `layers` command in flat mode (`--mode flat`), it is possible to sort the layers in the output by passing a path to a JSON file containing, sorting rules. See below for an example a sorting rules JSON file (use for example with: `ngr-spider layers --sort sorting-rules.json -n 20 -m flat --pretty -p "OGC:WMS,OGC:WMTS" output.json`):


```json
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
```