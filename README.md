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
