#!/usr/bin/env python3
import argparse
import asyncio
import enum
from importlib.metadata import metadata
import itertools
import json
import logging
import re
from typing import OrderedDict
import warnings
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from types import MethodType
from urllib import parse
from dataclasses import asdict

import requests
from owslib.csw import CatalogueServiceWeb
from owslib.wcs import WebCoverageService, wcs110
from owslib.wfs import WebFeatureService
from owslib.wms import WebMapService
from owslib.wmts import WebMapTileService

from .servicemetadata import CswDatasetRecord, CswListRecord, CswServiceRecord, Layer, Service, WcsService, WfsService, WmsLayer, WmsService, WmsStyle, WmtsLayer, WmtsService

CSW_URL = "https://nationaalgeoregister.nl/geonetwork/srv/dut/csw"
LOG_LEVEL = "INFO"
PROTOCOLS = ["OGC:WMS", "OGC:WFS", "OGC:WMTS", "OGC:WCS"]

SORTING_RULES = {
    0: {"names": ["opentopo+"], "types": ["wmts"]},
    10: {"names": ["^actueel_orthohr$"], "types": ["wmts"]},
    11: {"names": ["^actueel_ortho25$"], "types": ["wmts"]},
    12: {"names": ["^actueel_ortho25ir$"], "types": ["wmts"]},
    13: {"names": ["lufolabels"], "types": ["wmts"]},
    # 15: {'names': ['^\d{4}_ortho'], 'types': ['wmts']},
    # 16: {'names': ['^\d{4}_ortho+IR'], 'types': ['wmts']},
    20: {
        "names": ["landgebied", "provinciegebied", "gemeentegebied"],
        "types": ["wfs"],
    },
    30: {"names": ["top+"], "types": ["wmts"]},
    32: {
        "names": ["^standaard$", "^grijs$", "^pastel$", "^water$"],
        "types": ["wmts"],
    },  # BRT-lagen
    34: {"names": ["bgtstandaardv2", "bgtachtergrond"], "types": ["wmts"]},
    60: {"names": ["ahn3+"], "types": ["wmts"]},
    # 90: {'names': ['aan+'], 'types': ['wmts']},
}


logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s: %(message)s",
)


def get_sorting_value(layer_info):
    if not "name" in layer_info:
        return 101
    layer_name = layer_info["name"].lower()
    for key, sorting_rule in SORTING_RULES.items():
        if layer_info["service_type"].lower() in sorting_rule["types"]:
            for name in sorting_rule["names"]:
                if re.search(name, layer_name) is not None:
                    return key
    if layer_info["service_type"].lower() == "wmts":
        return 99  # other wmts layers
    else:
        return 100  # all other layers


def is_popular(service):
    if service["service_type"].lower() == "wmts":
        return True
    return False


def join_lists_by_property(list_1, list_2, prop_name):
    lst = sorted(itertools.chain(list_1, list_2), key=lambda x: x[prop_name])
    result = []
    for k, v in itertools.groupby(lst, key=lambda x: x[prop_name]):
        d = {}
        for dct in v:
            d.update(dct)
        result.append(d)
    return result


def get_csw_results(query:str, maxresults:int=0) -> list[CswListRecord]:
    csw = CatalogueServiceWeb(CSW_URL)
    result = []
    start = 1
    maxrecord = maxresults if (maxresults < 100 and maxresults != 0) else 100

    while True:
        csw.getrecords2(maxrecords=maxrecord, cql=query, startposition=start)
        records = [
            CswListRecord(
                title=rec[1].title,
                abstract=rec[1].abstract,
                type=rec[1].type,
                identifier=rec[1].identifier,
                keywords=rec[1].subjects,
                modified=rec[1].modified,
            )
            for rec in csw.records.items()
        ]
        result.extend(records)
        if (
            maxresults != 0 and len(result) >= maxresults
        ):  # break only early when maxresults set
            break
        if csw.results["nextrecord"] != 0:
            start = csw.results["nextrecord"]
            continue
        break
    return result


def get_csw_results_by_id(id: str) -> list[CswListRecord]:
    query = f"identifier='{id}'"
    return get_csw_results(query)    


def get_csw_results_by_protocol(protocol:str, maxresults:int=0) -> list[CswListRecord]:
    svc_owner = "Beheer PDOK"
    query = (
        f"type='service' AND organisationName='{svc_owner}' AND protocol='{protocol}'"
    )
    records = get_csw_results(query, maxresults)
    logging.info(f"found {len(records)} {protocol} service metadata records")
    return records


def get_record_by_id(metadata_id:str):
    csw = CatalogueServiceWeb(CSW_URL)
    csw.getrecordbyid(id=[metadata_id])
    return csw.records[metadata_id]


# def get_identifier_from_getrecordbyid_url(self, url):
#     url = url.lower()


# def get_protocol_by_ur(url):
#     for prot in PROTOCOLS:
#         # pattern looks like this: '.*\/wms(:?\/|\?).*'
#         pattern = re.compile(f'.*\/{prot.split(":")[1].lower()}(:?\/|\?).*')
#         m = pattern.match(url)
#         if m:
#             return prot
#     return ""


def get_dataset_metadata(md_id: str) -> CswDatasetRecord:
    csw = CatalogueServiceWeb(CSW_URL)
    csw.getrecordbyid(id=[md_id], outputschema="http://www.isotc211.org/2005/gmd")
    try:
        record = csw.records[md_id]
        result = CswDatasetRecord(
            title = record.identification.title,
            abstract = record.identification.abstract,
            metadata_id = md_id
        )
        return result
    except KeyError:  # TODO: log missing dataset records
        return None

def get_csw_service_records(record: CswListRecord) -> CswServiceRecord:
    """Retrieve service metadata record for input["metadata_id"] en return title, protocol en service url

    Args:
        input (dict): dict containing "metadata_id" key and optional "protocol"

    Returns:
        ServiceMetadata
    """
    csw = CatalogueServiceWeb(CSW_URL)
    csw.getrecordbyid(id=[record.identifier], outputschema="http://www.isotc211.org/2005/gmd")
    record = csw.records[record.identifier]
    service_metadata = CswServiceRecord(record.xml)

    service_url = service_metadata.service_url
    service_url = service_url.partition("?")[0]
    protocol = service_metadata.service_protocol
    query_param_svc_type = protocol.split(":")[1]
    if (
        "https://geodata.nationaalgeoregister.nl/tiles/service/wmts" in service_url
    ):  # shorten paths, some wmts services have redundant path elements in service_url
        service_url = "https://geodata.nationaalgeoregister.nl/tiles/service/wmts"
    if service_url.endswith(
        "/WMTSCapabilities.xml"
    ):  # handle cases for restful wmts url, assume kvp variant is supported
        service_url = service_url.replace("/WMTSCapabilities.xml", "")
    service_metadata.service_url = (
        f"{service_url}?request=GetCapabilities&service={query_param_svc_type}"
    )
    return service_metadata
    


async def get_data_asynchronous(results, fun):
    result = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(
                executor,
                fun,
                *(in_result,),
            )
            for in_result in results
        ]
        for task_result in await asyncio.gather(*tasks):
            result.append(task_result)
        return result


def get_services_list(service_record: CswServiceRecord):
    function_mapping = {
        "OGC:WMS": get_wms_cap,
        "OGC:WFS": get_wfs_cap,
        "OGC:WCS": get_wcs_cap,
        "OGC:WMTS": get_wmts_cap,
    }
    services_list: list[Service]= function_mapping[service_record.service_protocol](service_record)
    return services_list


def empty_string_if_none(input_str):
    return input_str if input_str is not None else ""


# def get_wms_cap(service_record: CswServiceRecord) -> WmsService:  
#     def convert_layer(lyr) -> WmsLayer:
def get_wcs_cap(service_record: CswServiceRecord)-> WcsService: 
    def convert_layer(lyr) -> Layer:
        return Layer (
            title= empty_string_if_none(wcs[lyr].title),
            abstract= empty_string_if_none(wcs[lyr].abstract),
            name= wcs[lyr].id,
            dataset_md_id=service_record.dataset_metadata_id
        )

    def OWS(cls, tag):
        return "{http://www.opengis.net/ows/1.1}" + tag
   
    try:
        url = service_record.service_url
        md_id = service_record.metadata_id
        logging.info(f"{md_id} - {url}")
        # monkeypatch OWS method to fix namespace issue
        # owslib is using a different namespace url than mapserver is in cap doc
        wcs110.Namespaces_1_1_0.OWS = MethodType(OWS, wcs110.Namespaces_1_1_0)
        # use version=1.1.0 since that cap doc list title and abstract for coverage
        wcs = WebCoverageService(url, version="1.1.0")
        # getcoverage_op = next(
        #     (x for x in wcs.operations if x.name == "GetCoverage"), None
        # )
        return WcsService(
            title=empty_string_if_none(wcs.identification.title),
            abstract=empty_string_if_none(wcs.identification.abstract),
            metadata_id=md_id,
            url=service_record.service_url,
            coverages=list(map(convert_layer, list(wcs.contents))),
            keywords=wcs.identification.keywords,
            dataset_metadata_id=service_record.dataset_metadata_id
        )
    except requests.exceptions.HTTPError as e:
        logging.error(f"md_id: {md_id} - {e}")
    except Exception:
        message = f"exception while retrieving WCS cap for service md-identifier: {md_id}, url: {url}"
        logging.exception(message)
    return None

def get_wfs_cap(service_record: CswServiceRecord) -> Service:
    def convert_layer(lyr) -> Layer:
        return Layer(
            title= empty_string_if_none(wfs[lyr].title),
            abstract=empty_string_if_none(wfs[lyr].abstract),
            name=wfs[lyr].id,
            dataset_md_id=service_record.dataset_metadata_id,
        ) 
    try:
        url = service_record.service_url
        md_id = service_record.metadata_id
        logging.info(f"{md_id} - {url}")
        wfs = WebFeatureService(url, version="2.0.0")
        getfeature_op = next(
            (x for x in wfs.operations if x.name == "GetFeature"), None
        )
        return WfsService(
            title=empty_string_if_none(wfs.identification.title),
            abstract=empty_string_if_none(wfs.identification.abstract),
            metadata_id=md_id,
            url=service_record.service_url,
            output_formats=getfeature_op.parameters["outputFormat"]["values"],
            featuretypes=list(map(convert_layer, list(wfs.contents))),
            keywords=wfs.identification.keywords,
            dataset_metadata_id=service_record.dataset_metadata_id
        )
    except requests.exceptions.HTTPError as e:
        logging.error(f"md-identifier: {md_id} - {e}")
    except Exception:
        message = f"exception while retrieving WFS cap for service md-identifier: {md_id}, url: {url}"
        logging.exception(message)
    return None


def get_md_id_from_url(url):
    logging.debug(f"get_md_id_from_url url: {url}")
    params = dict(parse.parse_qsl(parse.urlsplit(url).query))
    params = {k.lower(): v for k, v in params.items()}  # lowercase dict keys
    if "uuid" in params:
        return params["uuid"]
    else:
        return params["id"]


def get_wms_cap(service_record: CswServiceRecord) -> WmsService:  
    def convert_layer(lyr) -> WmsLayer:
        styles: list[WmsStyle] = []
        for style_name in list(wms[lyr].styles.keys()):
            style_obj = wms[lyr].styles[style_name]            
            title = None
            if "title" in style_obj:
                title = style_obj["title"]
            style = WmsStyle(title=title, name=style_name)
            styles.append(style)
        minscale = (
            wms[lyr].min_scale_denominator.text
            if wms[lyr].min_scale_denominator is not None
            else ""
        )
        maxscale = (
            wms[lyr].max_scale_denominator.text
            if wms[lyr].max_scale_denominator is not None
            else ""
        )
        tc211_md_urls = [x for x in wms[lyr].metadataUrls if x["type"] == "TC211"]
        dataset_md_url = tc211_md_urls[0]["url"] if len(tc211_md_urls) > 0 else ""
        dataset_md_id = "" if not dataset_md_url else get_md_id_from_url(dataset_md_url)

        return WmsLayer(
            name=lyr,
            title=empty_string_if_none(wms[lyr].title),
            abstract= empty_string_if_none(wms[lyr].abstract),
            styles=styles,
            crs=",".join([x[4] for x in wms[lyr].crs_list]),
            minscale=minscale,
            maxscale=maxscale,
            dataset_md_id=dataset_md_id
        )
    try:
        if "://secure" in service_record.service_url:
            # this is a secure layer not for the general public: ignore
            # TODO: apply filtering elswhere
            return None
        logging.info(f"{service_record.metadata_id} - {service_record.service_url}")
        wms = WebMapService(service_record.service_url, version="1.3.0")
        getmap_op = next((x for x in wms.operations if x.name == "GetMap"), None)
        layers: OrderedDict = list(wms.contents)     
        return WmsService(
            title=empty_string_if_none(wms.identification.title),
            abstract=empty_string_if_none(wms.identification.abstract),
            keywords=wms.identification.keywords,
            layers=list(map(convert_layer, layers)),
            imgformats=",".join(getmap_op.formatOptions),
            metadata_id=service_record.metadata_id,
            url=service_record.service_url,
            dataset_metadata_id=service_record.dataset_metadata_id
        )
    except requests.exceptions.HTTPError as e:
        logging.error(f"md-identifier: {service_record.metadata_id} - {e}")
    except Exception:
        message = f"exception while retrieving WMS cap for service md-identifier: {service_record.metadata_id}, url: {service_record.service_url}"
        logging.exception(message)
    return None


def get_wmts_cap(service_record: CswServiceRecord)-> WmtsService:
    def convert_layer(lyr) -> WmtsLayer:
        return WmtsLayer( 
            name= lyr,
            title= empty_string_if_none(wmts[lyr].title),
            abstract= empty_string_if_none(wmts[lyr].abstract),
            tilematrixsets= ",".join(list(wmts[lyr].tilematrixsetlinks.keys())),
            imgformats= ",".join(wmts[lyr].formats),
            dataset_md_id=service_record.dataset_metadata_id,
        )

    try:
        url = service_record.service_url
        md_id = service_record.metadata_id
        logging.info(f"{md_id} - {url}")
        if "://secure" in url:
            # this is a secure layer not for the general public: ignore
            return service_record
        wmts = WebMapTileService(url)
        return WmtsService(
            title=empty_string_if_none(wmts.identification.title),
            abstract=empty_string_if_none(wmts.identification.abstract),
            metadata_id=md_id,
            url=url,
            layers=list(map(convert_layer, list(wmts.contents))),
            keywords=wmts.identification.keywords,
            dataset_metadata_id=service_record.dataset_metadata_id
        )

    except requests.exceptions.HTTPError as e:
        logging.error(f"md-identifier: {md_id} - {e}")
    except Exception:
        message = f"unexpected error occured while retrieving cap doc, md-identifier {md_id}, url: {url}"
        logging.exception(message)
    return None


def flatten_service(service):
    def flatten_layer_wms(layer):
        layer["imgformats"] = service["imgformats"]
        layer["service_url"] = service["url"]
        layer["service_title"] = service["title"]
        layer["service_type"] = service["protocol"].split(":")[1].lower()
        layer["service_abstract"] = service["abstract"]
        layer["service_metadata_id"] = service["metadata_id"]
        layer["dataset_metadata_id"] = service["dataset_metadata_id"]
        return layer

    def flatten_layer_wcs(layer):
        layer["service_url"] = service["url"]
        layer["service_title"] = service["title"]
        layer["service_type"] = service["protocol"].split(":")[1].lower()
        layer["service_abstract"] = service["abstract"] if (not None) else ""
        layer["service_metadata_id"] = service["metadata_id"]
        layer["dataset_metadata_id"] = service["dataset_metadata_id"]

        return layer

    def flatten_layer_wfs(layer):
        layer["service_url"] = service["url"]
        layer["service_title"] = service["title"]
        layer["service_type"] = service["protocol"].split(":")[1].lower()
        layer["service_abstract"] = service["abstract"] if (not None) else ""
        layer["service_metadata_id"] = service["metadata_id"]
        layer["dataset_metadata_id"] = service["dataset_metadata_id"]
        return layer

    def flatten_layer_wmts(layer):
        layer["service_title"] = service["title"]
        layer["service_url"] = service["url"]
        layer["service_type"] = service["protocol"].split(":")[1].lower()
        layer["service_abstract"] = service["abstract"] if (not None) else ""
        layer["service_metadata_id"] = service["metadata_id"]
        layer["dataset_metadata_id"] = service["dataset_metadata_id"]
        return layer

    def flatten_layer(layer):
        fun_mapping = {
            "OGC:WMS": flatten_layer_wms,
            "OGC:WFS": flatten_layer_wfs,
            "OGC:WCS": flatten_layer_wcs,
            "OGC:WMTS": flatten_layer_wmts,
        }
        return fun_mapping[service["protocol"]](layer)

    result = list(map(flatten_layer, service["layers"]))
    return result


def sort_flat_layers(layers):
    sorted_layer_dict = {}
    for layer in layers:
        sorting_value = get_sorting_value(layer)
        if sorting_value in sorted_layer_dict:
            sorted_layer_dict[sorting_value].append(layer)
        else:
            sorted_layer_dict[sorting_value] = [layer]
    result = []
    for key in sorted(sorted_layer_dict.keys()):
        if len(sorted_layer_dict[key]) == 0:
            logging.info(f"no layers found for sorting rule: {SORTING_RULES[key]}")
        result += sorted_layer_dict[key]
    return result


def get_csw_list_result(protocol_list: list[str], number_records:int) -> list[CswListRecord]:
    csw_results = list(
        map(
            lambda x: get_csw_results_by_protocol(x, number_records),
            protocol_list,
        )
    )
    return [
        item for sublist in csw_results for item in sublist
    ]  # flatten list of lists


def get_capabilities_docs(service_records: list[CswServiceRecord]):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(service_records, get_services_list))
    loop.run_until_complete(future)
    services_list: list[Service] = future.result()
    return services_list


def get_datasets(dataset_ids: list[str])-> list[CswDatasetRecord]:
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        get_data_asynchronous(dataset_ids, get_dataset_metadata)
    )
    loop.run_until_complete(future)
    datasets: list[CswDatasetRecord] = future.result()
    datasets = list(
        filter(None, datasets)
    )  # filter out empty datasets, happens when an expected dataset metadatarecords is not present in NGR
    return datasets


def get_services(list_records: list[CswListRecord]) -> list[CswServiceRecord]:
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        get_data_asynchronous(list_records, get_csw_service_records)
    )
    loop.run_until_complete(future)
    services = future.result()
    filtered_services = filter_service_records(services)
    return sorted(filtered_services, key=lambda x: x.title)


def report_services_summary(services: list[CswServiceRecord], protocol_list: list[str]):
    nr_services = len(services)
    logging.info(f"indexed {nr_services} services")
    for prot in protocol_list:
        nr_services_prot = len([x for x in services if x.service_protocol == prot])
        logging.info(f"indexed {prot} {nr_services_prot} services")

def convert_snake_to_camelcase(snake_str):
    first, *others = snake_str.split('_')
    return ''.join([first.lower(), *map(str.title, others)])

def replace_keys(dictionary: dict, fun) -> dict:
    empty = {}
    # special case when it is called for element of array being NOT a dictionary
    if type(dictionary) == str:
        # nothing to do
        return dictionary
    for k, v in dictionary.items():
        if type(v) == dict:
            empty[fun(k)] = replace_keys(v, fun)
        elif type(v) == list:
            newvalues = [replace_keys(x, fun) for x in v]
            empty[fun(k)] = newvalues
        else:
            empty[fun(k)] = v
    return empty



def main_services(args):
    output_file = args.output_file
    number_records = args.number
    pretty = args.pretty
    retrieve_dataset_metadata = args.dataset_md
    protocols = args.protocols
    show_warnings = args.show_warnings

    protocol_list = PROTOCOLS
    if protocols:
        protocol_list = protocols.split(",")

    if not show_warnings:
        cm = warnings.catch_warnings()
        warnings.simplefilter("ignore")
    else:
        cm = nullcontext()
    with cm:
        list_records = get_csw_list_result(protocol_list, number_records)

        services = get_services(list_records)

        if retrieve_dataset_metadata:
            dataset_ids = list(set([x.dataset_metadata_id for x in services]))
            datasets = get_datasets(dataset_ids)
            
            datasets_dict = [asdict(x) for x in datasets]
            services_dict = [asdict(x) for x in services]

            datasets_services = {
                "datasets": [
                    {
                        **x,
                        "services": [
                            y
                            for y in services_dict
                            if y["dataset_metadata_id"] == x["metadata_id"]
                        ],
                    }
                    for x in datasets_dict
                ]
            }
            for ds in datasets_services["datasets"]:
                for svc in ds["services"]:
                    del svc[
                        "dataset_metadata_id"
                    ]  # del redundant dataset_metadata_id key from service
            
            
            datasets_services_camel = replace_keys(datasets_services, convert_snake_to_camelcase)
        report_services_summary(services, protocol_list)

        with open(output_file, "w") as f:
            indent = None
            if pretty:
                indent = 4
            json.dump(datasets_services_camel, f, indent=indent)
        logging.info(f"output written to {output_file}")


def filter_service_records(records:list[CswServiceRecord]) -> list[CswServiceRecord]:
    records = filter(
        lambda x: x.service_url != "", records
    )  # filter out results without serviceurl
    # delete duplicate service entries, some service endpoint have multiple service records
    # so last record in get_record_results will be retained in case of duplicate
    # since it will be inserted in new_dict last
    new_dict = {x.service_url:x for x in records}
    return [value for _, value in new_dict.items()]


def main_layers(args):
    output_file = args.output_file
    number_records = args.number
    sort = args.sort
    pretty = args.pretty
    mode = args.mode
    protocols = args.protocols
    identifier = args.id
    show_warnings = args.show_warnings

    protocol_list = PROTOCOLS
    if protocols:
        protocol_list = protocols.split(",")

    if not show_warnings:
        cm = warnings.catch_warnings()
        warnings.simplefilter("ignore")
    else:
        cm = nullcontext()

    with cm:
        if identifier:
            service_ids = get_csw_results_by_id(identifier)
        else:
            service_ids = get_csw_list_result(protocol_list, number_records)

        service_records = get_services(service_ids)
        capabilities_docs: list[Service] = get_capabilities_docs(service_records)
        failed_services = list(filter(lambda x: x is None, capabilities_docs))
        capabilities_docs = list(
            filter(lambda x: x is not None, capabilities_docs)
        )  # filter out services where getcap req failed

        if mode == LayersMode.Services:
            config = [asdict(x) for x in list(capabilities_docs)]
            config_camel = [replace_keys(x, convert_snake_to_camelcase) for x in config]
        elif mode == LayersMode.Datasets:
            dataset_ids = list(
                set([x.dataset_metadata_id for x in capabilities_docs])
            )
            datasets = get_datasets(dataset_ids)

            datasets_dict = [asdict(x) for x in datasets]
            services_dict = [asdict(x) for x in capabilities_docs]


            datasets_services = {
                "datasets": [
                    {
                        **x,
                        "services": [
                            y
                            for y in services_dict
                            if y["dataset_metadata_id"] == x["metadata_id"]
                        ],
                    }
                    for x in datasets_dict
                ]
            }
            for ds in datasets_services["datasets"]:
                for svc in ds["services"]:
                    del svc[
                        "dataset_metadata_id"
                    ]  # del redundant dataset_metadata_id key from service
            config_camel = replace_keys(datasets_services, convert_snake_to_camelcase) 
        if mode == LayersMode.Flat:
            raise NotImplementedError
        
        # Fix old implementation below
        # if mode == LayersMode.Flat:
        #     layers = list(map(flatten_service, capabilities_docs))
        #     layers = [
        #         item for sublist in layers for item in sublist
        #     ]  # each services returns as a list of layers, flatten list, see https://stackoverflow.com/a/953097
        #     if sort:
        #         logging.info(f"sorting services")
        #         layers = sort_flat_layers(layers)
        #     config = layers
        # elif mode == LayersMode.Services:
        #     config = list(capabilities_docs)
        # elif mode == LayersMode.Datasets:
        #     dataset_ids = list(
        #         set([x["dataset_metadata_id"] for x in capabilities_docs])
        #     )
        #     datasets = get_datasets(dataset_ids)
        #     datasets_services = {
        #         "datasets": [
        #             {
        #                 **x,
        #                 "services": [
        #                     y
        #                     for y in capabilities_docs
        #                     if y["dataset_metadata_id"] == x["metadata_id"]
        #                 ],
        #             }
        #             for x in datasets
        #         ]
        #     }
        #     for ds in datasets_services["datasets"]:
        #         for svc in ds["services"]:
        #             del svc[
        #                 "dataset_metadata_id"
        #             ]  # del redundant dataset_metadata_id key from service
        #     config = datasets_services

        with open(output_file, "w") as f:
            if pretty:
                json.dump(config_camel, f, indent=4)
            else:
                json.dump(config_camel, f)

        # logging.info(f"indexed {len(services)} services with {len(layers)} layers")
        # if len(failed_services) > 0:
        #     failed_svc_urls = map(lambda x: x["url"], failed_services)
        #     logging.info(f"failed to index {len(failed_services)} services")
        #     failed_svc_urls_str = "\n".join(failed_svc_urls)
        #     logging.info(f"failed service urls:\n{failed_svc_urls_str}")
        logging.info(f"output written to {output_file}")


class LayersMode(enum.Enum):
    Flat = "flat"
    Services = "services"
    Datasets = "datasets"

    def __str__(self):
        return self.value


def main():
    parser = argparse.ArgumentParser(
        description="Generate list of PDOK services and/or service layers"
    )
    parser.set_defaults(func=lambda args: parser.print_help())

    parent_parser = argparse.ArgumentParser(add_help=False)

    parent_parser.add_argument(
        "-n",
        "--number",
        action="store",
        type=int,
        default=0,
        help="limit nr of records to retrieve per service type",
    )

    parent_parser.add_argument(
        "output_file", metavar="output-file", type=str, help="JSON output file"
    )

    # TODO: validate protocols input, should comma-separated list of following vals: OGC:WMS,OGC:WMTS,OGC:WFS,OGC:WCS,Inspire Atom
    parent_parser.add_argument(
        "-p",
        "--protocols",
        action="store",
        type=str,
        default="",
        help=f'service protocols (types) to query, comma-separated, values: {", ".join(PROTOCOLS)}',
    )

    parent_parser.add_argument(
        "--pretty", dest="pretty", action="store_true", help="pretty JSON output"
    )
    parent_parser.add_argument(
        "--warnings",
        dest="show_warnings",
        action="store_true",
        help="show user warnings - owslib tends to show warnings about capabilities",
    )

    subparsers = parser.add_subparsers()
    subparsers.metavar = "subcommand"
    services_parser = subparsers.add_parser(
        "services", parents=[parent_parser], help="Generate list of all PDOK services"
    )

    services_parser.add_argument(
        "--dataset-md",
        action="store_true",
        help="group services/layers by dataset and retrieve dataset metadata",
    )

    layers_parser = subparsers.add_parser(
        "layers", parents=[parent_parser], help="Generate list of all PDOK layers"
    )

    layers_parser.add_argument(
        "-m",
        "--mode",
        type=LayersMode,
        choices=list(LayersMode),
        default=LayersMode.Services,
    )

    layers_parser.add_argument(
        "-i",
        "--id",
        action="store",
        type=str,
        default="",
        help="only process specific service (by service metadata identifier)",
    )

    layers_parser.add_argument(
        "--sort",
        action="store_true",
        help="sort service layers based on default sorting rules for  pdokservicesplugin",
    )

    layers_parser.set_defaults(func=main_layers)
    services_parser.set_defaults(func=main_services)

    args = parser.parse_args()
    if args.func:
        args.func(args)


if __name__ == "__main__":
    main()
