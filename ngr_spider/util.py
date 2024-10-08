#!/usr/bin/env python3
import argparse
import asyncio
import datetime
import itertools
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from types import MethodType
from typing import Union
from urllib import parse

import jq
import requests
import yaml
from azure.storage.blob import BlobClient, ContentSettings
from owslib.wcs import WebCoverageService, wcs110  # type: ignore
from owslib.wfs import WebFeatureService  # type: ignore
from owslib.wms import WebMapService  # type: ignore
from owslib.wmts import WebMapTileService

from ngr_spider.constants import (  # type: ignore
    ATOM_PROTOCOL,
    PROTOCOL_LOOKUP,
    OAF_PROTOCOL,
    OAT_PROTOCOL,
    PROTOCOLS,
    WCS_PROTOCOL,
    WFS_PROTOCOL,
    WMS_PROTOCOL,
    WMTS_PROTOCOL
)
from ngr_spider.csw_client import CSWClient
from ngr_spider.ogc_api_features import OGCApiFeatures
from ngr_spider.ogc_api_tiles import OGCApiTiles

from .models import (
    AtomService,
    CswDatasetRecord,
    CswServiceRecord,
    Layer,
    OafService,
    OatService,
    Service,
    ServiceError,
    Style,
    WcsService,
    WfsService,
    WmsLayer,
    WmsService,
    WmtsLayer,
    WmtsService
)

LOGGER = logging.getLogger(__name__)


def get_output(
    pretty,
    yaml_output,
    config: dict[str, Union[str, list[dict]]],
    no_timestamp,
    jq_filter,
):
    if not no_timestamp:
        timestamp = (
            datetime.datetime.now().astimezone().replace(microsecond=0).isoformat()
        )
        config["updated"] = timestamp

    if jq_filter:
        transformed_config_text = jq.compile(jq_filter).input(config).text()
        config = json.loads(transformed_config_text)
    if yaml_output:
        content = yaml.dump(config, default_flow_style=False)
    else:
        if pretty:
            content = json.dumps(config, indent=4)
        else:
            content = json.dumps(config)
    return content


def write_output(output_file, az_conn_string, az_container, yaml_output, content):
    if output_file == "-":
        sys.stdout.write(content)
    else:
        if az_conn_string and az_container:
            LOGGER.info(f"write result to Azure Blob Storage")
            blob = BlobClient.from_connection_string(
                conn_str=az_conn_string,
                container_name=az_container,
                blob_name=output_file,
            )
            content_type = "application/json"
            if yaml_output:
                content_type = "application/yaml"
            content_settings = ContentSettings(content_type=content_type)
            blob.upload_blob(
                content.encode("utf-8"),
                content_settings=content_settings,
                overwrite=True,
            )
        else:
            LOGGER.info(f"write result to local file system")
            with open(output_file, "w") as f:
                f.write(content)


def get_sorting_value(layer, rules):
    if not "name" in layer:
        return 101
    layer_name = layer["name"].lower()
    for rule in rules:
        if layer["service_protocol"] in rule["types"]:
            for name in rule["names"]:
                if re.search(name, layer_name) is not None:
                    return rule["index"]
    if layer["service_protocol"] == WMTS_PROTOCOL:
        return 99  # other wmts layers
    else:
        return 100  # all other layers


def join_lists_by_property(list_1, list_2, prop_name):
    lst = sorted(itertools.chain(list_1, list_2), key=lambda x: x[prop_name])
    result = []
    for k, v in itertools.groupby(lst, key=lambda x: x[prop_name]):
        d = {}
        for dct in v:
            d.update(dct)
        result.append(d)
    return result


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


def get_service(service_record: CswServiceRecord) -> Union[Service, ServiceError]:
    protocol = service_record.service_protocol
    if protocol == WMS_PROTOCOL:
        service = get_wms_service(service_record)
    elif protocol == WFS_PROTOCOL:
        service = get_wfs_service(service_record)
    elif protocol == WCS_PROTOCOL:
        service = get_wcs_service(service_record)
    elif protocol == WMTS_PROTOCOL:
        service = get_wmts_service(service_record)
    elif protocol == ATOM_PROTOCOL:
        service = get_atom_service(service_record)
    elif protocol == OAT_PROTOCOL:
        service = get_oat_service(service_record)
    elif protocol == OAF_PROTOCOL:
        service = get_oaf_service(service_record)
    return service


def empty_string_if_none(input_str):
    return input_str if input_str is not None else ""

def retrieve_generic_service(service_record, service_type: str):
    found = False
    retries = 3
    message = ""
    LOGGER.info(f"{service_record.metadata_id} - {service_record.service_url}")
    while not found and retries > 0:
        service_temp = None
        try:
            if service_type == "wms":
                service_temp = WebMapService(service_record.service_url, version="1.3.0")
            elif service_type == "wfs":
                service_temp = WebFeatureService(service_record.service_url, version="2.0.0")
            elif service_type == "wcs":
                service_temp = WebCoverageService(service_record.service_url, version="1.1.0")
            elif service_type == "wmts":
                service_temp = WebMapTileService(service_record.service_url)
            elif service_type == "atom":
                pass
            elif service_type == "oat":
                service_temp = OGCApiTiles(service_record.service_url)
            elif service_type == "oaf":
                service_temp = OGCApiFeatures(service_record.service_url)
            else:
                pass
            # The constructor initiates the http(s) connection
            # If it doesn't except, we are good to go
            return service_temp, None
        except requests.exceptions.HTTPError as e:
            message = f"md-identifier: {service_record.metadata_id} - {e}"
        except Exception:
            message = f"exception while retrieving {service_type} cap for service md-identifier: {service_record.metadata_id}, url: {service_record.service_url}"
        time.sleep(5)
        retries -= 1
    return None, message

def get_wcs_service(
    service_record: CswServiceRecord,
) -> Union[WcsService, ServiceError]:
    def convert_layer(lyr) -> Layer:
        return Layer(
            title=empty_string_if_none(wcs[lyr].title),
            abstract=empty_string_if_none(wcs[lyr].abstract),
            name=wcs[lyr].id,
            dataset_metadata_id=service_record.dataset_metadata_id,
        )

    def OWS(cls, tag):
        return "{http://www.opengis.net/ows/1.1}" + tag

    wcs, message = retrieve_generic_service(service_record, "wcs")
    if wcs is None:
        LOGGER.error(message)
        return ServiceError(service_record.service_url, service_record.metadata_id)
    return WcsService(
        title=empty_string_if_none(wcs.identification.title),
        abstract=empty_string_if_none(wcs.identification.abstract),
        metadata_id=service_record.metadata_id,
        url=service_record.service_url,
        coverages=list(map(convert_layer, list(wcs.contents))),
        keywords=wcs.identification.keywords,
        dataset_metadata_id=service_record.dataset_metadata_id,
    )

def get_wfs_service(
    service_record: CswServiceRecord,
) -> Union[WfsService, ServiceError]:
    def convert_layer(lyr) -> Layer:
        return Layer(
            title=empty_string_if_none(wfs[lyr].title),
            abstract=empty_string_if_none(wfs[lyr].abstract),
            name=wfs[lyr].id,
            dataset_metadata_id=service_record.dataset_metadata_id,
        )

    wfs, message = retrieve_generic_service(service_record, "wfs")
    if wfs is None:
        LOGGER.error(message)
        return ServiceError(service_record.service_url, service_record.metadata_id)

    getfeature_op = next(
        (x for x in wfs.operations if x.name == "GetFeature"), None
    )
    md_id = service_record.metadata_id

    return WfsService(
        title=empty_string_if_none(wfs.identification.title),
        abstract=empty_string_if_none(wfs.identification.abstract),
        metadata_id=md_id,
        url=service_record.service_url,
        output_formats=getfeature_op.parameters["outputFormat"]["values"],  # type: ignore
        featuretypes=list(map(convert_layer, list(wfs.contents))),
        keywords=wfs.identification.keywords,
        dataset_metadata_id=service_record.dataset_metadata_id,
    )

def get_md_id_from_url(url):
    LOGGER.debug(f"get_md_id_from_url url: {url}")
    params = dict(parse.parse_qsl(parse.urlsplit(url).query))
    params = {k.lower(): v for k, v in params.items()}  # lowercase dict keys
    if "uuid" in params:
        return params["uuid"]
    else:
        return params["id"]


def get_atom_service(
    service_record: CswServiceRecord,
) -> Union[WmsService, ServiceError]:
    r = requests.get(service_record.service_url)
    return AtomService(service_record.service_url, r.text)


def get_oaf_service(
    service_record: CswServiceRecord,
) -> Union[OafService, ServiceError]:
    if "://secure" in service_record.service_url:
        # this is a secure layer not for the general public: ignore
        return service_record

    oaf, message = retrieve_generic_service(service_record, "oaf")
    if oaf is None:
        LOGGER.error(message)
        return ServiceError(service_record.service_url, service_record.metadata_id)

    title = oaf.title or oaf.service_desc.get_info().title or ""
    description = oaf.description or oaf.service_desc.get_info().description or ""
    md_id = service_record.metadata_id or ""
    ds_md_id = service_record.dataset_metadata_id or ""

    featuretypes=oaf.get_featuretypes(ds_md_id)
    for featuretype in featuretypes:
        featuretype.dataset_metadata_id = service_record.dataset_metadata_id or ""

    return OafService(
        title=title,
        abstract=description,
        metadata_id=md_id,
        url=service_record.service_url,
        featuretypes=oaf.get_featuretypes(ds_md_id),
        keywords=oaf.service_desc.get_tags(),
        dataset_metadata_id=ds_md_id,
    )


def get_oat_service(
    service_record: CswServiceRecord,
) -> Union[OatService, ServiceError]:
    if "://secure" in service_record.service_url:
        # this is a secure layer not for the general public: ignore
        return service_record

    oat, message = retrieve_generic_service(service_record, "oat")
    if oat is None:
        LOGGER.error(message)
        return ServiceError(service_record.service_url, service_record.metadata_id)

    title = oat.title or oat.service_desc.get_info().title or ""
    description = oat.description or oat.service_desc.get_info().description or ""

    layers = oat.get_layers()

    for layer in layers:
        layer.dataset_metadata_id = service_record.dataset_metadata_id

    service_url = oat.service_desc.get_tile_request_url()

    return OatService(
        # http://docs.ogc.org/DRAFTS/19-072.html#rc_landing-page-section
        title=title,
        abstract=description,
        metadata_id=service_record.metadata_id,
        url=service_url,
        layers=layers,
        keywords=oat.service_desc.get_tags(),
        dataset_metadata_id=service_record.dataset_metadata_id,
    )


def get_wms_service(
    service_record: CswServiceRecord,
) -> Union[WmsService, ServiceError]:
    def convert_layer(lyr) -> WmsLayer:
        styles: list[Style] = []
        for style_name in list(wms[lyr].styles.keys()):
            style_obj = wms[lyr].styles[style_name]
            title: str = ""
            if "title" in style_obj:
                title = style_obj["title"]
            legend: str = ""
            if "legend" in style_obj:
                legend = style_obj["legend"]
            style = Style(title=title, name=style_name, legend_url=legend)
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
            abstract=empty_string_if_none(wms[lyr].abstract),
            styles=styles,
            crs=",".join([x[4] for x in wms[lyr].crs_list]),
            minscale=minscale,
            maxscale=maxscale,
            dataset_metadata_id=dataset_md_id,
        )

    if "://secure" in service_record.service_url:
        # this is a secure layer not for the general public: ignore
        # TODO: apply filtering elswhere
        return ServiceError(service_record.service_url, service_record.metadata_id)

    wms, message = retrieve_generic_service(service_record, "wms")
    if wms is None:
        LOGGER.error(message)
        return ServiceError(service_record.service_url, service_record.metadata_id)

    getmap_op = next((x for x in wms.operations if x.name == "GetMap"), None)
    layers = list(wms.contents)
    return WmsService(
        title=empty_string_if_none(wms.identification.title),
        abstract=empty_string_if_none(wms.identification.abstract),
        keywords=wms.identification.keywords,
        layers=list(map(convert_layer, layers)),
        imgformats=",".join(getmap_op.formatOptions),  # type: ignore
        metadata_id=service_record.metadata_id,
        url=service_record.service_url,
        dataset_metadata_id=service_record.dataset_metadata_id,
    )

def get_wmts_service(
    service_record: CswServiceRecord,
) -> Union[WmtsService, ServiceError]:
    def convert_layer(lyr) -> WmtsLayer:
        styles: list[Style] = []
        for style_name in list(wmts[lyr].styles.keys()):
            style_obj = wmts[lyr].styles[style_name]
            title: str = ""
            if "title" in style_obj:
                title = style_obj["title"]
            legend: str = ""
            if "legend" in style_obj:
                legend = style_obj["legend"]
            style = Style(title=title, name=style_name, legend_url=legend)
            styles.append(style)
        return WmtsLayer(
            name=lyr,
            title=empty_string_if_none(wmts[lyr].title),
            abstract=empty_string_if_none(wmts[lyr].abstract),
            tilematrixsets=",".join(list(wmts[lyr].tilematrixsetlinks.keys())),
            imgformats=",".join(wmts[lyr].formats),
            styles=styles,
            dataset_metadata_id=service_record.dataset_metadata_id,
        )

    if "://secure" in service_record.service_url:
        # this is a secure layer not for the general public: ignore
        return service_record

    wmts, message = retrieve_generic_service(service_record, "wmts")
    if wmts is None:
        LOGGER.error(message)
        return ServiceError(service_record.service_url, service_record.metadata_id)

    return WmtsService(
        title=empty_string_if_none(wmts.identification.title),
        abstract=empty_string_if_none(wmts.identification.abstract),
        metadata_id=service_record.metadata_id,
        url=service_record.service_url,
        layers=list(map(convert_layer, list(wmts.contents))),
        keywords=wmts.identification.keywords,
        dataset_metadata_id=service_record.dataset_metadata_id,
    )


def flatten_service(service):
    service_fields_mapping = ["url", "title", "abstract", "protocol", "metadata_id"]

    def flatten_layer_wms(layer):
        layer["imgformats"] = service["imgformats"]
        for field in service_fields_mapping:
            layer[f"service_{field}"] = service[field]

        return layer

    def flatten_coverage_wcs(coverage):
        for field in service_fields_mapping:
            coverage[f"service_{field}"] = service[field]
        return coverage

    def flatten_featuretype_wfs(featuretype):
        for field in service_fields_mapping:
            featuretype[f"service_{field}"] = service[field]
        return featuretype

    def flatten_featuretype_oaf(featuretype):
        for field in service_fields_mapping:
            featuretype[f"service_{field}"] = service[field]
        return featuretype

    def flatten_layer_wmts(layer):
        for field in service_fields_mapping:
            layer[f"service_{field}"] = service[field]
        return layer

    def flatten_layer_oat(layer):
        for field in service_fields_mapping:
            layer[f"service_{field}"] = service[field]
        return layer

    def flatten_layer(layer):
        fun_mapping = {
            WMS_PROTOCOL: flatten_layer_wms,
            WFS_PROTOCOL: flatten_featuretype_wfs,
            WCS_PROTOCOL: flatten_coverage_wcs,
            WMTS_PROTOCOL: flatten_layer_wmts,
            OAT_PROTOCOL: flatten_layer_oat,
            OAF_PROTOCOL: flatten_featuretype_oaf,
        }
        return fun_mapping[protocol](layer)

    protocol = service["protocol"]

    if protocol == "INSPIRE Atom":
        raise NotImplementedError(  # TODO: move check to argument parse function
            "Flat output for INSPIRE Atom services has not been implemented (yet)."
        )

    result = list(map(flatten_layer, service[PROTOCOL_LOOKUP[protocol]]))
    return result


def sort_flat_layers(layers, rules_path):
    with open(rules_path, "r") as f:
        rules = json.load(f)
        sorted_layer_dict = {}
        for layer in layers:
            sorting_value = get_sorting_value(layer, rules)
            if sorting_value in sorted_layer_dict:
                sorted_layer_dict[sorting_value].append(layer)
            else:
                sorted_layer_dict[sorting_value] = [layer]
        result = []
        for key in sorted(sorted_layer_dict.keys()):
            if len(sorted_layer_dict[key]) == 0:
                rule = next(filter(lambda x: x["index"] == key, rules), None)
                LOGGER.info(f"no layers found for sorting rule: {rule}")
            result += sorted_layer_dict[key]
        return result


def get_services(
    service_records: list[CswServiceRecord],
) -> list[Union[Service, ServiceError]]:
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(service_records, get_service))
    loop.run_until_complete(future)
    services_list: list[Union[Service, ServiceError]] = future.result()
    return services_list


def get_csw_datasets(
    client: CSWClient, dataset_ids: list[str]
) -> list[CswDatasetRecord]:
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        get_data_asynchronous(dataset_ids, client.get_dataset_metadata)
    )
    loop.run_until_complete(future)
    datasets: list[CswDatasetRecord] = future.result()
    datasets = list(
        filter(None, datasets)
    )  # filter out empty datasets, happens when an expected dataset metadatarecords is not present in NGR
    return datasets


def report_services_summary(services: list[CswServiceRecord], protocol_list: list[str]):
    nr_services = len(services)
    LOGGER.info(f"indexed {nr_services} services")
    for prot in protocol_list:
        nr_services_prot = len([x for x in services if x.service_protocol == prot])
        LOGGER.info(f"indexed {prot} {nr_services_prot} services")


def convert_snake_to_camelcase(snake_str):
    first, *others = snake_str.split("_")
    return "".join([first.lower(), *map(str.title, others)])


def replace_keys(dictionary: dict, fun) -> dict:
    empty = {}
    # special case when it is called for element of array being NOT a dictionary
    if not dictionary or type(dictionary) == str:
        # nothing to do
        return dictionary
    for k, v in dictionary.items():
        if type(v) == dict:
            empty[fun(k)] = replace_keys(v, fun)
        elif type(v) == list:
            newvalues = [replace_keys(x, fun) for x in v]
            empty[fun(k)] = newvalues  # type: ignore
        else:
            empty[fun(k)] = v
    return empty


def validate_protocol_argument(value):
    protocols = value.split(",")
    for protocol in protocols:
        if protocol not in PROTOCOLS:
            raise argparse.ArgumentTypeError(f"Invalid protocol: {protocol}")
    return value
