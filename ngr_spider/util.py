#!/usr/bin/env python3
import asyncio
import datetime
import itertools
import json
import logging
import re
import sys
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
    WCS_PROTOCOL,
    WFS_PROTOCOL,
    WMS_PROTOCOL,
    WMTS_PROTOCOL,
)
from ngr_spider.csw_client import CSWClient

from .models import (
    AtomService,
    CswDatasetRecord,
    CswServiceRecord,
    Layer,
    Service,
    ServiceError,
    Style,
    WcsService,
    WfsService,
    WmsLayer,
    WmsService,
    WmtsLayer,
    WmtsService,
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
    return service


def empty_string_if_none(input_str):
    return input_str if input_str is not None else ""


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

    try:
        url = service_record.service_url
        md_id = service_record.metadata_id
        LOGGER.info(f"{md_id} - {url}")
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
            dataset_metadata_id=service_record.dataset_metadata_id,
        )
    except requests.exceptions.HTTPError as e:
        LOGGER.error(f"md_id: {md_id} - {e}")
    except Exception:
        message = f"exception while retrieving WCS cap for service md-identifier: {md_id}, url: {url}"
        LOGGER.exception(message)
    return ServiceError(service_record.service_url, service_record.metadata_id)


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

    try:
        url = service_record.service_url
        md_id = service_record.metadata_id
        LOGGER.info(f"{md_id} - {url}")
        wfs = WebFeatureService(url, version="2.0.0")
        getfeature_op = next(
            (x for x in wfs.operations if x.name == "GetFeature"), None
        )
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
    except requests.exceptions.HTTPError as e:
        LOGGER.error(f"md-identifier: {md_id} - {e}")
    except Exception:
        message = f"exception while retrieving WFS cap for service md-identifier: {md_id}, url: {url}"
        LOGGER.exception(message)
    return ServiceError(service_record.service_url, service_record.metadata_id)


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

    try:
        if "://secure" in service_record.service_url:
            # this is a secure layer not for the general public: ignore
            # TODO: apply filtering elswhere
            return ServiceError(service_record.service_url, service_record.metadata_id)
        LOGGER.info(f"{service_record.metadata_id} - {service_record.service_url}")
        wms = WebMapService(service_record.service_url, version="1.3.0")
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
    except requests.exceptions.HTTPError as e:
        LOGGER.error(f"md-identifier: {service_record.metadata_id} - {e}")
    except Exception:
        message = f"exception while retrieving WMS cap for service md-identifier: {service_record.metadata_id}, url: {service_record.service_url}"
        LOGGER.exception(message)
    return ServiceError(service_record.service_url, service_record.metadata_id)


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

    try:
        url = service_record.service_url
        md_id = service_record.metadata_id
        LOGGER.info(f"{md_id} - {url}")
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
            dataset_metadata_id=service_record.dataset_metadata_id,
        )

    except requests.exceptions.HTTPError as e:
        LOGGER.error(f"md-identifier: {md_id} - {e}")
    except Exception:
        message = f"unexpected error occured while retrieving cap doc, md-identifier {md_id}, url: {url}"
        LOGGER.exception(message)
    return ServiceError(service_record.service_url, service_record.metadata_id)


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

    def flatten_layer_wmts(layer):
        for field in service_fields_mapping:
            layer[f"service_{field}"] = service[field]
        return layer

    def flatten_layer(layer):
        fun_mapping = {
            WMS_PROTOCOL: flatten_layer_wms,
            WFS_PROTOCOL: flatten_featuretype_wfs,
            WCS_PROTOCOL: flatten_coverage_wcs,
            WMTS_PROTOCOL: flatten_layer_wmts,
        }
        return fun_mapping[protocol](layer)

    resource_type_mapping = {
        WMS_PROTOCOL: "layers",
        WFS_PROTOCOL: "featuretypes",
        WCS_PROTOCOL: "coverages",
        WMTS_PROTOCOL: "layers",
    }
    protocol = service["protocol"]

    if protocol == "INSPIRE Atom":
        raise NotImplementedError(  # TODO: move check to argument parse function
            "Flat output for INSPIRE Atom services has not been implemented (yet)."
        )

    result = list(map(flatten_layer, service[resource_type_mapping[protocol]]))
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
