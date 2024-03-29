#!/usr/bin/env python3
import argparse
import logging
import os
import warnings
from contextlib import nullcontext

from ngr_spider.constants import CSW_URL, PROTOCOL_LOOKUP, PROTOCOLS
from ngr_spider.csw_client import CSWClient
from ngr_spider.decorators import asdict_minus_none
from ngr_spider.util import (  # type: ignore
    convert_snake_to_camelcase,
    flatten_service,
    get_csw_datasets,
    get_output,
    get_services,
    replace_keys,
    report_services_summary,
    sort_flat_layers,
    validate_protocol_argument,
    write_output
)

from .models import AtomService, LayersMode, LogLevel, Service, ServiceError

# Typically make globals in CAPS to avoid name space problems.. per PEP-8
LOGGER = None


def setup_logger(loglevel: LogLevel):
    global LOGGER
    if loglevel == LogLevel.Error:
        logging_loglevel = logging.ERROR
    elif loglevel == LogLevel.Info:
        logging_loglevel = logging.INFO
    elif loglevel == LogLevel.Debug:
        logging_loglevel = logging.DEBUG

    LOGGER = logging.getLogger(__package__)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging_loglevel)


def main_services(args):
    output_file = args.output_file
    number_records = args.number
    pretty = args.pretty
    retrieve_dataset_metadata = args.dataset_md
    protocols = args.protocols
    show_warnings = args.show_warnings
    svc_owner = args.service_owner
    az_conn_string = args.azure_storage_connection_string
    az_container = args.azure_storage_container
    yaml_output = args.yaml
    no_updated = args.no_updated
    jq_filter = args.jq_filter
    log_level = args.log_level
    no_filter = args.no_filter
    csw_url = args.csw_url
    setup_logger(log_level)
    protocol_list = PROTOCOLS

    csw_client = CSWClient(csw_url)

    if protocols:
        protocol_list = protocols.split(",")

    LOGGER.info("main_services start.")
    if not show_warnings:
        cm = warnings.catch_warnings()
        warnings.simplefilter("ignore")
    else:
        cm = nullcontext()
    with cm:
        services = csw_client.get_csw_records_by_protocols(
            protocol_list, svc_owner, number_records, no_filter
        )  # TODO: refactor to match implementation here with in main_layers(), so no-filter can also be used on layers

        services_dict = [asdict_minus_none(x) for x in services]

        if retrieve_dataset_metadata:
            dataset_ids = list(set([x.dataset_metadata_id for x in services]))
            datasets = get_csw_datasets(csw_client, dataset_ids)

            datasets_dict = [asdict_minus_none(x) for x in datasets]

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

            config = replace_keys(datasets_services, convert_snake_to_camelcase)
        else:
            config = {
                "services": [
                    replace_keys(x, convert_snake_to_camelcase) for x in services_dict
                ]
            }

        report_services_summary(services, protocol_list)
        content = get_output(pretty, yaml_output, config, no_updated, jq_filter)
        write_output(output_file, az_conn_string, az_container, yaml_output, content)

        LOGGER.info("main_services end.")

        LOGGER.info(f"output written to {output_file}")


def main_layers(args):
    output_file = args.output_file
    number_records = args.number
    sort = args.sort
    pretty = args.pretty
    mode = args.mode
    protocols = args.protocols
    identifier = args.id
    show_warnings = args.show_warnings
    snake_case = args.snake_case
    yaml_output = args.yaml
    svc_owner = args.service_owner
    az_conn_string = args.azure_storage_connection_string
    az_container = args.azure_storage_container
    no_updated = args.no_updated
    jq_filter = args.jq_filter
    log_level = args.log_level
    csw_url = args.csw_url
    setup_logger(log_level)
    protocol_list = PROTOCOLS

    LOGGER.info("main_layers start.")

    csw_client = CSWClient(csw_url)

    if protocols:
        protocol_list = protocols.split(",")

    if not show_warnings:
        cm = warnings.catch_warnings()
        warnings.simplefilter("ignore")
    else:
        cm = nullcontext()

    with cm:
        if identifier:
            service_records = csw_client.get_csw_record_by_id(identifier)
        else:
            service_records = csw_client.get_csw_records_by_protocols(
                protocol_list, svc_owner, number_records
            )

        services = get_services(service_records)

        service_errors: list[ServiceError] = [
            x for x in services if type(x) is ServiceError
        ]
        succesful_services: list[Service] = [
            x for x in services if issubclass(type(x), Service)
        ]

        if mode == LayersMode.Services:
            succesful_services_dict = [
                asdict_minus_none(x) for x in list(succesful_services)
            ]
            if not snake_case:
                config = {
                    "services": [
                        replace_keys(x, convert_snake_to_camelcase)
                        for x in succesful_services_dict
                    ]
                }

        elif mode == LayersMode.Datasets:
            if AtomService in list(
                map(lambda x: type(x), succesful_services)
            ):  # TODO: move this check to arg parse function
                raise NotImplementedError(
                    "Grouping Atom services by datasets has not been implemented (yet)."
                )

            dataset_ids = list(set([x.dataset_metadata_id for x in succesful_services]))
            datasets = get_csw_datasets(csw_client, dataset_ids)
            datasets_dict = [asdict_minus_none(x) for x in datasets]
            succesful_services_dict = [asdict_minus_none(x) for x in succesful_services]

            datasets_services = {
                "datasets": [
                    {
                        **x,
                        "services": [
                            y
                            for y in succesful_services_dict
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
            config = datasets_services
            if not snake_case:
                config = replace_keys(datasets_services, convert_snake_to_camelcase)
        elif mode == LayersMode.Flat:
            succesful_services_dict = [asdict_minus_none(x) for x in succesful_services]
            layers = list(map(flatten_service, succesful_services_dict))
            layers = [
                item for sublist in layers for item in sublist
            ]  # each services returns as a list of layers, flatten list, see https://stackoverflow.com/a/953097
            if sort:
                LOGGER.info(f"sorting services")
                layers = sort_flat_layers(layers, sort)

            if not snake_case:
                layers = [replace_keys(x, convert_snake_to_camelcase) for x in layers]
            config = {"layers": layers}

        content = get_output(pretty, yaml_output, config, no_updated, jq_filter)
        write_output(output_file, az_conn_string, az_container, yaml_output, content)
        total_nr_layers = sum(
            map(lambda x: len(x[PROTOCOL_LOOKUP[x["protocol"]]]), succesful_services_dict)
        )
        LOGGER.info(
            f"indexed {len(succesful_services_dict)} services with {total_nr_layers} layers/featuretypes/coverages"
        )
        if len(service_errors) > 0:
            service_errors_string = [f"{x.metadata_id}:{x.url}" for x in service_errors]
            LOGGER.info(f"failed to index {len(service_errors)} services")
            message = "\n".join(service_errors_string)
            LOGGER.info(f"failed service urls:\n{message}")

        LOGGER.info("main_layers end.")
        LOGGER.info(f"output written to {output_file}")


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

    # validate protocols input, should comma-separated list of following vals: 'OGC:WMS,OGC:WMTS,OGC:WFS,OGC:WCS,Inspire Atom,OGC:API tiles,OGC:API features'
    parent_parser.add_argument(
        "-p",
        "--protocols",
        action="store",
        type=validate_protocol_argument,
        default="",
        help=f'service protocols (types) to query, comma-separated, values: {", ".join(PROTOCOLS)}',
    )

    parent_parser.add_argument(
        "-c",
        "--csw-url",
        action="store",
        type=str,
        default=CSW_URL,
        help=f"CSW base url, defaults to `{CSW_URL}`",
    )

    parent_parser.add_argument(
        "--azure-storage-connection-string",
        action="store",
        type=str,
        default=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
    )

    parent_parser.add_argument(
        "--azure-storage-container",
        action="store",
        type=str,
        default=os.environ.get("AZURE_STORAGE_CONTAINER"),
    )

    parent_parser.add_argument(
        "--jq-filter",
        action="store",
        type=str,
        help=f"Apply JQ filter to output",
    )

    parent_parser.add_argument(
        "--service-owner",
        action="store",
        type=str,
        default="Beheer PDOK",
        help=f"Service Owner to query NGR for",
    )

    parent_parser.add_argument(
        "--snake-case",
        dest="snake_case",
        action="store_true",
        help="output snake_case attributes instead of camelCase",
    )

    parent_parser.add_argument(
        "-l",
        "--log-level",
        type=LogLevel,
        choices=list(LogLevel),
        default=LogLevel.Info,
    )

    parent_parser.add_argument(
        "--no-updated",
        action="store_true",
        help="do not add updated field to output file",
    )

    parent_parser.add_argument(
        "--yaml",
        dest="yaml",
        action="store_true",
        help="output YAML instead of JSON",
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

    services_parser.add_argument(
        "--no-filter",
        dest="no_filter",
        action="store_true",
        default=False,
        help="Do not filter out services records with duplicate or empty service URLS",
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
        "-s",
        "--sort",
        action="store",
        type=str,
        default="",
        help="filepath to sorting rules json document",
    )

    layers_parser.set_defaults(func=main_layers)
    services_parser.set_defaults(func=main_services)

    args = parser.parse_args()
    if args.func:
        args.func(args)


if __name__ == "__main__":
    main()
