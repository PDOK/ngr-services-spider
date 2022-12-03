#!/usr/bin/env python3
import argparse
import logging
import os
import warnings
from contextlib import nullcontext

from ngr_spider.constants import (
    ATOM_PROTOCOL,
    LOG_LEVEL,
    PROTOCOLS,
    WCS_PROTOCOL,
    WFS_PROTOCOL,
    WMS_PROTOCOL,
    WMTS_PROTOCOL
)
from ngr_spider.decorators import asdict_minus_none
from ngr_spider.util import (  # type: ignore
    convert_snake_to_camelcase,
    flatten_service,
    get_csw_datasets,
    get_csw_list_result,
    get_csw_results_by_id,
    get_csw_services,
    get_output,
    get_services,
    replace_keys,
    report_services_summary,
    sort_flat_layers,
    write_output
)

from .models import AtomService, LayersMode, Service, ServiceError

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s: %(message)s",
)


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

    protocol_list = PROTOCOLS
    if protocols:
        protocol_list = protocols.split(",")

    if not show_warnings:
        cm = warnings.catch_warnings()
        warnings.simplefilter("ignore")
    else:
        cm = nullcontext()
    with cm:
        list_records = get_csw_list_result(protocol_list, svc_owner, number_records)

        services = get_csw_services(list_records)
        services_dict = [asdict_minus_none(x) for x in services]

        if retrieve_dataset_metadata:
            dataset_ids = list(set([x.dataset_metadata_id for x in services]))
            datasets = get_csw_datasets(dataset_ids)

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
        content = get_output(pretty, yaml_output, config, no_updated)
        write_output(output_file, az_conn_string, az_container, yaml_output, content)

        logging.info(f"output written to {output_file}")


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
            service_ids = get_csw_list_result(protocol_list, svc_owner, number_records)

        service_records = get_csw_services(service_ids)
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
            datasets = get_csw_datasets(dataset_ids)

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
                logging.info(f"sorting services")
                layers = sort_flat_layers(layers, sort)

            config = {"layers": layers}
            if not snake_case:
                config = [replace_keys(x, convert_snake_to_camelcase) for x in layers]

        content = get_output(pretty, yaml_output, config, no_updated)
        write_output(output_file, az_conn_string, az_container, yaml_output, content)
        lookup = {
            WMTS_PROTOCOL: "layers",
            WMS_PROTOCOL: "layers",
            WFS_PROTOCOL: "featuretypes",
            WCS_PROTOCOL: "coverages",
            ATOM_PROTOCOL: "datasets",
        }  # TODO: move to constants.py
        total_nr_layers = sum(
            map(lambda x: len(x[lookup[x["protocol"]]]), succesful_services_dict)
        )
        logging.info(
            f"indexed {len(succesful_services_dict)} services with {total_nr_layers} layers/featuretypes/coverages"
        )
        if len(service_errors) > 0:
            service_errors_string = [f"{x.metadata_id}:{x.url}" for x in service_errors]
            logging.info(f"failed to index {len(service_errors)} services")
            message = "\n".join(service_errors_string)
            logging.info(f"failed service urls:\n{message}")
        logging.info(f"output written to {output_file}")


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
        "--no-updated",
        dest="snake_case",
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
