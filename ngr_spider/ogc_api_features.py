import json
import logging
import urllib.request

from .models import Layer

LOGGER = logging.getLogger(__name__)


class Info:
    description: str
    title: str
    version: str

    def __init__(self, data: dict):
        self.description = data["description"]
        self.title = data["title"]
        self.version = data["version"]


# TODO Implement service to retrieve correct info
class ServiceDesc:
    def __init__(self, href: str):
        with urllib.request.urlopen(href) as url:
            self.json = json.load(url)

    def get_info(self):
        return Info(self.json["info"])

    def get_tags(self):
        return self.json["tags"] | []

    def get_servers(self):
        return self.json["servers"]

    def get_dataset_metadata_id(self):
        return ""

    def get_output_format(self):
        return ""

    def __get_url_from_servers(self, servers: list[str]):
        for server in servers:
            if len(server["url"]) > 0:
                return server["url"]


class Data:
    def __init__(self, href: str):
        with urllib.request.urlopen(href) as url:
            self.json = json.load(url)

#TODO implement class to retrieve correct info
class OGCApiFeatures:
    service_url: str
    service_type: str

    service_desc: ServiceDesc
    data: Data

    title: str
    description: str

    def __init__(self, url):
        self.service_url = url
        self.__load_landing_page(url)

    # TODO Get correct info for featuretypes info when available
    def get_featuretypes(self):
        service_layer_name: str = "service_layer_name"
        service_layer_title: str = "service_layer_title"
        service_layer_abstract: str = "service_layer_abstract"
        service_layer_metadata_id: str = "ervice_layer_metadata_id"

        return [
            Layer(
                service_layer_name,
                service_layer_title,
                service_layer_abstract,
                service_layer_metadata_id,
            )
        ]

    def __load_landing_page(self, service_url: str):
        with urllib.request.urlopen(service_url) as response:
            response_body = response.read().decode("utf-8")
            response_body_data = json.loads(response_body)

            links = response_body_data["links"]
            for link in links:
                if link["rel"] == "service-desc":
                    self.service_desc = ServiceDesc(link["href"])
                elif link["rel"] == "data":
                    self.data = Data(link["href"])
            title = response_body_data["title"]
            self.title = title if title else ""
            description = response_body_data["description"]
            self.description = description if description else ""
