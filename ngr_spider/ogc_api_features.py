import logging
import requests

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
        url = requests.get(href)
        self.json = url.json()

    def get_info(self):
        return Info(self.json["info"])

    def get_tags(self):
        return self.json.get('tags', []) or []

    def get_servers(self):
        return self.json["servers"]

    def get_dataset_metadata_id(self):
        return ""

    def get_output_format(self):
        return ""

    def _get_url_from_servers(self, servers: list[str]):
        for server in servers:
            if len(server["url"]) > 0:
                return server["url"]


class Data:
    def __init__(self, href: str):
        url = requests.get(href)
        self.json = url.json()

class OGCApiFeatures:
    service_url: str
    service_type: str

    service_desc: ServiceDesc
    data: Data

    title: str
    description: str

    def __init__(self, url):
        self.service_url = url
        self._load_landing_page(url)

    # TODO Get correct info for featuretypes info when available
    def get_featuretypes(self):
        service_layer_name: str = "service_layer_name"
        service_layer_title: str = "service_layer_title"
        service_layer_abstract: str = "service_layer_abstract"
        service_layer_metadata_id: str = "service_layer_metadata_id"

        return [
            Layer(
                service_layer_name,
                service_layer_title,
                service_layer_abstract,
                service_layer_metadata_id,
            )
        ]

    def _load_landing_page(self, service_url: str):
        response = requests.get(service_url)
        response_body_data = response.json()

        links = response_body_data["links"]
        for link in links:
            if link["rel"] == "service-desc":
                self.service_desc = ServiceDesc(link["href"])
            elif link["rel"] == "data":
                self.data = Data(link["href"])
        self.title = response_body_data["title"] or ""
        self.description = response_body_data["description"] or ""
