import logging
import requests

from .models import Layer

LOGGER = logging.getLogger(__name__)


class Collection:
    id: str
    title: str
    description: str
    crs: str

    def __init__(self, data: dict):
        self.id = data["id"]
        self.title = data["title"]
        self.description = data["description"]
        self.crs = data.get("extent", {}).get("spatial", {}).get("crs", "")


class Info:
    description: str
    title: str
    version: str

    def __init__(self, data: dict):
        self.description = data["description"]
        self.title = data["title"]
        self.version = data["version"]


class ServiceDesc:
    def __init__(self, href: str):
        url = requests.get(href)
        self.json = url.json()

    def get_info(self):
        return Info(self.json["info"])

    def get_tags(self):
        return self.json.get("tags", []) or []

    def get_servers(self):
        return self.json["servers"]

    def _get_url_from_servers(self, servers: list[str]):
        for server in servers:
            if len(server["url"]) > 0:
                return server["url"]


class Data:
    def __init__(self, href: str):
        url = requests.get(href)
        self.json = url.json()

    def get_collections(self):
        collection_list = []
        for collection in self.json["collections"]:
            collection_list.append(Collection(collection))
        return collection_list


class OGCApiFeatures:
    service_url: str

    service_desc: ServiceDesc
    data: Data

    title: str
    description: str

    def __init__(self, url):
        self.service_url = url
        self._load_landing_page(url)

    # TODO Get correct info for featuretypes info when available
    def get_featuretypes(self):
        featuretypes = []
        for featuretype in self.data.get_collections():
            collection_name: str = featuretype["id"]
            collection_title: str = featuretype["title"]
            collection_abstract: str = featuretype["description"]
            featuretypes.append(
                Layer(
                    collection_name,
                    collection_title,
                    collection_abstract,
                    "",
                )
            )
        return featuretypes

    def _load_landing_page(self, service_url: str):
        response = requests.get(service_url)
        response_body_data = response.json()

        links = response_body_data["links"]
        for link in links:
            if link["rel"] == "service-desc":
                self.service_desc = ServiceDesc(link["href"])
            elif link["rel"].endswith("data"):
                self.data = Data(link["href"])
        self.title = response_body_data["title"] or ""
        self.description = response_body_data["description"] or ""
