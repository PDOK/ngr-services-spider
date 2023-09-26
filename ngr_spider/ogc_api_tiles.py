import json
import logging
import urllib.request

from .models import OatLayer, OatTiles, VectorTileStyle

LOGGER = logging.getLogger(__name__)


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
        with urllib.request.urlopen(href) as url:
            self.json = json.load(url)

    def get_info(self):
        return Info(self.json["info"])

    def get_tags(self):
        return self.json["tags"]

    def get_servers(self):
        return self.json["servers"]

    def __get_url_from_servers(self, servers: list[str]):
        for server in servers:
            if len(server["url"]) > 0:
                return server["url"]

    def get_tile_request_url(self):
        paths = self.json["paths"]
        for path in paths:
            if (
                "{tileMatrixSetId}" in path
                and "{tileMatrix}" in path
                and "{tileRow}" in path
                and "{tileCol}" in path
            ):
                return (
                    self.__get_url_from_servers(self.get_servers()) + path
                )  # kijken in de spec of service url altijd goed is


class Data:
    def __init__(self, href: str):
        with urllib.request.urlopen(href) as url:
            self.json = json.load(url)


class Tiles:
    def __init__(self, href: str):
        with urllib.request.urlopen(href) as url:
            self.json = json.load(url)


class TileMatrixSets:
    def __init__(self, href: str):
        with urllib.request.urlopen(href) as url:
            self.json = json.load(url)


# TODO use async methods
class OGCApiTiles:
    service_url: str
    service_type: str

    service_desc: ServiceDesc
    data: Data
    tiles: OatTiles
    tile_matrix_sets: TileMatrixSets

    title: str
    description: str

    def __init__(self, url):
        self.service_url = url
        self.__load_landing_page(url)

    def get_layers(self):
        tiles_title: str
        tiles_abstract: str
#We doen in de viewer nog niets met de verschillende tilesets
#         tileset_titles: str = ""
#         tileset_crs: str = ""
#         tileset_min_scale: str = ""
#         tileset_max_scale: str = ""
#         tileset_data_type: str

        # process layers
        tiles_json = self.tiles.json
        tiles_title = tiles_json["title"]
        tiles_abstract = tiles_json["description"]
#         tile_sets = tiles_json["tilesets"]
#         for tile_set in tile_sets:
#             t_links = tile_set["links"]
#             for l in t_links:
#                 if l["rel"] == "self":
#                     with urllib.request.urlopen(l["href"]) as url:
#                         tile = json.load(url)
#                         tileset_title = tile["title"]
#                         tileset_crs = tile["crs"]
#                         tileset_data_type: tile["dataType"]
        styles = self.get_styles()

        layer = OatLayer(
            tiles_title,
            tiles_title,
            tiles_abstract, "",
            self.get_styles()
            )
        return [layer]


    def __load_landing_page(self, service_url: str):
        with urllib.request.urlopen(service_url) as response:
            response_body = response.read().decode("utf-8")
            response_body_data = json.loads(response_body)
            links = response_body_data["links"]
            for link in links:
                if link["rel"] == "service-desc":
                    self.service_desc = ServiceDesc(link["href"])
                elif link["rel"] == "data" or link["rel"].endswith('styles'):
                    self.data = Data(link["href"])
                elif link["rel"] == "tiles" or link["rel"].endswith('tilesets-vector'):
                    self.tiles = Tiles(link["href"])
                elif link["rel"] == "tileMatrixSets" or link["rel"].endswith('tiling-schemes'):
                    self.tile_matrix_sets = TileMatrixSets(link["href"])
            title = response_body_data["title"]
            self.title = title if title else ""
            description = response_body_data["description"]
            self.description = description if description else ""

    def get_styles(self):
        styles: list[VectorTileStyle] = []
        data = self.data.json
        default_style_name: str = ""
        if data["default"] is not None:
            default_style_name = data["default"]
        for style in data["styles"]:
            style_stylesheet = ""
            for link in style["links"]:
                sr = link["rel"]
                if sr == "stylesheet":
                    style_stylesheet = link["href"]
            s = VectorTileStyle(style["id"], style["title"], style_stylesheet)
            if len(default_style_name) > 0 and style["title"] == default_style_name:
                styles.insert(0, s)  # insert as first element if it is default
            else:
                styles.append(s)
        return styles


    def get_tile_matrix_sets(self):
        tile_matrix_sets = dict()
        matrix_sets = self.tile_matrix_sets.json
        for matrix_set in matrix_sets["tileMatrixSets"]:
            matrix_set_id = matrix_set["id"]
            tile_matrix_sets[matrix_set["id"]] = {}
            matrix_set_url: str
            for link in matrix_set["links"]:
                se = link["rel"]
                if se == "self":
                    with urllib.request.urlopen(link["href"]) as url:
                        matrix_set_meta = json.load(url)
                        for i in matrix_set_meta["tileMatrices"]:
                            tile_matrix_sets[matrix_set_id] = i["scaleDenominator"]

        return tile_matrix_sets
