import urllib.request
import json
import logging
from .models import VectorTileStyle, OatLayer

LOGGER = logging.getLogger(__name__)

class Info:
    description: str
    title: str
    version: str

    def __init__(self, data: []):
        self.description = data["description"]
        self.title = data["title"]
        self.version = data["version"]
        pass


class ServiceDesc:
    json: []
    def __init__(self, href: str):
        with urllib.request.urlopen(href) as url:
            self.json = json.load(url)

    def get_info(self):
        return Info(self.json["info"])

    def get_tags(self):
        return self.json["tags"]

    def get_servers(self):
        return self.json["servers"]

    def __get_url_from_servers(self, servers: []):
        for server in servers:
            if len(server["url"]) > 0:
                return server["url"]

    def get_tile_request_url(self):
        paths = self.json["paths"]
        for path in paths:
            if "{tileMatrixSetId}" in path and "{tileMatrix}" in path and "{tileRow}" in path and "{tileCol}" in path:
                return self.__get_url_from_servers(self.get_servers()) + path # kijken in de spec of service url altijd goed is

class Data:
    json: []
    def __init__(self, href: str):
        with urllib.request.urlopen(href) as url:
            self.json = json.load(url)


class Tiles:
    json: []
    def __init__(self, href: str):
        with urllib.request.urlopen(href) as url:
            self.json = json.load(url)


class TileMatrixSets:
    json: []
    def __init__(self, href: str):
        with urllib.request.urlopen(href) as url:
            self.json = json.load(url)


# TODO use async methods
class OGCApiTiles:
    service_url: str
    service_type: str

    service_desc: ServiceDesc
    data: Data
    tiles: Tiles
    tile_matrix_sets: TileMatrixSets

    def __init__(self, url):
        self.service_url = url
        self.__load_landing_page(url)

    # https://docs.mapbox.com/mapbox-gl-js/style-spec/sources/#vector
    def get_layers(self):
        service_layer_name: str # technical name ?
        service_layer_title: str = "" # display name ?
        service_layer_abstract: str # ?
        service_layer_crs: str = ""
        service_layer_min_scale: str = ""
        service_layer_max_scale: str = ""
        service_data_type: str

        # process styles
        vector_tile_styles: [VectorTileStyle] = self.get_styles()

        # process min/max resolution
        layer_tile_set_matrix = self.get_tile_matrix_sets()

        # process layers
        tiles_json = self.tiles.json

        service_layer_name = tiles_json["title"]
        service_layer_abstract = tiles_json["description"]

        tile_sets = tiles_json["tilesets"]
        for tile_set in tile_sets:
            layer_tile_matrix_set_id = tile_set["tileMatrixSetId"]
            if layer_tile_matrix_set_id in layer_tile_set_matrix:
                # TODO https://docs.kadaster.nl/ggc/ggs-ggc-library/algemeen/scale-set/
                service_layer_min_scale = layer_tile_set_matrix[layer_tile_matrix_set_id]
            service_layer_crs = tile_set["crs"]
            service_layer_title = tile_set["title"]
            t_links = tile_set["links"]
            for l in t_links: # href uit de
                if l["rel"] == "self":
                    with urllib.request.urlopen(l["href"]) as url:
                        tile = json.load(url)
                        service_layer_title = tile["title"]
                        # datatype vector op laag niveau, niet mogelijk in GGC component
                        self.service_type = tile["dataType"]

        return [OatLayer(service_layer_name,
                         service_layer_title,
                         service_layer_abstract,
                         "",
                         vector_tile_styles,
                         service_layer_crs,
                         service_layer_min_scale,
                         service_layer_max_scale)]

    def __load_landing_page(self, service_url: str):
        with urllib.request.urlopen(service_url) as url:
            links = json.load(url)["links"]
            for link in links:
                if link["rel"] == "service-desc":
                    self.service_desc = ServiceDesc(link["href"])
                elif link["rel"] == "data":
                    self.data = Data(link["href"])
                elif link["rel"] == "tiles":
                    self.tiles = Tiles(link["href"])
                elif link["rel"] == "tileMatrixSets":
                    self.tile_matrix_sets = TileMatrixSets(link["href"])

    def get_styles(self):
        styles: [VectorTileStyle] = []
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
            s = VectorTileStyle(style["title"], style_stylesheet) # checken of het vectortiles zijn
            if len(default_style_name) > 0:
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
