import json
import logging
import urllib.request

from .models import OatLayer, OatTileSet, OatTiles, VectorTileStyle

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

        # process layers
        tiles_json = self.tiles.json
        tiles_title = tiles_json["title"]
        tiles_abstract = tiles_json["description"]

        layer = OatLayer(
            tiles_title,
            tiles_title,
            tiles_abstract, "",
            self.get_styles(),
            self.get_tiles()
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
    
    def get_tiles(self):
        tiles: list[OatTiles] = []
        tiles_json = self.tiles.json
        tiles.append(
            OatTiles(
                title=tiles_json["title"],
                abstract=tiles_json["description"],
                tilesets=self.get_tilesets()
            )
        )
        return tiles

    def get_tilesets(self):
        tilesets: list[OatTileSet] = []
        tilesets_json = self.tiles.json["tilesets"]
        for tileset in tilesets_json:
            tileset_url = self.get_self_link(tileset['links'])
            tileset_max_zoomlevel = self.get_zoomlevel(tileset_url)
            tilesets.append(
                OatTileSet(
                    tileset_id = tileset["tileMatrixSetId"],
                    tileset_crs = tileset["crs"],
                    tileset_max_zoomlevel = tileset_max_zoomlevel
                )
            )
        return tilesets
    
    def get_self_link(self, links):
        for link in links:
            if link.get('rel') == 'self':
                return link.get('href')
        return links[0].get('href')
    
    def get_zoomlevel(self, tileset_url):
        with urllib.request.urlopen(tileset_url) as url:
            tileset_info = json.load(url)
        tile_matrix_limits = tileset_info.get('tileMatrixSetLimits', [])
        max_tile_matrix_zoom = max(
                    (int(limit.get('tileMatrix')) for limit in tile_matrix_limits),
                    default=None
                )
        return max_tile_matrix_zoom
