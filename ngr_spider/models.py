import dataclasses
import enum
import logging
from typing import Optional, Tuple
from urllib import parse
from urllib.parse import parse_qs, urlparse

import requests
from dataclass_wizard import JSONWizard  # type: ignore
from lxml import etree  # type: ignore

from ngr_spider.constants import (  # type: ignore
    ATOM_PROTOCOL,
    WCS_PROTOCOL,
    WFS_PROTOCOL,
    WMS_PROTOCOL,
    WMTS_PROTOCOL
)
from ngr_spider.decorators import nested_dataclass


def get_query_param_val(url, param_name):
    try:
        parsed_url = parse.urlparse(url)
        return parse.parse_qs(parsed_url.query)[param_name][0]
    except (KeyError, IndexError):
        return ""


class LayersMode(enum.Enum):
    Flat = "flat"
    Services = "services"
    Datasets = "datasets"

    def __str__(self):
        return self.value


def ExcludeIfNone(value):
    """Do not include field for None values"""
    return value is None


@dataclasses.dataclass
class CswListRecord:
    title: str
    abstract: str
    type: str
    identifier: str
    keywords: list[str]
    modified: str


@dataclasses.dataclass
class WmsStyle:
    title: str
    name: str


@dataclasses.dataclass
class Layer:
    name: str
    title: str
    abstract: str
    dataset_metadata_id: str


@dataclasses.dataclass
class WmsLayer(Layer):
    styles: list[WmsStyle]
    crs: str
    minscale: str = ""
    maxscale: str = ""


@dataclasses.dataclass
class WmtsLayer(Layer):
    tilematrixsets: str
    imgformats: str


@dataclasses.dataclass
class ServiceError:
    url: str
    metadata_id: str


@dataclasses.dataclass
class Service(JSONWizard):
    title: str
    abstract: str
    metadata_id: str
    dataset_metadata_id: str
    url: str
    keywords: list[str]
    protocol: str


@dataclasses.dataclass
class Link:
    url: str
    type: str
    length: int
    title: str
    bbox: Optional[Tuple[float, float, float, float]]


@nested_dataclass
class Download:
    id: str
    title: str
    content: str
    updated: str
    links: list[Link]


@nested_dataclass
class DatasetFeed:
    id: str
    url: str
    title: str
    abstract: str
    updated: str
    rights: str
    dataset_source_id: str
    dataset_metadata_id: str
    downloads: list[Download]


def get_text_xpath(xpath_query, el, ns):
    try:
        return str(el.xpath(xpath_query, namespaces=ns)[0])
    except IndexError:
        return ""


@nested_dataclass
class AtomService(Service):
    datasets: list[Optional[DatasetFeed]]
    id: str
    url: str
    title: str
    abstract: str
    updated: str
    rights: str
    protocol: str

    _ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "georss": "http://www.georss.org/georss",
        "inspire_dls": "http://inspire.ec.europa.eu/schemas/inspire_dls/1.0",
    }

    def get_link(self, link_el) -> Link:

        url = get_text_xpath("@href", link_el, self._ns)
        type = get_text_xpath("@type", link_el, self._ns)
        length = get_text_xpath("@length", link_el, self._ns)
        title = get_text_xpath("@title", link_el, self._ns)
        bbox_str = get_text_xpath("@bboxy", link_el, self._ns)
        bbox: Optional[tuple[float, float, float, float]] = None
        if bbox_str:
            bbox_list = bbox_str.split(",")
            bbox = tuple(
                [
                    float(bbox_list[0]),
                    float(bbox_list[1]),
                    float(bbox_list[2]),
                    float(bbox_list[3]),
                ]  # type: ignore
            )
        return Link(url, type, length, title, bbox)

    def get_download(self, download_el) -> Download:
        dl_title = get_text_xpath("atom:title/text()", download_el, self._ns)
        dl_id = get_text_xpath("atom:id/text()", download_el, self._ns)
        dl_content = get_text_xpath("atom:content/text()", download_el, self._ns)
        dl_updated = get_text_xpath("atom:updated/text()", download_el, self._ns)
        link_els = download_el.xpath(
            "atom:link",
            namespaces=self._ns,
        )

        links: list[Link] = list(map(lambda x: self.get_link(x), link_els))

        link_urls = list(map(lambda x: x.url, links))
        duplicate_urls = list_duplicates(link_urls)

        for duplicate_url in duplicate_urls:
            links = list(
                filter(lambda x: x.url == duplicate_url and x.type != "", links)
            )

        # filter out duplicate links without type attribute, some sort of legacy convention of PDOK services...
        return Download(dl_id, dl_title, dl_content, dl_updated, links)

    def get_dataset_feed(self, entry) -> Optional[DatasetFeed]:
        ds_feed_url = get_text_xpath(
            "atom:link[@type='application/atom+xml']/@href", entry, self._ns
        )
        dataset_source_id = get_text_xpath(
            "inspire_dls:spatial_dataset_identifier_code/text()", entry, self._ns
        )
        dataset_metadata_url = get_text_xpath(
            "atom:link[@rel='describedby'][@type='application/xml']/@href",
            entry,
            self._ns,
        )
        if dataset_metadata_url == "":
            return None
        dataset_metadata_id = get_query_param_val(dataset_metadata_url, "id")

        r = requests.get(ds_feed_url)
        ds_root = etree.fromstring(r.content, parser=self._parser)
        id = get_text_xpath("/atom:feed/atom:id/text()", ds_root, self._ns)
        title = get_text_xpath("/atom:feed/atom:title/text()", ds_root, self._ns)
        abstract = get_text_xpath("/atom:feed/atom:subtitle/text()", ds_root, self._ns)
        updated = get_text_xpath("/atom:feed/atom:updated/text()", ds_root, self._ns)
        rights = get_text_xpath("/atom:feed/atom:rights/text()", ds_root, self._ns)
        download_els = ds_root.xpath("/atom:feed/atom:entry", namespaces=self._ns)

        downloads: list[Download] = []
        for download_el in download_els:
            dl = self.get_download(download_el)
            downloads.append(dl)
        return DatasetFeed(
            id,
            ds_feed_url,
            title,
            abstract,
            updated,
            rights,
            dataset_source_id,
            dataset_metadata_id,
            downloads,
        )

    def __init__(self, url, xml):
        self._parser = etree.XMLParser(ns_clean=True, recover=True, encoding="utf-8")
        self._root = etree.fromstring(xml.encode(), parser=self._parser)
        self.xml = xml
        self.url = url
        self.protocol = ATOM_PROTOCOL
        self.id = self.title = get_text_xpath(
            "/atom:feed/atom:id/text()", self._root, self._ns
        )
        self.title = get_text_xpath(
            "/atom:feed/atom:title/text()", self._root, self._ns
        )
        self.abstract = get_text_xpath(
            "/atom:feed/atom:subtitle/text()", self._root, self._ns
        )
        self.updated = get_text_xpath(
            "/atom:feed/atom:updated/text()", self._root, self._ns
        )
        self.rights = get_text_xpath(
            "/atom:feed/atom:rights/text()", self._root, self._ns
        )

        self.datasets = []
        entries = self._root.xpath(
            "/atom:feed/atom:entry",
            namespaces=self._ns,
        )
        datasets = []
        for entry in entries:
            ds = self.get_dataset_feed(entry)
            datasets.append(ds)

        self.datasets = list(filter(lambda x: x is not None, datasets))

        super(AtomService, self).__init__(
            self.title, self.abstract, "", "", url, [], self.protocol
        )


@dataclasses.dataclass(kw_only=True)
class WfsService(Service):
    featuretypes: list[Layer]
    output_formats: str
    protocol: str = WFS_PROTOCOL


@dataclasses.dataclass(kw_only=True)
class WcsService(Service):
    coverages: list[Layer]
    # formats: str # formats no supported for now, OWSLib does not seem to extract the formats correctly
    protocol: str = WCS_PROTOCOL


@dataclasses.dataclass(kw_only=True)
class WmsService(Service):
    imgformats: str
    layers: list[WmsLayer]
    protocol: str = WMS_PROTOCOL


@dataclasses.dataclass(kw_only=True)
class WmtsService(Service):
    layers: list[WmtsLayer]
    protocol: str = WMTS_PROTOCOL


@dataclasses.dataclass
class Dataset:
    title: str
    abstract: str
    metadata_id: str
    services: list[Service]


@dataclasses.dataclass
class CswDatasetRecord(JSONWizard):
    title: str
    abstract: str
    metadata_id: str


def list_duplicates(seq):
    seen = set()
    seen_add = seen.add
    # adds all elements it doesn't know yet to seen and all other to seen_twice
    seen_twice = set(x for x in seq if x in seen or seen_add(x))
    # turn the set into a list (as requested)
    return list(seen_twice)


@dataclasses.dataclass
class CswServiceRecord(JSONWizard):
    title: str
    abstract: str
    use_limitation: str
    keywords: list[str]
    operates_on: str
    metadata_id: str
    dataset_metadata_id: str
    service_url: str
    service_protocol: str
    service_description: str

    _ns = {
        "csw": "http://www.opengis.net/cat/csw/2.0.2",
        "gmd": "http://www.isotc211.org/2005/gmd",
        "dc": "http://purl.org/dc/elements/1.1/",
        "dct": "http://purl.org/dc/terms/",
        "srv": "http://www.isotc211.org/2005/srv",
        "xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "gmx": "http://www.isotc211.org/2005/gmx",
        "gco": "http://www.isotc211.org/2005/gco",
        "xlink": "http://www.w3.org/1999/xlink",
    }
    _xpath_sv_service_identification = (
        ".//gmd:identificationInfo/srv:SV_ServiceIdentification"
    )
    _xpath_ci_resource = ".//gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine"

    def __repr__(self):
        dict_repr = ", ".join(
            f"{k}={v!r}"
            for k, v in filter(
                lambda item: not item[0].startswith("_"), self.__dict__.items()
            )
        )
        return f"{self.__class__.__name__}({dict_repr})"

    def get_record_identifier(self):
        xpath_query = f".//gmd:fileIdentifier/gco:CharacterString/text()"
        return self.get_text_xpath(xpath_query)

    def get_use_limitation(self):
        xpath_query = f"{self._xpath_sv_service_identification}/gmd:resourceConstraints/gmd:MD_Constraints/gmd:useLimitation/gco:CharacterString/text()"
        return self.get_text_xpath(xpath_query)

    def get_text_xpath(self, xpath_query, el=None):
        if el is None:
            el = self.root
        try:
            return str(el.xpath(xpath_query, namespaces=self._ns)[0])
        except IndexError:
            return ""

    def get_title(self):
        xpath_query = f"{self._xpath_sv_service_identification}/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()"
        return self.get_text_xpath(xpath_query)

    def get_abstract(self):
        xpath_query = f"{self._xpath_sv_service_identification}/gmd:abstract/gco:CharacterString/text()"
        return self.get_text_xpath(xpath_query)

    def get_point_of_contact(self):
        return {}

    def get_keywords(self):
        xpath_query = f"{self._xpath_sv_service_identification}/gmd:descriptiveKeywords/gmd:MD_Keywords"
        md_keywords = self.root.xpath(xpath_query, namespaces=self._ns)
        keywords_result = {}
        for md_keyword in md_keywords:
            keywords_els = md_keyword.xpath("./gmd:keyword", namespaces=self._ns)
            for keyword_el in keywords_els:
                try:
                    keyword_val = str(
                        keyword_el.xpath(
                            "./gco:CharacterString/text()", namespaces=self._ns
                        )[0]
                    )
                    if "" not in keywords_result:
                        keywords_result[""] = []
                    keywords_result[""].append(keyword_val)

                except IndexError:
                    try:
                        keyword_val = str(
                            keyword_el.xpath(
                                "./gmx:Anchor/text()", namespaces=self._ns
                            )[0]
                        )
                        keyword_ns = str(
                            keyword_el.xpath(
                                "./gmx:Anchor/@xlink:href", namespaces=self._ns
                            )[0]
                        )
                        if keyword_ns not in keywords_result:
                            keywords_result[keyword_ns] = []
                        keywords_result[keyword_ns].append(keyword_val)
                    except IndexError:
                        logging.error(
                            f"unexpected error while retrieving keyword for record {self.metadata_id}"
                        )

        return keywords_result

    def get_operates_on(self):
        return self.get_text_xpath(
            f"{self._xpath_sv_service_identification}/srv:operatesOn/@xlink:href"
        )

    def get_dataset_record_identifier(self, operates_on_url):
        parsed_url = urlparse(operates_on_url.lower())
        try:
            return parse_qs(parsed_url.query)["id"][0]
        except IndexError:
            return ""

    def get_service_protocol(self, el):
        xpath_query = f"gmd:CI_OnlineResource/gmd:protocol/gmx:Anchor/text()"
        result = self.get_text_xpath(xpath_query, el)
        if result == "":
            xpath_query = (
                f"gmd:CI_OnlineResource/gmd:protocol/gco:CharacterString/text()"
            )
            result = self.get_text_xpath(xpath_query, el)
        return result

    def get_service_url(self, el):
        xpath_query = f"gmd:CI_OnlineResource/gmd:linkage/gmd:URL/text()"
        result = str(el.xpath(xpath_query, namespaces=self._ns)[0])
        return result

    def get_service_el(self):
        xpath_query = f"{self._xpath_ci_resource}"
        online_els = self.root.xpath(xpath_query, namespaces=self._ns)
        for el in online_els:
            protocol = self.get_service_protocol(el)
            if protocol.startswith("OGC:") or protocol == "INSPIRE Atom":
                return el
        return None

    def get_service_description(self, el):
        xpath_query = f"gmd:CI_OnlineResource/gmd:description/gmx:Anchor/text()"
        result = self.get_text_xpath(xpath_query, el)
        if result == "":
            xpath_query = (
                f"gmd:CI_OnlineResource/gmd:description/gco:CharacterString/text()"
            )
            result = self.get_text_xpath(xpath_query, el)

        return result

    def __init__(self, xml):
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding="utf-8")
        self.root = etree.fromstring(xml, parser=parser)
        self.metadata_id = self.get_record_identifier()
        self.title = self.get_title()
        self.abstract = self.get_abstract()
        self.use_limitation = self.get_use_limitation()
        # self.point_of_contact = self.get_point_of_contact()
        self.keywords = self.get_keywords()
        self.operates_on = self.get_operates_on()
        self.dataset_metadata_id = self.get_dataset_record_identifier(self.operates_on)
        service_el = self.get_service_el()
        self.service_url = self.get_service_url(service_el)
        self.service_protocol = self.get_service_protocol(service_el)
        self.service_description = self.get_service_description(service_el)
