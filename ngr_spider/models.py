
import enum
from urllib.parse import parse_qs, urlparse
import logging
from dataclass_wizard import JSONWizard # type: ignore
from lxml import etree # type: ignore
import dataclasses # type: ignore

class LayersMode(enum.Enum):
    Flat = "flat"
    Services = "services"
    Datasets = "datasets"
    def __str__(self):
        return self.value
        
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
class ServiceError():
    url: str
    metadata_id: str

@dataclasses.dataclass
class Service(JSONWizard):
    title: str
    abstract: str
    metadata_id: str
    dataset_metadata_id:str
    url: str
    keywords: list[str]
    protocol: str

@dataclasses.dataclass(kw_only=True)
class WfsService(Service):
    featuretypes: list[Layer]
    output_formats: str
    protocol: str = "OGC:WFS"

@dataclasses.dataclass(kw_only=True)
class WcsService(Service):
    coverages: list[Layer]
    # formats: str # formats no supported for now, OWSLib does not seem to extract the formats correctly
    protocol: str = "OGC:WCS"

@dataclasses.dataclass(kw_only=True)
class WmsService(Service):
    imgformats: str
    layers: list[WmsLayer]
    protocol: str = "OGC:WMS"

@dataclasses.dataclass(kw_only=True)
class WmtsService(Service):
    layers: list[WmtsLayer]
    protocol: str = "OGC:WMTS"


@dataclasses.dataclass
class Dataset():
    title: str
    abstract: str
    metadata_id: str
    services: list[Service]

@dataclasses.dataclass
class CswDatasetRecord(JSONWizard):
    title: str
    abstract: str
    metadata_id: str

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
    _xpath_ci_resource = ".//gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource"

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

    def get_text_xpath(self, xpath_query):
        try:
            return str(self.root.xpath(xpath_query, namespaces=self._ns)[0])
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
                    keyword_val = str(keyword_el.xpath(
                        "./gco:CharacterString/text()", namespaces=self._ns
                    )[0])
                    if "" not in keywords_result:
                        keywords_result[""] = []
                    keywords_result[""].append(keyword_val)

                except IndexError:
                    try:
                        keyword_val = str(keyword_el.xpath(
                            "./gmx:Anchor/text()", namespaces=self._ns
                        )[0])
                        keyword_ns = str(keyword_el.xpath(
                            "./gmx:Anchor/@xlink:href", namespaces=self._ns
                        )[0])
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

    def get_service_url(self):
        xpath_query = f"{self._xpath_ci_resource}/gmd:linkage/gmd:URL/text()"
        return self.get_text_xpath(xpath_query)

    def get_service_protocol(self):
        xpath_query = f"{self._xpath_ci_resource}/gmd:protocol/gmx:Anchor/text()"
        result = self.get_text_xpath(xpath_query)
        if result == '':
            xpath_query = f"{self._xpath_ci_resource}/gmd:protocol/gco:CharacterString/text()"
            result = self.get_text_xpath(xpath_query)
        return result

    def get_service_description(self):
        xpath_query = f"{self._xpath_ci_resource}/gmd:description/gmx:Anchor/text()"
        return self.get_text_xpath(xpath_query)

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
        self.service_url = self.get_service_url()
        self.service_protocol = self.get_service_protocol()
        self.service_description = self.get_service_description()
