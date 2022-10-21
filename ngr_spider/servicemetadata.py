from lxml import etree
from urllib.parse import parse_qs, urlparse

class ServiceMetadata:

    ns = {
        "csw": "http://www.opengis.net/cat/csw/2.0.2",
        "gmd": "http://www.isotc211.org/2005/gmd",
        "dc": "http://purl.org/dc/elements/1.1/",
        "dct": "http://purl.org/dc/terms/",
        "srv":"http://www.isotc211.org/2005/srv",
        "xsi":"http://www.w3.org/2001/XMLSchema-instance",
        "gmx":"http://www.isotc211.org/2005/gmx",
        "gco":"http://www.isotc211.org/2005/gco",
        "xlink":"http://www.w3.org/1999/xlink",
    }
    xpath_sv_service_identification=".//gmd:identificationInfo/srv:SV_ServiceIdentification"
    xpath_ci_resource=".//gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions/gmd:onLine/gmd:CI_OnlineResource"

    def get_record_identifier(self):
        xpath_query= f".//gmd:fileIdentifier/gco:CharacterString/text()"
        return self.get_text_xpath(xpath_query)

    def get_use_limitation(self):
        xpath_query= f"{self.xpath_sv_service_identification}/gmd:resourceConstraints/gmd:MD_Constraints/gmd:useLimitation/gco:CharacterString/text()"
        return self.get_text_xpath(xpath_query)

    def get_service_url(self):
        result=""
        return result
    
    def get_service_protocol(self):
        result=""
        return result
    
    def get_text_xpath(self, xpath_query):
        try:
            return self.root.xpath(xpath_query, namespaces=self.ns)[0]
        except IndexError:            
            return ""
        

    def get_title(self):
        xpath_query= f"{self.xpath_sv_service_identification}/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()"
        return self.get_text_xpath(xpath_query)
        
    def get_abstract(self):
        xpath_query= f"{self.xpath_sv_service_identification}/gmd:abstract/gco:CharacterString/text()"
        return self.get_text_xpath(xpath_query)
    
    def get_point_of_contact(self):
        return {}

    def get_keywords(self):
        xpath_query= f"{self.xpath_sv_service_identification}/gmd:descriptiveKeywords/gmd:MD_Keywords"
        md_keywords = self.root.xpath(xpath_query, namespaces=self.ns)
        keywords_result = {}
        for md_keyword in md_keywords:
            keywords_els = md_keyword.xpath('./gmd:keyword',  namespaces=self.ns)
            for keyword_el in keywords_els:
                try:
                    keyword_val = keyword_el.xpath('./gco:CharacterString/text()', namespaces=self.ns)[0]
                    if "" not in keywords_result:
                        keywords_result[""] = []
                    keywords_result[""].append(keyword_val)
                        
                except IndexError:
                    keyword_val = keyword_el.xpath('./gmx:Anchor/text()', namespaces=self.ns)[0]
                    keyword_ns =  keyword_el.xpath('./gmx:Anchor/@xlink:href', namespaces=self.ns)[0]
                    if keyword_ns not in keywords_result:
                        keywords_result[keyword_ns] = []
                    keywords_result[keyword_ns].append(keyword_val)
        return keywords_result    

    def get_operates_on(self):
        return self.get_text_xpath(f"{self.xpath_sv_service_identification}/srv:operatesOn/@xlink:href")
    
    def get_dataset_record_identifier(self, operates_on_url):
        parsed_url = urlparse(operates_on_url.lower())
        return parse_qs(parsed_url.query)['id'][0]

    def get_service_url(self):
        xpath_query= f"{self.xpath_ci_resource}/gmd:linkage/gmd:URL/text()"
        return self.get_text_xpath(xpath_query)
        
    def get_service_protocol(self):
        xpath_query= f"{self.xpath_ci_resource}/gmd:protocol/gmx:Anchor/text()"
        return self.get_text_xpath(xpath_query)
    
    def get_service_description(self):
        xpath_query= f"{self.xpath_ci_resource}/gmd:description/gmx:Anchor/text()"
        return self.get_text_xpath(xpath_query)
         

    def __init__(self, xml):
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding='utf-8')
        self.root = etree.fromstring(xml, parser=parser)
        self.title = self.get_title()
        self.abstract = self.get_abstract()
        self.use_limitation = self.get_use_limitation()
        self.record_identifier = self.get_record_identifier()
        self.point_of_contact = self.get_point_of_contact()
        self.keywords = self.get_keywords()
        self.operates_on = self.get_operates_on()
        self.dataset_record_identifier = self.get_dataset_record_identifier(self.operates_on)
        
        self.service_url = self.get_service_url()
        self.service_protocol = self.get_service_protocol()
        self.service_description = self.get_service_description()
        