import logging
from typing import Optional

from owslib.csw import CatalogueServiceWeb  # type: ignore
from .models import CswDatasetRecord, CswServiceRecord

LOGGER = logging.getLogger(__name__)


class CSWClient:
    def __init__(self, csw_url):
        self.csw_url = csw_url

    def _filter_service_records(
        self, records: list[CswServiceRecord]
    ) -> list[CswServiceRecord]:
        filtered_records = filter(
            lambda x: x.service_url != "", records
        )  # filter out results without serviceurl
        # delete duplicate service entries, some service endpoint have multiple service records
        # so last record in get_record_results will be retained in case of duplicate
        # since it will be inserted in new_dict last
        new_dict: dict[str, CswServiceRecord] = {
            x.service_url: x for x in filtered_records
        }
        return [value for _, value in new_dict.items()]

    def _get_csw_records(
        self, query: str, maxresults: int = 0
    ) -> list[CswServiceRecord]:
        csw = CatalogueServiceWeb(self.csw_url)
        result: list[CswServiceRecord] = []
        start = 1
        maxrecord = maxresults if (maxresults < 100 and maxresults != 0) else 100

        while True:
            csw.getrecords2(
                maxrecords=maxrecord,
                cql=query,
                startposition=start,
                esn="full",
                outputschema="http://www.isotc211.org/2005/gmd",
            )
            records = [CswServiceRecord(rec[1].xml) for rec in csw.records.items()]
            result.extend(records)
            if (
                maxresults != 0 and len(result) >= maxresults
            ):  # break only early when maxresults set
                break
            if csw.results["nextrecord"] != 0:
                start = csw.results["nextrecord"]
                continue
            break

        filtered_services = self._filter_service_records(result)
        return sorted(filtered_services, key=lambda x: x.title)

    def _get_csw_records_by_protocol(
        self, protocol: str, svc_owner: str, max_results: int = 0
    ) -> list[CswServiceRecord]:
        query = f"type='service' AND organisationName='{svc_owner}' AND protocol='{protocol}'"
        records = self._get_csw_records(query, max_results)
        LOGGER.debug(f"query: {query}")
        LOGGER.info(f"found {len(records)} {protocol} service metadata records")
        return records

    def get_dataset_metadata(self, md_id: str) -> Optional[CswDatasetRecord]:
        csw = CatalogueServiceWeb(self.csw_url)
        csw.getrecordbyid(id=[md_id], outputschema="http://www.isotc211.org/2005/gmd")
        try:
            record = csw.records[md_id]
            result = CswDatasetRecord(
                title=record.identification.title,
                abstract=record.identification.abstract,
                metadata_id=md_id,
            )
            return result
        except KeyError:  # TODO: log missing dataset records
            return None

    def get_csw_record_by_id(self, id: str) -> list[CswServiceRecord]:
        query = f"identifier='{id}'"
        result = self._get_csw_records(query)
        return result

    def get_csw_records_by_protocols(
        self, protocol_list: list[str], svc_owner: str, number_records: int
    ) -> list[CswServiceRecord]:
        csw_results = list(
            map(
                lambda x: self._get_csw_records_by_protocol(
                    x, svc_owner, number_records
                ),
                protocol_list,
            )
        )
        return [
            item for sublist in csw_results for item in sublist
        ]  # flatten list of lists
