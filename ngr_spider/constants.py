CSW_URL = "https://nationaalgeoregister.nl/geonetwork/srv/dut/csw"
WFS_PROTOCOL = "OGC:WFS"
WMS_PROTOCOL = "OGC:WMS"
WCS_PROTOCOL = "OGC:WCS"
WMTS_PROTOCOL = "OGC:WMTS"
ATOM_PROTOCOL = "INSPIRE Atom"
OAT_PROTOCOL = "OGC:API tiles"
OAF_PROTOCOL = "OGC:API features"
PROTOCOLS = [
    WFS_PROTOCOL,
    WMS_PROTOCOL,
    WCS_PROTOCOL,
    WMTS_PROTOCOL,
    ATOM_PROTOCOL,
    OAT_PROTOCOL,
    OAF_PROTOCOL,
]
PROTOCOL_LOOKUP = {
    OAT_PROTOCOL: "layers",
    WMTS_PROTOCOL: "layers",
    WMS_PROTOCOL: "layers",
    WFS_PROTOCOL: "featuretypes",
    OAF_PROTOCOL: "featuretypes",
    WCS_PROTOCOL: "coverages",
    ATOM_PROTOCOL: "datasets",
}
