"""
hacvt_rdt.py — SOSA/RDT exporter for Home Assistant
=====================================================
Inherits from HACVT (hacvt.py) and overrides the ontology backend
to produce .ttl files compatible with the RDT SmartNode ontology:
  http://www.semanticweb.org/ivans/ontologies/2025/ruleless-digital-twins/

Key differences from hacvt.py (SAREF backend):
  - Uses SOSA (http://www.w3.org/ns/sosa/) and SSN namespaces
  - Maps HA domains → sosa:Sensor / sosa:Actuator / sosa:Platform
  - Attaches rdt:hasIdentifier with the raw HA entity_id
  - Queries /api/services to capture possible actuator states
    and emits rdt:hasActuatorState triples for each
  - ObservableProperty used for sensor measurement targets
  - No SAREF, S4BLDG or homeassistantcore.rdf side-effect

Usage (same CLI as hacvt.py):
  python hacvt_rdt.py http://homeassistant.local:8123/api/ HA_TOKEN \\
      --namespace "http://www.semanticweb.org/rayan/ontologies/2025/ha/" \\
      --out ha_rdt.ttl
"""

import argparse
import logging
from typing import Optional

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import Namespace, RDF, RDFS, OWL, XSD

import homeassistant.const as hc
import homeassistant.core as ha

from ConfigSource import CLISource
from hacvt import HACVT, PrivacyFilter, mkname


# ---------------------------------------------------------------------------
# Namespaces
# ---------------------------------------------------------------------------
SOSA_NS = Namespace("http://www.w3.org/ns/sosa/")
SSN_NS  = Namespace("http://www.w3.org/ns/ssn/")
RDT_NS  = Namespace(
    "http://www.semanticweb.org/ivans/ontologies/2025/ruleless-digital-twins/"
)


# ---------------------------------------------------------------------------
# Domain → SOSA class mapping
# ---------------------------------------------------------------------------
# True  → use HA sub-class  (HASS[domain.title()] rdfs:subClassOf sosa:X)
# False → use sosa:X directly
# None  → skip this domain entirely
_DOMAIN_TO_SOSA: dict = {
    hc.Platform.BINARY_SENSOR:       (False, "Sensor"),
    hc.Platform.SENSOR:              (False, "Sensor"),
    hc.Platform.AIR_QUALITY:         (True,  "Sensor"),
    hc.Platform.DEVICE_TRACKER:      (True,  "Sensor"),
    hc.Platform.WEATHER:             (True,  "Sensor"),
    hc.Platform.SWITCH:              (True,  "Actuator"),
    hc.Platform.FAN:                 (True,  "Actuator"),
    hc.Platform.LIGHT:               (True,  "Actuator"),
    hc.Platform.COVER:               (True,  "Actuator"),
    hc.Platform.LOCK:                (True,  "Actuator"),
    hc.Platform.HUMIDIFIER:          (True,  "Actuator"),
    hc.Platform.SIREN:               (True,  "Actuator"),
    hc.Platform.VACUUM:              (True,  "Actuator"),
    hc.Platform.WATER_HEATER:        (True,  "Actuator"),
    hc.Platform.CLIMATE:             (True,  "Actuator"),
    hc.Platform.MEDIA_PLAYER:        (True,  "Actuator"),
    hc.Platform.BUTTON:              (True,  "Actuator"),
    hc.Platform.REMOTE:              (True,  "Actuator"),
    hc.Platform.CAMERA:              (True,  "Platform"),
    hc.Platform.ALARM_CONTROL_PANEL: (True,  "Platform"),
    # Skipped domains
    hc.Platform.CALENDAR:        None,
    hc.Platform.GEO_LOCATION:    None,
    hc.Platform.IMAGE_PROCESSING: None,
    hc.Platform.NOTIFY:          None,
    hc.Platform.NUMBER:          None,
    hc.Platform.SCENE:           None,
    hc.Platform.SELECT:          None,
    hc.Platform.STT:             None,
    hc.Platform.TEXT:            None,
    hc.Platform.TTS:             None,
    hc.Platform.UPDATE:          None,
}

_ACTUATOR_DOMAINS = {
    p for p, v in _DOMAIN_TO_SOSA.items()
    if v is not None and v[1] == "Actuator"
}


class HACVT_RDT(HACVT):
    """
    SOSA/RDT variant of HACVT.

    Overrides:
      - main()                  : sets up SOSA graph instead of SAREF
      - _handle_entity_rdt()    : maps to sosa:Sensor / sosa:Actuator
      - _add_possible_states()  : harvests /api/services for actuator states
    """

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------
    def main(
        self,
        debug=logging.INFO,
        certificate=None,
        privacy=None,
        namespace="http://my.name.space/ha/",
    ):
        logging.basicConfig(level=debug, format="%(levelname)s: %(message)s")
        self.cs.ws = self.cs._ws_connect(certificate=certificate)
        self.cs.ws_counter = 1

        pf = PrivacyFilter(self.cs)
        pf.privacyFilter_init(privacy=privacy)

        g = Graph(bind_namespaces="core")
        MINE, HASS = self._setup_sosa(g, namespace)

        the_devices = self.cs.getDevices()
        for d in the_devices:
            d_g          = pf.mkDevice(MINE, d)
            manufacturer = self.cs.getDeviceAttr(d, hc.ATTR_MANUFACTURER)
            name         = self.cs.getDeviceAttr(d, hc.ATTR_NAME)
            model        = self.cs.getDeviceAttr(d, hc.ATTR_MODEL)

            # Device super-class (manufacturer + model)
            d_super = MINE["device/" + mkname(manufacturer) + "_" + mkname(model)]
            g.add((d_super, RDFS.subClassOf,  SOSA_NS["Platform"]))
            g.add((d_g,     RDF.type,          d_super))
            g.add((d_g,     RDFS.label,        Literal(name)))

            # Area
            d_area    = self.cs.getYAMLText(f'area_id("{d}")')
            area_name = self.cs.getYAMLText(f'area_name("{d}")')
            if d_area.strip() not in ("None", ""):
                area = pf.mkLocationURI(MINE, d_area.strip())
                g.add((area,  RDF.type,           SOSA_NS["Platform"]))
                g.add((area,  RDFS.label,          Literal(area_name.strip())))
                g.add((d_g,   SSN_NS["isHostedBy"], area))

            # Entities hosted by this device
            es = self.cs.getDeviceEntities(d)
            for e in es:
                if not (isinstance(e, str) and e.count(".") == 1):
                    continue
                e_node = self._handle_entity_rdt(pf, MINE, HASS, d, e, g)
                if e_node is not None:
                    g.add((d_g, SOSA_NS["hosts"], e_node))

        # Entities without a parent device
        for e_state in self._get_entities_wo_device():
            e_id = e_state["entity_id"]
            if e_id.count(".") != 1:
                continue
            self._handle_entity_rdt(pf, MINE, HASS, None, e_id, g)

        logging.info("Serialising RDT/SOSA graph ...")
        return g

    # ------------------------------------------------------------------
    # Graph / namespace setup
    # ------------------------------------------------------------------
    def _setup_sosa(self, g: Graph, namespace: str):
        MINE = Namespace(namespace)
        HASS = Namespace("https://www.foldr.org/profiles/homeassistant/")

        g.bind("sosa",  SOSA_NS)
        g.bind("ssn",   SSN_NS)
        g.bind("rdt",   RDT_NS)
        g.bind("hass",  HASS)
        g.bind("mine",  MINE)
        g.bind("owl",   OWL)

        ont = URIRef(str(MINE))
        g.add((ont, RDF.type,    OWL.Ontology))
        g.add((ont, OWL.imports, URIRef("http://www.w3.org/ns/ssn/")))
        g.add((ont, OWL.imports, URIRef("http://www.w3.org/ns/sosa/")))

        # Declare RDT datatype properties so the file is self-contained
        for prop in ("hasIdentifier", "hasActuatorState", "hasPossibleValue"):
            g.add((RDT_NS[prop], RDF.type, OWL.DatatypeProperty))

        # Sub-class HA domains under the appropriate SOSA class
        for domain, mapping in _DOMAIN_TO_SOSA.items():
            if mapping is None:
                continue
            subclass_flag, sosa_class = mapping
            if subclass_flag:
                g.add((HASS[domain.title()], RDFS.subClassOf, SOSA_NS[sosa_class]))

        return MINE, HASS

    # ------------------------------------------------------------------
    # Entity handler — SOSA version
    # ------------------------------------------------------------------
    def _handle_entity_rdt(
        self,
        pf: PrivacyFilter,
        MINE: Namespace,
        HASS: Namespace,
        device: Optional[str],
        entity_id: str,
        g: Graph,
    ) -> Optional[URIRef]:

        domain, _ = ha.split_entity_id(entity_id)
        mapping   = _DOMAIN_TO_SOSA.get(domain)

        if mapping is None:
            logging.warning(f"Skipping {entity_id}: domain '{domain}' not mapped.")
            return None

        subclass_flag, sosa_class = mapping
        sosa_type = HASS[domain.title()] if subclass_flag else SOSA_NS[sosa_class]

        e_node, e_name = pf.mkEntityURI(MINE, entity_id)
        g.add((e_node, RDF.type,               sosa_type))
        g.add((e_node, RDT_NS["hasIdentifier"], Literal(entity_id, datatype=XSD.string)))

        # Friendly name
        try:
            attrs = self.cs.getAttributes(entity_id)
            fname = attrs.get(hc.ATTR_FRIENDLY_NAME)
            if fname:
                g.add((e_node, RDFS.label, Literal(fname)))
        except Exception:
            pass

        # Sensors: link to an ObservableProperty
        if sosa_class == "Sensor":
            prop_node = MINE["property/" + mkname(e_name)]
            g.add((prop_node, RDF.type,            SOSA_NS["ObservableProperty"]))
            g.add((e_node,    SOSA_NS["observes"],  prop_node))

        # Actuators: link to a Procedure + harvest possible states
        elif sosa_class == "Actuator":
            proc_node = MINE["procedure/" + mkname(e_name)]
            g.add((proc_node, RDF.type,             SOSA_NS["Procedure"]))
            g.add((e_node,    SOSA_NS["implements"], proc_node))
            self._add_possible_states(g, MINE, e_node, entity_id, domain)

        return e_node

    # ------------------------------------------------------------------
    # Harvest possible actuator states from /api/services
    # ------------------------------------------------------------------
    def _add_possible_states(
        self,
        g: Graph,
        MINE: Namespace,
        e_node: URIRef,
        entity_id: str,
        domain,
    ):
        """
        Populate rdt:hasActuatorState triples from /api/services.

        Strategy:
          1. For on/off-capable domains always add "on" / "off"
          2. Inspect service fields:
             - selector.select  → explicit enum values
             - selector.number  → range annotation string range:min:max:stepN
             - selector.boolean → "true" / "false"
        """
        services   = self.cs.getServices()
        domain_key = str(domain)

        toggle_domains = {
            "switch", "light", "fan", "climate", "cover",
            "lock", "siren", "vacuum", "humidifier", "media_player",
        }
        if domain_key in toggle_domains:
            for state in ("on", "off"):
                g.add((e_node, RDT_NS["hasActuatorState"], Literal(state)))

        if domain_key not in services:
            return

        svc_map = services[domain_key]  # dict: service_name → service_info
        for svc_name, svc_info in svc_map.items():
            if not isinstance(svc_info, dict):
                continue
            fields = svc_info.get("fields", {})
            for field_name, field_info in fields.items():
                if not isinstance(field_info, dict):
                    continue
                selector = field_info.get("selector", {})
                if not isinstance(selector, dict):
                    continue

                # Explicit enum of possible values
                if "select" in selector:
                    options = selector["select"].get("options", [])
                    for opt in options:
                        val = opt if isinstance(opt, str) else opt.get("value", "")
                        if val:
                            g.add((e_node, RDT_NS["hasActuatorState"], Literal(val)))

                # Numeric range → encoded as range:min:max:stepN
                if "number" in selector:
                    num_sel = selector["number"]
                    v_min   = num_sel.get("min")
                    v_max   = num_sel.get("max")
                    step    = num_sel.get("step", 1)
                    if v_min is not None and v_max is not None:
                        g.add((
                            e_node,
                            RDT_NS["hasActuatorState"],
                            Literal(f"range:{v_min}:{v_max}:step{step}", datatype=XSD.string),
                        ))

                # Boolean field
                if "boolean" in selector:
                    for bv in ("true", "false"):
                        g.add((e_node, RDT_NS["hasActuatorState"], Literal(bv)))

    # ------------------------------------------------------------------
    # Entities without a parent device (skip automations)
    # ------------------------------------------------------------------
    def _get_entities_wo_device(self):
        for k in self.cs.getStates():
            if self.cs.getDeviceId(k["entity_id"]) == "None":
                yield k


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export a Home Assistant instance to a SOSA/RDT-compatible .ttl file."
    )
    parser.add_argument(
        "-d", "--debug", default="INFO", const="DEBUG", nargs="?",
        help="Log level (INFO by default, DEBUG if flag given alone).",
    )
    parser.add_argument(
        "-n", "--namespace",
        default="http://www.semanticweb.org/rayan/ontologies/2025/ha/",
        help="Base namespace for generated individuals.",
    )
    parser.add_argument(
        "-o", "--out", default="ha_rdt.ttl",
        help="Output .ttl file (default: ha_rdt.ttl).",
    )
    parser.add_argument(
        "-p", "--privacy", nargs="*", metavar="platform",
        help="Enable privacy filter.",
    )

    cli  = CLISource(parser)
    tool = HACVT_RDT(cli)
    g    = tool.main(
        debug=cli.args.debug,
        certificate=cli.args.certificate,
        privacy=cli.args.privacy,
        namespace=cli.args.namespace,
    )
    with open(cli.args.out, "w") as f_out:
        f_out.write(g.serialize(format="turtle"))
    print(f"Written to {cli.args.out}")
