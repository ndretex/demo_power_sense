from typing import Any, Dict, List, Tuple

from dateutil import parser as date_parser

from .config import DEFAULT_SOURCE


def remap_metric_name(original_name: str) -> str:
    """
    Remap original metric names to standardized ones.
    """
    # Known explicit mappings from the source column names to normalized keys
    map_dict = {
        "Périmètre": "perimetre",
        "Nature": "nature",
        "Date": "date",
        "Heures": "heure",
        "Consommation": "consommation",
        "Prévision J-1": "prevision_j1",
        "Prévision J": "prevision_j",
        "Fioul": "fioul",
        "Charbon": "charbon",
        "Gaz": "gaz",
        "Nucléaire": "nucleaire",
        "Eolien": "eolien",
        "Eolien terrestre": "eolien_terrestre",
        "Eolien offshore": "eolien_offshore",
        "Solaire": "solaire",
        "Hydraulique": "hydraulique",
        "Pompage": "pompage",
        "Bioénergies": "bioenergies",
        "Bioénergies - Déchets": "bioenergies_dechets",
        "Bioénergies - Biomasse": "bioenergies_biomasse",
        "Bioénergies - Biogaz": "bioenergies_biogaz",
        "Ech. physiques": "ech_physiques",
        "Taux de Co2": "taux_co2",
        "Ech. comm. Angleterre": "ech_comm_angleterre",
        "Ech. comm. Espagne": "ech_comm_espagne",
        "Ech. comm. Italie": "ech_comm_italie",
        "Ech. comm. Suisse": "ech_comm_suisse",
        "Ech. comm. Allemagne-Belgique": "ech_comm_allemagne_belgique",
        "Fioul - TAC": "fioul_tac",
        "Fioul - Cogén.": "fioul_cogen",
        "Fioul - Autres": "fioul_autres",
        "Gaz - TAC": "gaz_tac",
        "Gaz - Cogén.": "gaz_cogen",
        "Gaz - CCG": "gaz_ccg",
        "Gaz - Autres": "gaz_autres",
        "Hydraulique - Fil de l'eau + éclusée": "hydraulique_fil_eau_eclusee",
        "Hydraulique - Lacs": "hydraulique_lacs",
        "Hydraulique - STEP turbinage": "hydraulique_step_turbinage",
        " Stockage batterie": "stockage_batterie",
        "Déstockage batterie": "destockage_batterie",
    }

    # Normalize input and return mapped value when possible. Fallback to a
    # sanitized snake_case version of the original name.
    key = original_name.strip()
    if key in map_dict:
        return map_dict[key]

    # Fallback normalization: remove accents, replace non-alnum with underscore
    try:
        import unicodedata

        nk = unicodedata.normalize("NFKD", key)
        ascii_key = "".join([c for c in nk if not unicodedata.combining(c)])
    except Exception:
        ascii_key = key

    # replace characters that are not letters/numbers with underscore
    norm = []
    for c in ascii_key:
        if c.isalnum():
            norm.append(c.lower())
        else:
            norm.append("_")
    norm_key = "".join(norm)
    # collapse multiple underscores
    while "__" in norm_key:
        norm_key = norm_key.replace("__", "_")
    norm_key = norm_key.strip("_")
    return norm_key or original_name


def normalize_record(rec: Dict[str, Any]) -> List[Tuple]:
    """
    Convert one API result object into a list of measurement rows.

    Each numeric field (except metadata fields) becomes its own row:
    (ts, source, metric, value)

    We skip null values to avoid inserting empty metrics.
    """
    ts = rec.get("date_heure")

    # If `date_heure` not present, attempt to infer from available fields
    if not ts:
        # look for values that look like YYYY-MM-DD and HH:MM
        date_val = None
        heure_val = None
        for _, v in rec.items():
            if v is None:
                continue
            try:
                s = str(v).strip()
            except Exception:
                continue
            # simple ISO date detection
            if date_val is None and len(s) >= 8 and s[0:4].isdigit() and s[4] == "-":
                # matches 2025-01-01 etc.
                date_val = s
            # time detection HH:MM
            if (
                heure_val is None
                and len(s) >= 4
                and s[0:2].isdigit()
                and s[2] == ":"
                and s[3:5].isdigit()
            ):
                heure_val = s
            if date_val and heure_val:
                break
        if date_val and heure_val:
            ts = f"{date_val}T{heure_val}:00+00:00"
        else:
            return []
    try:
        ts_dt = date_parser.parse(ts)
    except Exception:
        return []

    source = rec.get("perimetre", DEFAULT_SOURCE)
    nature = rec.get("nature")

    skip_keys = {
        "perimetre",
        "nature",
        "date",
        "heure",
        "date_heure",
        "total_count",
        "results",
    }
    rows: List[Tuple] = []
    for k, v in rec.items():
        if k in skip_keys:
            continue
        if v is None:
            continue
        try:
            val = float(v)
        except Exception:
            continue

        metric = k
    rows.append((ts_dt, source, metric, val, source, nature))

    return rows
