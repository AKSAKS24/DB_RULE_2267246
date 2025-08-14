from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
import re
import json

app = FastAPI(
    title="ABAP MARC/MARD Obsolete Fields Scanner for SAP Note 2267246"
)

# Obsolete fields per SAP Note 2267246
OBSOLETE_FIELDS = {
    "MARC": [
        "MEGRU",
        "USEQU",
        "ALTSL",
        "MDACH",
        "DPLFS",
        "DPLPU",
        "DPLHO",
        "FHORI"
    ],
    "MARD": [
        "DISKZ",
        "LSOBS",
        "LMINB",
        "LBSTF"
    ]
}

# Flatten map table-field and also store obsolete_fieldnames set
FIELD_TABLE_MAP = {f"{tbl}-{fld}": (tbl, fld) for tbl, flds in OBSOLETE_FIELDS.items() for fld in flds}
OBSOLETE_FIELDNAMES = set(fld for flds in OBSOLETE_FIELDS.values() for fld in flds)

# Regex for detecting SQL SELECT/JOIN usage for MARC/MARD
SQL_STMT_RE = re.compile(
    r"\bSELECT\b(?P<select_part>.*?)\bFROM\b\s+(?P<table>MARC|MARD)\b(?P<rest>.*?)(?=\bSELECT\b|$)",
    re.IGNORECASE | re.DOTALL
)

# Qualified field usage: MARC-MEGRU
QUALIFIED_FIELD_RE = re.compile(
    r"\b(?P<table>MARC|MARD)-(?P<field>[A-Z0-9_]+)\b",
    re.IGNORECASE
)

# Declaration patterns for TYPE / LIKE (qualified form)
DECLARATION_QUAL_RE = re.compile(
    r"\b(TYPE|LIKE)\b\s+(?P<table>MARC|MARD)-(?P<field>[A-Z0-9_]+)",
    re.IGNORECASE
)

# Declaration patterns: TYPE / LIKE data element name only
DECLARATION_DE_RE = re.compile(
    r"\b(TYPE|LIKE)\b\s+(?P<fieldname>[A-Z0-9_]+)\b",
    re.IGNORECASE
)

class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    code: Optional[str] = ""


def todo_comment(table: str, field: str) -> str:
    return (f"* TODO: {table.upper()}-{field.upper()} is obsolete in S/4HANA (SAP Note 2267246). "
            f"The related functionality is no longer available; remove or replace usage.")


def todo_comment_dataelement(field: str) -> str:
    return (f"* TODO: Data element {field.upper()} relates to obsolete field in S/4HANA (SAP Note 2267246). "
            f"Remove or replace usage.")


def scan_sql(code: str):
    results = []
    for m in SQL_STMT_RE.finditer(code):
        table = m.group("table").upper()
        select_part = m.group("select_part")
        rest_part = m.group("rest")

        # Check qualified usage in SELECT list
        for fm in QUALIFIED_FIELD_RE.finditer(select_part):
            tbl = fm.group("table").upper()
            fld = fm.group("field").upper()
            if tbl in OBSOLETE_FIELDS and fld in OBSOLETE_FIELDS[tbl]:
                results.append({
                    "table": tbl,
                    "field": fld,
                    "span": fm.span(),
                    "suggested_statement": todo_comment(tbl, fld)
                })

        # Unqualified fields in SELECT list
        fields_raw = re.split(r"[,\s]+", select_part.strip())
        for fld in fields_raw:
            fld_up = fld.replace(".", "").upper()
            if fld_up in OBSOLETE_FIELDS.get(table, []):
                results.append({
                    "table": table,
                    "field": fld_up,
                    "span": m.span(),
                    "suggested_statement": todo_comment(table, fld_up)
                })

        # Check WHERE/JOIN part for qualified usages
        for fm in QUALIFIED_FIELD_RE.finditer(rest_part):
            tbl = fm.group("table").upper()
            fld = fm.group("field").upper()
            if tbl in OBSOLETE_FIELDS and fld in OBSOLETE_FIELDS[tbl]:
                results.append({
                    "table": tbl,
                    "field": fld,
                    "span": fm.span(),
                    "suggested_statement": todo_comment(tbl, fld)
                })
    return results


def scan_declarations(code: str):
    results = []
    # Qualified field declaration
    for fm in DECLARATION_QUAL_RE.finditer(code):
        tbl = fm.group("table").upper()
        fld = fm.group("field").upper()
        if tbl in OBSOLETE_FIELDS and fld in OBSOLETE_FIELDS[tbl]:
            results.append({
                "table": tbl,
                "field": fld,
                "span": fm.span(),
                "suggested_statement": todo_comment(tbl, fld)
            })

    # Data element based declaration
    for fm in DECLARATION_DE_RE.finditer(code):
        fld = fm.group("fieldname").upper()
        if fld in OBSOLETE_FIELDNAMES:
            results.append({
                "table": None,
                "field": fld,
                "span": fm.span(),
                "suggested_statement": todo_comment_dataelement(fld)
            })

    return results


@app.post("/remediate-array")
def remediate_array(units: List[Unit]):
    results = []
    for u in units:
        src = u.code or ""
        selects_metadata = []

        # Scan SQL
        for hit in scan_sql(src):
            selects_metadata.append({
                "table": hit["table"],
                "field": hit["field"],
                "target_type": None,
                "target_name": None,
                "start_char_in_unit": hit["span"][0],
                "end_char_in_unit": hit["span"][1],
                "used_fields": [f"{hit['table']}-{hit['field']}"] if hit["table"] else [hit['field']],
                "ambiguous": False,
                "suggested_fields": None,
                "suggested_statement": hit["suggested_statement"]
            })

        # Scan Declarations
        for hit in scan_declarations(src):
            selects_metadata.append({
                "table": hit["table"],
                "field": hit["field"],
                "target_type": None,
                "target_name": None,
                "start_char_in_unit": hit["span"][0],
                "end_char_in_unit": hit["span"][1],
                "used_fields": [f"{hit['table']}-{hit['field']}"] if hit["table"] else [hit['field']],
                "ambiguous": False,
                "suggested_fields": None,
                "suggested_statement": hit["suggested_statement"]
            })

        obj = json.loads(u.model_dump_json())
        obj["selects"] = selects_metadata
        results.append(obj)

    return results