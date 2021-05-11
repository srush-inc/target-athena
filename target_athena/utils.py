#!/usr/bin/env python3

from datetime import datetime
import time
import singer
import json
import re
import collections
import inflection

from decimal import Decimal
from datetime import datetime

logger = singer.get_logger("target_athena")


def float_to_decimal(value):
    """Walk the given data structure and turn all instances of float into
    double."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


# pylint: disable=unnecessary-comprehension
def flatten_key(k, parent_key, sep):
    """"""
    full_key = parent_key + [k]
    inflected_key = [n for n in full_key]
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 255 and reducer_index < len(inflected_key):
        reduced_key = re.sub(
            r"[a-z]", "", inflection.camelize(inflected_key[reducer_index])
        )
        inflected_key[reducer_index] = (
            reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]
        ).lower()
        reducer_index += 1

    return sep.join(inflected_key)


def flatten_record(d, parent_key=[], sep="__"):
    """"""
    items = []
    for k in sorted(d.keys()):
        v = d[k]
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_record(v, parent_key + [k], sep=sep).items())
        else:
            items.append((new_key, json.dumps(v) if type(v) is list else v))
    return dict(items)


def get_target_key(stream_name, prefix=None, timestamp=None, naming_convention=None):
    """Creates and returns an S3 key for the message"""
    if not naming_convention:
        naming_convention = (
            "{stream}/{timestamp}.csv"  # o['stream'] + '-' + now + '.csv'
        )
    if not timestamp:
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    key = naming_convention

    # replace simple tokens
    for k, v in {
        "{stream}": stream_name,
        "{timestamp}": timestamp,
        "{date}": datetime.now().strftime("%Y-%m-%d"),
    }.items():
        if k in key:
            try:
                key = key.replace(k, v)
            except Exception as ex:
                raise Exception(f"{ex}. Key was {key}, k was {k}. Value was {v}")

    # replace dynamic tokens
    # todo: replace dynamic tokens such as {date(<format>)} with the date formatted as requested in <format>

    if prefix:
        filename = key.split("/")[-1]
        key = key.replace(filename, f"{prefix}{filename}")
    return key


# This function is borrowed direclty from https://github.com/datadudes/json2hive/blob/master/json2hive/generators.py
def generate_field_definitions(schema, level=0):
    keywords = ["timestamp", "date", "datetime"]
    tab = "  "
    type_separator = " " if level == 0 else ": "
    field_separator = ",\n" if level == 0 else ",\n"
    field_definitions = []
    new_level = level + 1
    indentation = new_level * tab
    for name, attributes in schema.items():
        cleaned_name = "`{}`".format(name)  # if name.lower() in keywords else name
        if attributes["type"] == "object":
            field_definitions.append(
                "{indentation}{name}{separator}STRUCT<\n{definitions}\n{indentation}>".format(
                    indentation=indentation,
                    name=cleaned_name,
                    separator=type_separator,
                    definitions=generate_field_definitions(
                        attributes["properties"], new_level
                    ),
                )
            )
        elif attributes["type"] == "array":
            extra_indentation = (new_level + 1) * tab
            if attributes["items"]["type"] == "object":
                closing_bracket = "\n" + indentation + ">"
                array_type = "STRUCT<\n{definitions}\n{indentation}>".format(
                    indentation=extra_indentation,
                    definitions=generate_field_definitions(
                        attributes["items"]["properties"], new_level + 1
                    ),
                )
            else:
                closing_bracket = ">"
                array_type = attributes["items"]["type"].upper()
            field_definitions.append(
                "{indentation}{name}{separator}ARRAY<{definitions}{closing_bracket}".format(
                    indentation=indentation,
                    name=cleaned_name,
                    separator=type_separator,
                    definitions=array_type,
                    closing_bracket=closing_bracket,
                )
            )
        elif isinstance(attributes["type"], list):
            types = [_ for _ in attributes["type"] if _ != "null"]
            field_definitions.append(
                "{indentation}{name}{separator}{type}".format(
                    indentation=indentation,
                    name=cleaned_name,
                    separator=type_separator,
                    type="STRING"
                    # type=types[0].upper()
                )
            )
        else:
            field_definitions.append(
                "{indentation}{name}{separator}{type}".format(
                    indentation=indentation,
                    name=cleaned_name,
                    separator=type_separator,
                    # type=attributes['type'].upper()
                    type="STRING",
                )
            )
    return field_separator.join(field_definitions)


# This function is borrowed direclty from https://github.com/datadudes/json2hive/blob/master/json2hive/generators.py
def generate_json_table_statement(
    table,
    schema,
    headers=None,
    data_location="",
    database="default",
    external=True,
    serde="org.apache.hadoop.hive.serde2.OpenCSVSerde",
):
    if not headers:
        field_definitions = generate_field_definitions(schema["properties"])
    else:
        field_definitions = ",\n".join(["  `{}` STRING".format(_) for _ in headers])
    external_marker = "EXTERNAL " if external else ""
    row_format = "ROW FORMAT SERDE '{serde}'".format(serde=serde) if serde else ""
    stored = "\nSTORED AS TEXTFILE"
    location = "\nLOCATION '{}'".format(data_location) if external else ""
    tblproperties = '\nTBLPROPERTIES ("skip.header.line.count" = "1")'
    statement = """CREATE {external_marker}TABLE IF NOT EXISTS {database}.{table} (
{field_definitions}
)
{row_format}{stored}{location}{tblproperties};""".format(
        external_marker=external_marker,
        database=database,
        table=table,
        field_definitions=field_definitions,
        row_format=row_format,
        stored=stored,
        location=location,
        tblproperties=tblproperties,
    )
    return statement
