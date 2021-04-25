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

logger = singer.get_logger('target_athena')


def validate_config(config):
    """Validates config"""
    errors = []
    required_config_keys = [
        's3_bucket'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append("Required key is missing from config: [{}]".format(k))

    return errors


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


def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    extended_schema_message['schema']['properties']['_sdc_batched_at'] = { 'type': ['null', 'string'], 'format': 'date-time' }
    extended_schema_message['schema']['properties']['_sdc_deleted_at'] = { 'type': ['null', 'string'] }
    extended_schema_message['schema']['properties']['_sdc_extracted_at'] = { 'type': ['null', 'string'], 'format': 'date-time' }
    extended_schema_message['schema']['properties']['_sdc_primary_key'] = {'type': ['null', 'string'] }
    extended_schema_message['schema']['properties']['_sdc_received_at'] = { 'type': ['null', 'string'], 'format': 'date-time' }
    extended_schema_message['schema']['properties']['_sdc_sequence'] = {'type': ['integer'] }
    extended_schema_message['schema']['properties']['_sdc_table_version'] = {'type': ['null', 'string'] }

    return extended_schema_message


def add_metadata_values_to_record(record_message, schema_message):
    """Populate metadata _sdc columns from incoming record message
    The location of the required attributes are fixed in the stream
    """
    extended_record = record_message['record']
    extended_record['_sdc_batched_at'] = datetime.now().isoformat()
    extended_record['_sdc_deleted_at'] = record_message.get('record', {}).get('_sdc_deleted_at')
    extended_record['_sdc_extracted_at'] = record_message.get('time_extracted')
    extended_record['_sdc_primary_key'] = schema_message.get('key_properties')
    extended_record['_sdc_received_at'] = datetime.now().isoformat()
    extended_record['_sdc_sequence'] = int(round(time.time() * 1000))
    extended_record['_sdc_table_version'] = record_message.get('version')

    return extended_record


def remove_metadata_values_from_record(record_message):
    """Removes every metadata _sdc column from a given record message
    """
    cleaned_record = record_message['record']
    cleaned_record.pop('_sdc_batched_at', None)
    cleaned_record.pop('_sdc_deleted_at', None)
    cleaned_record.pop('_sdc_extracted_at', None)
    cleaned_record.pop('_sdc_primary_key', None)
    cleaned_record.pop('_sdc_received_at', None)
    cleaned_record.pop('_sdc_sequence', None)
    cleaned_record.pop('_sdc_table_version', None)

    return cleaned_record


# pylint: disable=unnecessary-comprehension
def flatten_key(k, parent_key, sep):
    """
    """
    full_key = parent_key + [k]
    inflected_key = [n for n in full_key]
    reducer_index = 0
    while len(sep.join(inflected_key)) >= 255 and reducer_index < len(inflected_key):
        reduced_key = re.sub(r'[a-z]', '', inflection.camelize(inflected_key[reducer_index]))
        inflected_key[reducer_index] = \
            (reduced_key if len(reduced_key) > 1 else inflected_key[reducer_index][0:3]).lower()
        reducer_index += 1

    return sep.join(inflected_key)

def flatten_record(d, parent_key=[], sep='__'):
    """
    """
    items = []
    for k in sorted(d.keys()):
        v = d[k]
        new_key = flatten_key(k, parent_key, sep)
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten_record(v, parent_key + [k], sep=sep).items())
        else:
            items.append((new_key, json.dumps(v) if type(v) is list else v))
    return dict(items)


def get_target_key(message, prefix=None, timestamp=None, naming_convention=None):
    """Creates and returns an S3 key for the message"""
    if not naming_convention:
        naming_convention = '{stream}/{timestamp}.csv' # o['stream'] + '-' + now + '.csv'
    if not timestamp:
        timestamp = datetime.now().strftime('%Y%m%dT%H%M%S')
    key = naming_convention
    
    # replace simple tokens
    for k, v in {
        '{stream}': message['stream'],
        '{timestamp}': timestamp,
        '{date}': datetime.now().strftime('%Y-%m-%d')
    }.items():
        if k in key:
            key = key.replace(k, v)

    # replace dynamic tokens
    # todo: replace dynamic tokens such as {date(<format>)} with the date formatted as requested in <format>

    if prefix:
        filename = key.split('/')[-1]
        key = key.replace(filename, f'{prefix}{filename}')
    return key

# This function is borrowed direclty from https://github.com/datadudes/json2hive/blob/master/json2hive/generators.py
def generate_field_definitions(schema, level=0):
    keywords = ['timestamp', 'date', 'datetime']
    tab = "  "
    type_separator = " " if level == 0 else ": "
    field_separator = ",\n" if level == 0 else ",\n"
    field_definitions = []
    new_level = level + 1
    indentation = new_level * tab
    for name, attributes in schema.items():
        cleaned_name = "`{}`".format(name) #if name.lower() in keywords else name
        if attributes['type'] == 'object':
            field_definitions.append("{indentation}{name}{separator}STRUCT<\n{definitions}\n{indentation}>".format(
                indentation=indentation,
                name=cleaned_name,
                separator=type_separator,
                definitions=generate_field_definitions(attributes['properties'], new_level)
            ))
        elif attributes['type'] == 'array':
            extra_indentation = (new_level + 1) * tab
            if attributes['items']['type'] == 'object':
                closing_bracket = "\n" + indentation + ">"
                array_type = "STRUCT<\n{definitions}\n{indentation}>".format(
                    indentation=extra_indentation,
                    definitions=generate_field_definitions(attributes['items']['properties'], new_level + 1)
                )
            else:
                closing_bracket = ">"
                array_type = attributes['items']['type'].upper()
            field_definitions.append("{indentation}{name}{separator}ARRAY<{definitions}{closing_bracket}".format(
                indentation=indentation,
                name=cleaned_name,
                separator=type_separator,
                definitions=array_type,
                closing_bracket=closing_bracket
            ))
        elif isinstance(attributes['type'], list):
            types = [_ for _ in attributes['type'] if _ != 'null']
            field_definitions.append("{indentation}{name}{separator}{type}".format(
                indentation=indentation,
                name=cleaned_name,
                separator=type_separator,
                type='STRING'
                # type=types[0].upper()
            ))           
        else:
            field_definitions.append("{indentation}{name}{separator}{type}".format(
                indentation=indentation,
                name=cleaned_name,
                separator=type_separator,
                # type=attributes['type'].upper()
                type='STRING'                
            ))
    return field_separator.join(field_definitions)

# This function is borrowed direclty from https://github.com/datadudes/json2hive/blob/master/json2hive/generators.py
def generate_json_table_statement(table, schema, data_location='', database='default', 
                                  external=True, serde='org.apache.hadoop.hive.serde2.OpenCSVSerde'):
    field_definitions = generate_field_definitions(schema['properties'])
    external_marker = "EXTERNAL " if external else ""
    row_format = "ROW FORMAT SERDE '{serde}'".format(serde=serde) if serde else ""
    stored = "\nSTORED AS TEXTFILE"
    location = "\nLOCATION '{}'".format(data_location) if external else ''
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
        tblproperties=tblproperties
    )
    return statement