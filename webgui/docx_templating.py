import json
from typing import List
import py_docx_cc

from .rules import create_rule_function


def get_docx_schema(docx_data: bytes) -> dict:
    data = py_docx_cc.get_content_controls(docx_data)
    return data


def build_mapper(template_config: str):
    json_rules = json.loads(template_config)
    rule_functions = {
        tag: create_rule_function(rule) for tag, rule in json_rules.items() if rule
    }
    def _parse_config(data, *args, **kwargs):
        return {tag: fun(data, *args, **kwargs) for tag, fun in rule_functions.items()}
    return _parse_config


def check_config(template_config: str) -> List[str]:
    json_rules = json.loads(template_config)
    failed_tags = []
    for tag, raw_rule in json_rules.items():
        if not raw_rule:
            continue
        try:
            create_rule_function(raw_rule)
        except:
            failed_tags.append(tag)
    return failed_tags


def generate_docx(template_config, case_data, docx_data, snippets=None):
    mapper = build_mapper(template_config)
    mappings = mapper(case_data, snippets)
    from pprint import pprint
    pprint(mappings)
    mapped_docx = py_docx_cc.map_content_controls(docx_data, mappings)
    return mapped_docx
