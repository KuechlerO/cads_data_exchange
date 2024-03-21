from lark import Lark
from lark import Transformer


def _call_data(arg, data):
    if isinstance(arg, str):
        return arg
    return arg(data)


def field(field_name, default = None, *_):
    def _field(data):
        return data.get(field_name, default)
    return _field


def first(default = None, *_):
    def _first(data):
        if data:
            return data[0]
        return default
    return _first

def concat(*args):
    def _first(data):
        result = ""
        for arg in args:
            result += _call_data(arg, data)
        return result
    return _first


def format(fmtstring, *args):
    def _format(data):
        entries = [_call_data(arg, data) for arg in args]
        return fmtstring.format(*entries)
    return _format


def translateGender(*_):
    def _format(data):
        return {
            "Male": "männlich",
            "Female": "weiblich",
        }.get(data, "Unbekannt")
    return _format


class RuleTransformer(Transformer):
    def function(self, values):
        return values[0](*values[1:])

    def WORD(self, value):
        return RULE_FUNCTIONS[str(value)]

    def ESCAPED_STRING(self, value):
        return str(value[1:-1])

    def SIGNED_NUMBER(self, value):
        return int(value)


RULE_FUNCTIONS = {
    "field": field,
    "first": first,
    "concat": concat,
    "format": format,
    "translateGender": translateGender
}

RULE_PARSER = Lark(r"""
rule: function ("|" function)*
?value: function | ESCAPED_STRING | SIGNED_NUMBER
?function: WORD ["(" value ("," value)* ")" ]

%import common.ESCAPED_STRING
%import common.SIGNED_NUMBER
%import common.WS
%import common.WORD
%ignore WS

""", start='rule')


def parse_rule(rule_string):
    return RuleTransformer().transform(RULE_PARSER.parse(rule_string))

def create_rule_function(rule_string):
    tree = parse_rule(rule_string)
    def _parse_rule(data):
        res = data
        for fun in tree.children:
            res = fun(res)
        return res
    return _parse_rule
