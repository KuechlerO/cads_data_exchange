import datetime
from functools import reduce, wraps
from typing import Any, List
from lark import Lark
from lark import Transformer


def _call_data(arg, data):
    if isinstance(arg, str):
        return arg
    elif arg is None:
        return data
    return arg(data)


def iterable(f):
    @wraps(f)
    def _iterable(arg, *_):
        if isinstance(arg, list):
            return [f(elem) for elem in arg]
        return f(arg)

    return _iterable


def field(field_name, default = None, *_):
    """Select field from data by name."""
    @iterable
    def _field(data):
        if data is None:
            return data
        return data.get(field_name, default)
    return _field


def first(default = None, *_):
    """Select first item from list."""
    def _first(data):
        if data:
            return data[0]
        return default
    return _first

def concat(*args):
    """Concatenate multiple fields from input arguments."""
    def _first(data):
        result = ""
        for arg in args:
            result += _call_data(arg, data) or ""
        return result
    return _first


def join(separator):
    """Join input list using separator symbol."""
    def _join(elements):
        return separator.join(elements)
    return _join


def format(fmtstring, *args):
    """Format arguments using a python format string, e.g. format("{}", ...)."""

    @iterable
    def _format(data):
        entries = [_call_data(arg, data) for arg in args]
        return fmtstring.format(*entries)
    return _format


def formatDate(input_fmt=None, output_fmt=None):
    """Format a date in YYYY-MM-DD into DD.MM.YYYY. This can be further refined using syntax defined in https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes."""
    if input_fmt is None:
        input_fmt = "%Y-%m-%d"
    if output_fmt is None:
        output_fmt = "%d.%m.%Y"

    @iterable
    def _formatDate(entry):
        try:
            date_entry = datetime.datetime.strptime(entry, input_fmt)
        except (ValueError, TypeError):
            return None
        output = date_entry.date().strftime(output_fmt)
        return output

    return _formatDate

def translateGender(*_):
    """Maps english gender terms to german ones."""
    def _format(data):
        return {
            "Male": "männlich",
            "Female": "weiblich",
        }.get(data, "Unbekannt")
    return _format

def translateRelation(*_):
    """Maps english relation terms to german ones."""
    def _format(data):
        return {
            "Father": "Vater",
            "Mother": "Mutter",
            "Sister": "Schwester",
            "Brother": "Bruder",
        }.get(data, "Unbekannt")
    return _format


def testIs(field, value):
    """Test if a given field is of a certain value."""
    def _testIs(data) -> bool:
        return data.get(field) == value
    return _testIs


def testNot(arg):
    """Test if a given argument is not."""
    def _testNot(data) -> bool:
        return not arg(data)
    return _testNot

def testOr(*arg):
    """Test if any argument is true."""
    def _testOr(data) -> bool:
        return any(a(data) for a in arg)
    return _testOr

def testAnd(*arg):
    """Test if all arguments are true."""
    def _testAnd(data) -> bool:
        return all(a(data) for a in arg)
    return _testAnd


def funFilter(prefix):
    """Filter list of entries based on function."""
    def _filter(entries) -> List[Any]:
        return [e for e in entries if prefix(e)]
    return _filter


def funSort(direction = None, key = None):
    """Sort list of entries."""
    if not direction:
        direction = "asc"
    reverse = {"asc": False, "desc": True}[direction]

    if key is not None:
        keyfun = lambda k: k.get(key)
    else:
        keyfun = None

    def _sort(entries) -> List[Any]:
        return sorted(entries, key=keyfun, reverse=reverse)

    return _sort


def ifCondition(test, ifConsequence, elseConsequence):
    """Test prefix and either choose branch A or branch B."""

    @iterable
    def _if(data):
        if test(data):
            if isinstance(ifConsequence, str) or ifConsequence is None:
                return ifConsequence
            return ifConsequence(data)
        else:
            if isinstance(elseConsequence, str) or elseConsequence is None:
                return elseConsequence
            return elseConsequence(data)
    return _if


def pipe(funs):
    def _pipe(data):
        result = data
        for fun in funs:
            result = fun(result)
        return result
    return _pipe



class RuleTransformer(Transformer):
    def function(self, values):
        return values[0](*values[1:])

    def value(self, values):
        if isinstance(values, list):
            return pipe(values)
        return values

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
    "translateGender": translateGender,
    "translateRelation": translateRelation,
    "is": testIs,
    "not": testNot,
    "and": testAnd,
    "or": testOr,
    "filter": funFilter,
    "join": join,
    "sort": funSort,
    "formatDate": formatDate,
    "if": ifCondition,
}

RULE_PARSER = Lark(r"""
rule: function ("|" function)*
?value: function ("|" function)* | ESCAPED_STRING | SIGNED_NUMBER
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
        if res is None:
            res = ""
        return res
    return _parse_rule
