from .rules import parse_rule, create_rule_function


def test_simple_rule():
    data = {"Name": "Test"}
    rule = "field(\"Name\")"
    expected = "Test"
    fun = create_rule_function(rule)

    assert fun(data) == expected


def test_simple_concat():
    data = {"First": "Ein", "Last": "horn"}
    rule = "concat(field(\"First\"), field(\"Last\"))"
    expected = "Einhorn"

    assert create_rule_function(rule)(data) == expected


def test_sort():
    data = [2, 1, 3, 4]

    rule = "sort | index(0)"

    expected = 1

    assert create_rule_function(rule)(data) == expected
