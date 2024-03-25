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


def test_boolean():
    data = {"Relation": "Father"}

    rule = 'is("Relation", "Father")'

    assert create_rule_function(rule)(data)


def test_filter():

    data = [{"Relation": "Father"}, {"Relation": "Mother"}, {"Relation": "Pet"}]
    rule = 'filter(is("Relation", "Pet"))'
    assert create_rule_function(rule)(data) == [{"Relation": "Pet"}]

def test_field_list():
    data = [{"Relation": "Father"}, {"Relation": "Mother"}, {"Relation": "Pet"}]
    rule = 'field("Relation")'
    assert create_rule_function(rule)(data) == ["Father", "Mother", "Pet"]


def test_format_joint():
    data = [{"Relation": "Father"}, {"Relation": "Mother"}, {"Relation": "Pet"}]
    rule = 'field("Relation")|join("; ")'
    assert create_rule_function(rule)(data) == "Father; Mother; Pet"


def test_sort():
    data = [5, 4, 2, 3]
    rule = 'sort'
    assert create_rule_function(rule)(data) == [2, 3, 4, 5]

    data = [5, 4, 2, 3]
    rule = 'sort("desc")'
    assert create_rule_function(rule)(data) == [5, 4, 3, 2]


def test_formatDate():
    data = ["2024-04-02", "invalid", "", None]
    rule = 'formatDate'
    assert create_rule_function(rule)(data) == ["02.04.2024", None, None, None]
