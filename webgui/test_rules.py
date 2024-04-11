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

    data = [{"Relation": "Father"}, {"Relation": "Aardvark"}]
    rule = 'sort("asc", "Relation")'
    assert create_rule_function(rule)(data) == [{"Relation": "Aardvark"}, {"Relation": "Father"}]


def test_formatDate():
    data = ["2024-04-02", "invalid", "", None]
    rule = 'formatDate'
    assert create_rule_function(rule)(data) == ["02.04.2024", None, None, None]


def test_if():
    data = [{"Relation": "Father"}]
    rule = 'if(is("Relation", "Father"), format("{} is more", field("Relation")), "No")'
    assert create_rule_function(rule)(data) == ["Father is more"]


def test_nested():
    data = [{"Birthdate": "2024-01-03"}]
    rule = 'format("Hi {}", field("Birthdate") | formatDate)'
    assert create_rule_function(rule)(data) == ["Hi 03.01.2024"]


def test_list_conditional():
    data = [1, 2, 3]
    rule = 'any(eq(1))'

    assert create_rule_function(rule)(data) is True

    data = [4, 4, 4]
    rule = 'all(eq(4))'

    assert create_rule_function(rule)(data) is True


def test_list_dict():
    data = [{"RelationToIndex": "Mother"}]
    rule = 'if(and(any(is("RelationToIndex", "Mother")), all(not(is("RelationToIndex", "Father")))),  "Vater", " ")'
    assert create_rule_function(rule)(data) == "Vater"


def test_assert_all_same():
    data = ["COL1A1", "COL1A1"]
    rule = 'set | len | eq(1)'
    assert create_rule_function(rule)(data)

    data = ["COL1A1", "COL1A2"]
    rule = 'set | len | eq(1)'
    assert not create_rule_function(rule)(data)
