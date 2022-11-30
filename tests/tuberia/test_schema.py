from tuberia.schema import column


def test_column_with_type():
    class schema:
        id = column(int)

    assert schema.id == "id"
    assert schema.id.dtype == int
    assert not schema.id.key


def test_column_with_alias():
    class schema:
        id = column(int, alias="identity")

    assert schema.id == "identity"
    assert schema.id.dtype == int
    assert not schema.id.key


def test_column_with_key():
    class schema:
        id = column(key=True)

    assert schema.id == "id"
    assert schema.id.dtype == str
    assert schema.id.key


def test_column_default_params():
    class schema:
        id = column()

    assert schema.id == "id"
    assert schema.id.dtype == str
    assert not schema.id.key


def test_nested_schema():
    class schema:
        id = column()

        class nested:
            id_nested = column()

    assert schema.id == "id"
    assert schema.nested.__name__ == "nested"
    assert schema.nested.id_nested == "id_nested"
