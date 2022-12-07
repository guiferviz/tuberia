from tuberia.schema import Column


def test_column_with_type():
    class schema:
        id = Column(int)

    assert schema.id == "id"
    assert schema.id.dtype == int
    assert not schema.id.key


def test_column_with_alias():
    class schema:
        id = Column(int, alias="identity")

    assert schema.id == "identity"
    assert schema.id.dtype == int
    assert not schema.id.key


def test_column_with_key():
    class schema:
        id = Column(key=True)

    assert schema.id == "id"
    assert schema.id.dtype == str
    assert schema.id.key


def test_column_default_params():
    class schema:
        id = Column()

    assert schema.id == "id"
    assert schema.id.dtype == str
    assert not schema.id.key


def test_nested_schema():
    class schema:
        id = Column()

        class nested:
            id_nested = Column()

    assert schema.id == "id"
    assert schema.nested.__name__ == "nested"
    assert schema.nested.id_nested == "id_nested"
