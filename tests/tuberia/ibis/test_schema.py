import ibis.expr.datatypes as ibis_dt

from tuberia.schema import Column, to_ibis


def test_ibis_schema():
    class schema:
        a = Column("int")
        b = Column("string")
        c = Column("array<double>")

    ibis_schema = to_ibis(schema)
    assert ibis_schema.names == ("a", "b", "c")
    assert ibis_schema.types == (
        ibis_dt.Int64(nullable=True),
        ibis_dt.String(nullable=True),
        ibis_dt.Array(value_type=ibis_dt.Float64(nullable=True), nullable=True),
    )
