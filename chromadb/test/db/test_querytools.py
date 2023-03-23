import chromadb.db.querytools as qt
import pypika


def test_value_params_default():
    t = pypika.Table("foo")
    original = (
        pypika.Query.from_(t)
        .select(t.a, t.b)
        .where(t.a == pypika.Parameter("?"))
        .where(t.b == pypika.Parameter("?"))
    )
    value_based = (
        pypika.Query.from_(t).select(t.a, t.b).where(t.a == qt.Value(42)).where(t.b == qt.Value(43))
    )
    sql, values = qt.build(value_based)
    assert sql == original.get_sql()
    assert values == (42, 43)


def test_value_params_numeric():
    t = pypika.Table("foo")
    original = (
        pypika.Query.from_(t)
        .select(t.a, t.b)
        .where(t.a == pypika.NumericParameter(1))
        .where(t.b == pypika.NumericParameter(2))
    )
    value_based = (
        pypika.Query.from_(t).select(t.a, t.b).where(t.a == qt.Value(42)).where(t.b == qt.Value(43))
    )
    sql, values = qt.build(value_based, formatstr=":{}")
    assert sql == original.get_sql()
    assert values == (42, 43)
