from chromadb.db.base import ParameterValue, get_sql
import pypika


def test_value_params_default() -> None:
    t = pypika.Table("foo")

    original_query = (
        pypika.Query.from_(t)
        .select(t.a, t.b)
        .where(t.a == pypika.Parameter("?"))
        .where(t.b == pypika.Parameter("?"))
    )

    value_based_query = (
        pypika.Query.from_(t)
        .select(t.a, t.b)
        .where(t.a == ParameterValue(42))
        .where(t.b == ParameterValue(43))
    )
    sql, values = get_sql(value_based_query)
    assert sql == original_query.get_sql()
    assert values == (42, 43)


def test_value_params_numeric() -> None:
    t = pypika.Table("foo")
    original_query = (
        pypika.Query.from_(t)
        .select(t.a, t.b)
        .where(t.a == pypika.NumericParameter(1))
        .where(t.b == pypika.NumericParameter(2))
    )
    value_based_query = (
        pypika.Query.from_(t)
        .select(t.a, t.b)
        .where(t.a == ParameterValue(42))
        .where(t.b == ParameterValue(43))
    )
    sql, values = get_sql(value_based_query, formatstr=":{}")
    assert sql == original_query.get_sql()
    assert values == (42, 43)
