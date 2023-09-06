import string
from typing import Any
from typing_extensions import get_args
from hypothesis import strategies as st, given
from hypothesis.strategies import SearchStrategy

from chromadb import Where
from chromadb.types import WhereOperator, InclusionExclusionOperator
from chromadb.utils.query_helper import WhereFilter


def join_to_str(terms: Any) -> str:
    return " ".join(
        [
            f'"{term}"'
            if isinstance(term, str)
            and term
            not in ["==", "!=", ">", ">=", "<", "<=", "and", "or", "in", "not in"]
            else str(term)
            for term in terms
        ]
    )


def join_with_logical(terms: Any, operator: str) -> str:
    return f"({f' {operator} '.join(terms)})"


equal_operators = st.sampled_from(["==", "!="])
comp_operators = st.sampled_from([">", ">=", "<", "<="])
logical_operators = st.sampled_from(["and", "or"])
set_operators = st.sampled_from(["in", "not in"])

set_values = st.one_of(
    st.lists(st.integers(), min_size=1, max_size=4),
    st.lists(st.text(), min_size=1, max_size=4),
    st.lists(st.booleans(), min_size=1, max_size=4),
)


def escape_special_chars(s: str) -> str:
    return s.encode("unicode_escape").decode()


def filter_quotes(s: str) -> bool:
    return '"' not in s and "'''" not in s and '"""' not in s


escaped_text = st.text().map(escape_special_chars).filter(filter_quotes)

values = st.one_of(st.integers(), escaped_text, st.booleans())

valid_chars = string.digits + string.ascii_letters + "_-"
variables = st.text(alphabet=valid_chars, min_size=1, max_size=5).filter(
    lambda x: x[0]
    not in ["==", "!=", ">", ">=", "<", "<=", "and", "or", "in", "not in"]
)

basic_expr_equal = st.tuples(variables, equal_operators, values).map(join_to_str)
basic_expr_comp = st.tuples(variables, comp_operators, st.integers()).map(join_to_str)
basic_expr_set = st.tuples(variables, set_operators, set_values).map(
    lambda x: f'"{x[0]}" {x[1]} {x[2]}'
)

basic_expr = st.one_of(basic_expr_equal, basic_expr_comp, basic_expr_set)


class ComplexExprGen:
    def __init__(self, logical_ops: SearchStrategy[str]) -> None:
        self.logical_ops = logical_ops

    def __call__(self, exprs: Any) -> SearchStrategy[str]:
        return st.tuples(
            st.lists(exprs, min_size=2, max_size=4), st.one_of(self.logical_ops)
        ).map(lambda x: join_with_logical(x[0], x[1]))


final_expr_strategy = st.recursive(
    basic_expr, ComplexExprGen(logical_operators), max_leaves=10
)

allowed_operators = set(
    x.__args__[0]
    for x in get_args(WhereOperator) + get_args(InclusionExclusionOperator)
)


def validate_where_expression(expr: Where) -> bool:
    for k, v in expr.items():
        if k in ["$and", "$or"]:
            if not isinstance(v, list):
                return False
            for sub_expr in v:
                if not validate_where_expression(sub_expr):
                    return False
        else:
            if isinstance(v, dict):
                if not all(op in allowed_operators for op in v.keys()):
                    return False
            elif not isinstance(v, (str, int, float, bool)):
                return False
    return True


@given(expr=final_expr_strategy)
def test_expr(expr: str) -> None:
    v = validate_where_expression(WhereFilter(expr))  # type: ignore
    assert v is True, f"Failed for {expr}"
