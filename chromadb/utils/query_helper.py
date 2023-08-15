import ast
import re
import sys
import time
import traceback
from typing import Any, Dict, cast, get_args

from chromadb.types import (
    WhereOperator,
    LiteralValue,
    WhereDocument,
    Where,
)


def extract_conditions(line):
    # The regex pattern matches the conditions inside the where function
    print(f"Extracting conditions from: {line}")
    pattern = r"(where\_document|where)\((.*)\)"
    match = re.search(pattern, line)
    print(f"Match: {match.group(2)}")
    if match:
        return match.group(2)
    return None


def _map_ast_operator_to_where_operator(operator: ast.operator) -> WhereOperator:
    if isinstance(operator, ast.Eq):
        return "$eq"
    elif isinstance(operator, ast.NotEq):
        return "$ne"
    elif isinstance(operator, ast.Gt):
        return "$gt"
    elif isinstance(operator, ast.GtE):
        return "$gte"
    elif isinstance(operator, ast.Lt):
        return "$lt"
    elif isinstance(operator, ast.LtE):
        return "$lte"
    elif isinstance(operator, ast.In):
        return "$in"
    elif isinstance(operator, ast.NotIn):
        return "$nin"
    else:
        raise ValueError(f"Unsupported operator: {operator}")


def _process_ast_wd(node: Any) -> Dict[str, Any]:
    pass


def _process_ast(node: Any) -> Dict[str, Any]:
    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return {"$and": [_process_ast(value) for value in node.values]}
        elif isinstance(node.op, ast.Or):
            return {"$or": [_process_ast(value) for value in node.values]}
    elif isinstance(node, ast.Compare):
        if not isinstance(node.left.value, str):
            # TODO throw exception that lhs must always be a str which is an attribute in the metadata
            raise ValueError(
                f"Unsupported left hand side type: {type(node.left.value)}. Must be a string."
            )
        left = node.left.s
        operator = node.ops[0]
        # print(f"operator: {operator}")
        right = node.comparators[0]
        if not isinstance(
            operator,
            (ast.Eq, ast.NotEq, ast.In, ast.NotIn, ast.Gt, ast.GtE, ast.Lt, ast.LtE),
        ):
            raise ValueError(f"Unsupported operator: {operator}")
        if isinstance(right, (ast.Str, ast.Num, ast.Constant)):
            right_value = right.value
        elif isinstance(right, ast.List):
            right_value = [_process_ast(value) for value in right.elts]
        if isinstance(
            operator,
            (ast.Eq, ast.NotEq, ast.In, ast.NotIn, ast.Gt, ast.GtE, ast.Lt, ast.LtE),
        ):
            return {
                f"{left}": {
                    f"{_map_ast_operator_to_where_operator(operator)}": right_value
                }
            }
        else:
            raise ValueError(
                f"Unsupported right hand side type: {type(right)}. Must be a string or a list of strings."
            )
    elif isinstance(node, ast.Module):
        return _process_ast(node.body[0])
    elif isinstance(node, ast.Expr):
        return _process_ast(node.value)
    elif isinstance(node, get_args(LiteralValue)):
        return node.value
    elif isinstance(node, ast.Constant) and isinstance(
        node.value, get_args(LiteralValue)
    ):
        return cast(type(node.value), node.value)
    raise ValueError(f"Unsupported node type: {type(node)}")


class Filter(Dict[str, Any]):
    @staticmethod
    def where(_: Any) -> Where:
        stack = traceback.extract_stack()
        _exp = extract_conditions(stack[:-1][0].line)[0:-1]
        print(f"Evaluating: {_exp}")
        _filter_expr = _process_ast(ast.parse(_exp))
        return cast(Where, _filter_expr)

    @staticmethod
    def where_document(prm: Any) -> WhereDocument:
        stack = traceback.extract_stack()
        _exp = extract_conditions(stack[:-1][0].line)[0:-1]
        print(f"Evaluating: {_exp}")
        _filter_expr = _process_ast(ast.parse(_exp))
        return cast(Where, _filter_expr)


if __name__ == "__main__":
    start_time = time.time()
    print(Filter.where("k" == "10" and ("p" == "x" or "p" == "y")))
    print(Filter.where("k" == "10" and "p" == "x" or "p" == "y"))
    print(Filter.where("k" in [1, 2, 3, 4]))
    print(Filter.where("m" in ["a", "b", "c"]))
    print(Filter.where("k" not in ["a", "b", "c"]))
    try:
        print(Filter.where(1 in ["a", "b", "c"]))
    except Exception as e:
        print(e, file=sys.stdout)
    print(Filter.where("a" != "b"))
    try:
        print(Filter.where(not "a"))
    except Exception as e:
        print(e, file=sys.stdout)

    print(Filter.where("a" == True))
    print(Filter.where("a" in [True, False]))
    end_time = time.time()

    elapsed_time = end_time - start_time
    print(f"Function executed in: {elapsed_time} seconds")

    # print(Filter.where_document(True) in ["a", "b", "c"])
