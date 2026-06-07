# type: ignore
import numpy as np
import pytest


class TestSearchDictSupport:
    """Test Search class dict input support."""

    def test_search_with_dict_where(self):
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import Where

        search = Search(where={"status": "active"})
        assert search._where is not None
        assert isinstance(search._where, Where)
        search = Search(where={"$and": [{"status": "active"}, {"score": {"$gt": 0.5}}]})
        assert search._where is not None

    def test_search_with_dict_rank(self):
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import Rank

        search = Search(rank={"$knn": {"query": [0.1, 0.2]}})
        assert search._rank is not None
        assert isinstance(search._rank, Rank)
        search = Search(rank={"$val": 0.5})
        assert search._rank is not None

    def test_search_with_dict_limit(self):
        from chromadb.execution.expression.plan import Search

        search = Search(limit={"limit": 10, "offset": 5})
        assert search._limit.limit == 10
        assert search._limit.offset == 5
        search = Search(limit=10)
        assert search._limit.limit == 10
        assert search._limit.offset == 0

    def test_search_with_dict_select(self):
        from chromadb.execution.expression.plan import Search

        search = Search(select={"keys": ["#document", "#score"]})
        assert search._select is not None
        search = Search(select=["#document", "#metadata"])
        assert search._select is not None
        search = Search(select={"#document", "#embedding"})
        assert search._select is not None

    def test_search_mixed_inputs(self):
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import Key

        search = Search(
            where=Key("status") == "active",
            rank={"$knn": {"query": [0.1, 0.2]}},
            limit=10,
            select=["#document"],
        )
        assert search._where is not None
        assert search._rank is not None
        assert search._limit.limit == 10
        assert search._select is not None

    def test_search_builder_methods_with_dicts(self):
        from chromadb.execution.expression.plan import Search

        search = Search().where({"status": "active"}).rank({"$val": 0.5})
        assert search._where is not None
        assert search._rank is not None

    def test_search_invalid_inputs(self):
        import pytest
        from chromadb.execution.expression.plan import Search

        with pytest.raises(TypeError, match="where must be"):
            Search(where="invalid")
        with pytest.raises(TypeError, match="rank must be"):
            Search(rank=0.5)
        with pytest.raises(TypeError, match="limit must be"):
            Search(limit="10")
        with pytest.raises(TypeError, match="select must be"):
            Search(select=123)

    def test_search_with_group_by(self):
        import pytest
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import GroupBy, MinK, Key

        search = Search(
            group_by={
                "keys": ["category"],
                "aggregate": {"$min_k": {"keys": ["#score"], "k": 3}},
            }
        )
        assert isinstance(search._group_by, GroupBy)
        group_by = GroupBy(keys=Key("category"), aggregate=MinK(keys=Key.SCORE, k=3))
        assert Search(group_by=group_by)._group_by is group_by
        assert Search().group_by(group_by)._group_by.aggregate is not None
        with pytest.raises(TypeError, match="group_by must be"):
            Search(group_by="invalid")
        with pytest.raises(ValueError, match="requires 'aggregate' field"):
            Search(group_by={"keys": ["category"]})

    def test_search_group_by_serialization(self):
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import GroupBy, MinK, Key, Knn

        search = Search().rank(Knn(query=[0.1, 0.2])).limit(10)
        assert search.to_dict()["group_by"] == {}
        search = Search().group_by(
            GroupBy(keys=Key("category"), aggregate=MinK(keys=Key.SCORE, k=3))
        )
        result = search.to_dict()["group_by"]
        assert result["keys"] == ["category"]
        assert result["aggregate"] == {"$min_k": {"keys": ["#score"], "k": 3}}


class TestWhereFromDict:
    """Test Where.from_dict() conversion."""

    def test_simple_equality(self):
        from chromadb.execution.expression.operator import Where, Eq

        where = Where.from_dict({"status": "active"})
        assert isinstance(where, Eq)
        where = Where.from_dict({"status": {"$eq": "active"}})
        assert isinstance(where, Eq)

    def test_comparison_operators(self):
        from chromadb.execution.expression.operator import Where, Ne, Gt, Gte, Lt, Lte

        where = Where.from_dict({"status": {"$ne": "inactive"}})
        assert isinstance(where, Ne)
        where = Where.from_dict({"score": {"$gt": 0.5}})
        assert isinstance(where, Gt)
        where = Where.from_dict({"score": {"$gte": 0.5}})
        assert isinstance(where, Gte)
        where = Where.from_dict({"score": {"$lt": 1.0}})
        assert isinstance(where, Lt)
        where = Where.from_dict({"score": {"$lte": 1.0}})
        assert isinstance(where, Lte)

    def test_membership_operators(self):
        from chromadb.execution.expression.operator import Where, In, Nin

        where = Where.from_dict({"status": {"$in": ["active", "pending"]}})
        assert isinstance(where, In)
        where = Where.from_dict({"status": {"$nin": ["deleted", "archived"]}})
        assert isinstance(where, Nin)

    def test_string_operators(self):
        from chromadb.execution.expression.operator import (
            Where,
            Contains,
            NotContains,
            Regex,
            NotRegex,
        )

        where = Where.from_dict({"text": {"$contains": "hello"}})
        assert isinstance(where, Contains)
        where = Where.from_dict({"text": {"$not_contains": "spam"}})
        assert isinstance(where, NotContains)
        where = Where.from_dict({"text": {"$regex": "^test.*"}})
        assert isinstance(where, Regex)
        where = Where.from_dict({"text": {"$not_regex": r"\d+"}})
        assert isinstance(where, NotRegex)

    def test_array_contains_operators(self):
        from chromadb.execution.expression.operator import Where, Contains, NotContains

        where = Where.from_dict({"tags": {"$contains": "action"}})
        assert isinstance(where, Contains)
        assert where.to_dict() == {"tags": {"$contains": "action"}}
        where = Where.from_dict({"scores": {"$contains": 42}})
        assert isinstance(where, Contains)
        assert where.to_dict() == {"scores": {"$contains": 42}}
        where = Where.from_dict({"ratings": {"$contains": 4.5}})
        assert isinstance(where, Contains)
        assert where.to_dict() == {"ratings": {"$contains": 4.5}}
        where = Where.from_dict({"flags": {"$contains": True}})
        assert isinstance(where, Contains)
        assert where.to_dict() == {"flags": {"$contains": True}}
        where = Where.from_dict({"tags": {"$not_contains": "draft"}})
        assert isinstance(where, NotContains)
        assert where.to_dict() == {"tags": {"$not_contains": "draft"}}
        where = Where.from_dict({"scores": {"$not_contains": 0}})
        assert isinstance(where, NotContains)
        assert where.to_dict() == {"scores": {"$not_contains": 0}}

    def test_array_contains_invalid_operands(self):
        import pytest
        from chromadb.execution.expression.operator import Where

        with pytest.raises(TypeError, match="\\$contains requires"):
            Where.from_dict({"tags": {"$contains": [1, 2]}})
        with pytest.raises(TypeError, match="\\$not_contains requires"):
            Where.from_dict({"tags": {"$not_contains": {"nested": True}}})

    def test_array_contains_round_trip(self):
        from chromadb.execution.expression.operator import Where, Key

        cases = [
            Key("tags").contains("action"),
            Key("scores").contains(42),
            Key("ratings").contains(4.5),
            Key("flags").contains(True),
            Key("tags").not_contains("draft"),
        ]
        for original in cases:
            d = original.to_dict()
            restored = Where.from_dict(d)
            assert restored.to_dict() == d, f"Round-trip failed for {d}"

    def test_array_contains_in_composite(self):
        from chromadb.execution.expression.operator import Where, And, Key

        where = (Key("tags").contains("action")) & (Key("year") > 2020)
        assert isinstance(where, And)
        d = where.to_dict()
        restored = Where.from_dict(d)
        assert restored.to_dict() == d

    def test_document_contains_requires_string(self):
        import pytest
        from chromadb.execution.expression.operator import Key

        expr = Key.DOCUMENT.contains("hello")
        assert expr.to_dict() == {"#document": {"$contains": "hello"}}
        expr = Key.DOCUMENT.not_contains("hello")
        assert expr.to_dict() == {"#document": {"$not_contains": "hello"}}
        for bad_value in [42, 3.14, True, False]:
            with pytest.raises(
                TypeError, match="\\$contains on #document requires a string"
            ):
                Key.DOCUMENT.contains(bad_value)
            with pytest.raises(
                TypeError, match="\\$not_contains on #document requires a string"
            ):
                Key.DOCUMENT.not_contains(bad_value)
        assert Key("scores").contains(42).to_dict() == {"scores": {"$contains": 42}}
        assert Key("flags").not_contains(True).to_dict() == {
            "flags": {"$not_contains": True}
        }

    def test_logical_operators(self):
        from chromadb.execution.expression.operator import Where, And, Or

        where = Where.from_dict(
            {"$and": [{"status": "active"}, {"score": {"$gt": 0.5}}]}
        )
        assert isinstance(where, And)
        where = Where.from_dict({"$or": [{"status": "active"}, {"status": "pending"}]})
        assert isinstance(where, Or)

    def test_nested_logical_operators(self):
        from chromadb.execution.expression.operator import Where, And

        where = Where.from_dict(
            {
                "$and": [
                    {"$or": [{"status": "active"}, {"status": "pending"}]},
                    {"score": {"$gte": 0.5}},
                ]
            }
        )
        assert isinstance(where, And)

    def test_special_keys(self):
        from chromadb.execution.expression.operator import Where, In

        where = Where.from_dict({"#id": {"$in": ["id1", "id2"]}})
        assert isinstance(where, In)

    def test_invalid_where_dicts(self):
        import pytest
        from chromadb.execution.expression.operator import Where

        with pytest.raises(TypeError, match="Expected dict"):
            Where.from_dict("not a dict")
        with pytest.raises(ValueError, match="cannot be empty"):
            Where.from_dict({})
        with pytest.raises(ValueError, match="requires at least one condition"):
            Where.from_dict({"$and": []})


class TestRankFromDict:
    """Test Rank.from_dict() conversion."""

    def test_val_conversion(self):
        from chromadb.execution.expression.operator import Rank, Val

        rank = Rank.from_dict({"$val": 0.5})
        assert isinstance(rank, Val)
        assert rank.value == 0.5

    def test_knn_conversion(self):
        import numpy as np
        from chromadb.execution.expression.operator import Rank, Knn

        rank = Rank.from_dict({"$knn": {"query": [0.1, 0.2]}})
        assert isinstance(rank, Knn)
        if isinstance(rank.query, np.ndarray):
            assert np.allclose(rank.query, np.array([0.1, 0.2]))
        else:
            assert rank.query == [0.1, 0.2]
        assert rank.key == "#embedding"
        assert rank.limit == 16
        rank = Rank.from_dict(
            {
                "$knn": {
                    "query": [0.1, 0.2],
                    "key": "sparse_embedding",
                    "limit": 256,
                    "return_rank": True,
                }
            }
        )
        assert rank.key == "sparse_embedding"
        assert rank.limit == 256
        assert rank.return_rank

    def test_arithmetic_operators(self):
        from chromadb.execution.expression.operator import Rank, Sum, Sub, Mul, Div

        rank = Rank.from_dict({"$sum": [{"$val": 0.5}, {"$val": 0.3}]})
        assert isinstance(rank, Sum)
        rank = Rank.from_dict({"$sub": {"left": {"$val": 1.0}, "right": {"$val": 0.3}}})
        assert isinstance(rank, Sub)
        rank = Rank.from_dict({"$mul": [{"$val": 2.0}, {"$val": 0.5}]})
        assert isinstance(rank, Mul)
        rank = Rank.from_dict({"$div": {"left": {"$val": 1.0}, "right": {"$val": 2.0}}})
        assert isinstance(rank, Div)

    def test_math_functions(self):
        from chromadb.execution.expression.operator import Rank, Abs, Exp, Log

        rank = Rank.from_dict({"$abs": {"$val": -0.5}})
        assert isinstance(rank, Abs)
        rank = Rank.from_dict({"$exp": {"$val": 1.0}})
        assert isinstance(rank, Exp)
        rank = Rank.from_dict({"$log": {"$val": 2.0}})
        assert isinstance(rank, Log)

    def test_aggregation_functions(self):
        from chromadb.execution.expression.operator import Rank, Max, Min

        rank = Rank.from_dict({"$max": [{"$val": 0.5}, {"$val": 0.8}]})
        assert isinstance(rank, Max)
        rank = Rank.from_dict({"$min": [{"$val": 0.5}, {"$val": 0.8}]})
        assert isinstance(rank, Min)

    def test_complex_rank_expression(self):
        from chromadb.execution.expression.operator import Rank, Sum

        rank = Rank.from_dict(
            {
                "$sum": [
                    {"$mul": [{"$knn": {"query": [0.1, 0.2]}}, {"$val": 0.8}]},
                    {"$mul": [{"$val": 0.5}, {"$val": 0.2}]},
                ]
            }
        )
        assert isinstance(rank, Sum)

    def test_invalid_rank_dicts(self):
        import pytest
        from chromadb.execution.expression.operator import Rank

        with pytest.raises(TypeError, match="Expected dict"):
            Rank.from_dict("not a dict")
        with pytest.raises(ValueError, match="cannot be empty"):
            Rank.from_dict({})
        with pytest.raises(ValueError, match="exactly one operator"):
            Rank.from_dict({"$val": 0.5, "$knn": {"query": [0.1]}})
        with pytest.raises(TypeError, match="requires a number"):
            Rank.from_dict({"$val": "not a number"})


class TestLimitFromDict:
    """Test Limit.from_dict() conversion."""

    def test_limit_only(self):
        from chromadb.execution.expression.operator import Limit

        limit = Limit.from_dict({"limit": 20})
        assert limit.limit == 20
        assert limit.offset == 0

    def test_offset_only(self):
        from chromadb.execution.expression.operator import Limit

        limit = Limit.from_dict({"offset": 10})
        assert limit.offset == 10
        assert limit.limit is None

    def test_limit_and_offset(self):
        from chromadb.execution.expression.operator import Limit

        limit = Limit.from_dict({"limit": 20, "offset": 10})
        assert limit.limit == 20
        assert limit.offset == 10

    def test_validation(self):
        import pytest
        from chromadb.execution.expression.operator import Limit

        with pytest.raises(ValueError, match="must be positive"):
            Limit.from_dict({"limit": -1})
        with pytest.raises(ValueError, match="must be positive"):
            Limit.from_dict({"limit": 0})
        with pytest.raises(ValueError, match="must be non-negative"):
            Limit.from_dict({"offset": -1})

    def test_invalid_types(self):
        import pytest
        from chromadb.execution.expression.operator import Limit

        with pytest.raises(TypeError, match="Expected dict"):
            Limit.from_dict("not a dict")
        with pytest.raises(TypeError, match="must be an integer"):
            Limit.from_dict({"limit": "20"})
        with pytest.raises(TypeError, match="must be an integer"):
            Limit.from_dict({"offset": 10.5})

    def test_unexpected_keys(self):
        import pytest
        from chromadb.execution.expression.operator import Limit

        with pytest.raises(ValueError, match="Unexpected keys"):
            Limit.from_dict({"limit": 10, "invalid": "key"})


class TestSelectFromDict:
    """Test Select.from_dict() conversion."""

    def test_special_keys(self):
        from chromadb.execution.expression.operator import Select, Key

        select = Select.from_dict(
            {"keys": ["#document", "#embedding", "#metadata", "#score"]}
        )
        assert Key.DOCUMENT in select.keys
        assert Key.EMBEDDING in select.keys
        assert Key.METADATA in select.keys
        assert Key.SCORE in select.keys

    def test_metadata_keys(self):
        from chromadb.execution.expression.operator import Select, Key

        select = Select.from_dict({"keys": ["title", "author", "date"]})
        assert Key("title") in select.keys
        assert Key("author") in select.keys
        assert Key("date") in select.keys

    def test_mixed_keys(self):
        from chromadb.execution.expression.operator import Select, Key

        select = Select.from_dict({"keys": ["#document", "title", "#score"]})
        assert Key.DOCUMENT in select.keys
        assert Key("title") in select.keys
        assert Key.SCORE in select.keys

    def test_empty_keys(self):
        from chromadb.execution.expression.operator import Select

        select = Select.from_dict({"keys": []})
        assert len(select.keys) == 0

    def test_validation(self):
        import pytest
        from chromadb.execution.expression.operator import Select

        with pytest.raises(TypeError, match="Expected dict"):
            Select.from_dict("not a dict")
        with pytest.raises(TypeError, match="must be a list/tuple/set"):
            Select.from_dict({"keys": "not a list"})
        with pytest.raises(TypeError, match="must be a string"):
            Select.from_dict({"keys": [123]})

    def test_unexpected_keys(self):
        import pytest
        from chromadb.execution.expression.operator import Select

        with pytest.raises(ValueError, match="Unexpected keys"):
            Select.from_dict({"keys": [], "invalid": "key"})


class TestRoundTripConversion:
    """Test that to_dict() and from_dict() round-trip correctly."""

    def test_where_round_trip(self):
        from chromadb.execution.expression.operator import Where, And, Key

        original = And([Key("status") == "active", Key("score") > 0.5])
        dict_form = original.to_dict()
        restored = Where.from_dict(dict_form)
        assert restored.to_dict() == dict_form

    def test_rank_round_trip(self):
        import numpy as np
        from chromadb.execution.expression.operator import Rank, Knn, Val

        original = Knn(query=[0.1, 0.2]) * 0.8 + Val(0.5) * 0.2
        dict_form = original.to_dict()
        restored = Rank.from_dict(dict_form)
        restored_dict = restored.to_dict()

        def compare_dicts(d1, d2):
            if isinstance(d1, dict) and isinstance(d2, dict):
                if "$knn" in d1 and "$knn" in d2:
                    knn1, knn2 = d1["$knn"], d2["$knn"]
                    if "query" in knn1 and "query" in knn2:
                        q1 = np.array(knn1["query"], dtype=np.float32)
                        q2 = np.array(knn2["query"], dtype=np.float32)
                        if not np.allclose(q1, q2):
                            return False
                        for key in knn1:
                            if key != "query" and knn1[key] != knn2.get(key):
                                return False
                        return True
                if set(d1.keys()) != set(d2.keys()):
                    return False
                for key in d1:
                    if not compare_dicts(d1[key], d2[key]):
                        return False
                return True
            elif isinstance(d1, list) and isinstance(d2, list):
                if len(d1) != len(d2):
                    return False
                return all(compare_dicts(a, b) for a, b in zip(d1, d2))
            else:
                return d1 == d2

        assert compare_dicts(restored_dict, dict_form)

    def test_limit_round_trip(self):
        from chromadb.execution.expression.operator import Limit

        original = Limit(limit=20, offset=10)
        dict_form = original.to_dict()
        restored = Limit.from_dict(dict_form)
        assert restored.to_dict() == dict_form

    def test_select_round_trip(self):
        from chromadb.execution.expression.operator import Select, Key

        original = Select(keys={Key.DOCUMENT, Key("title"), Key.SCORE})
        dict_form = original.to_dict()
        restored = Select.from_dict(dict_form)
        assert set(restored.to_dict()["keys"]) == set(dict_form["keys"])

    def test_search_round_trip(self):
        import numpy as np
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import Key, Knn, Limit, Select

        original_search = Search(
            where=Key("status") == "active",
            rank=Knn(query=[0.1, 0.2]),
            limit=Limit(limit=10),
            select=Select(keys={Key.DOCUMENT}),
        )
        search_dict = original_search.to_dict()
        new_search = Search(
            where=search_dict["filter"] if search_dict["filter"] else None,
            rank=search_dict["rank"] if search_dict["rank"] else None,
            limit=search_dict["limit"],
            select=search_dict["select"],
        )
        new_dict = new_search.to_dict()

        def compare_search_dicts(d1, d2):
            if isinstance(d1, dict) and isinstance(d2, dict):
                if "rank" in d1 and "rank" in d2:
                    rank1, rank2 = d1["rank"], d2["rank"]
                    if isinstance(rank1, dict) and isinstance(rank2, dict):
                        if "$knn" in rank1 and "$knn" in rank2:
                            knn1, knn2 = rank1["$knn"], rank2["$knn"]
                            if "query" in knn1 and "query" in knn2:
                                q1 = np.array(knn1["query"], dtype=np.float32)
                                q2 = np.array(knn2["query"], dtype=np.float32)
                                if not np.allclose(q1, q2):
                                    return False
                                for key in knn1:
                                    if key != "query" and knn1[key] != knn2.get(key):
                                        return False
                                for key in d1:
                                    if key != "rank" and d1[key] != d2.get(key):
                                        return False
                                return True
                if set(d1.keys()) != set(d2.keys()):
                    return False
                for key in d1:
                    if isinstance(d1[key], dict) and isinstance(d2[key], dict):
                        if not compare_search_dicts(d1[key], d2[key]):
                            return False
                    elif d1[key] != d2[key]:
                        return False
                return True
            else:
                return d1 == d2

        assert compare_search_dicts(new_dict, search_dict)

    def test_search_round_trip_with_group_by(self):
        from chromadb.execution.expression.plan import Search
        from chromadb.execution.expression.operator import Key, GroupBy, MinK

        original = Search(
            where=Key("status") == "active",
            group_by=GroupBy(
                keys=[Key("category")],
                aggregate=MinK(keys=[Key.SCORE], k=3),
            ),
        )
        search_dict = original.to_dict()
        assert search_dict["group_by"]["keys"] == ["category"]
        assert search_dict["group_by"]["aggregate"] == {
            "$min_k": {"keys": ["#score"], "k": 3}
        }
        restored = Search(group_by=GroupBy.from_dict(search_dict["group_by"]))
        assert restored.to_dict()["group_by"] == search_dict["group_by"]


class TestGroupByFromDict:
    """Test GroupBy.from_dict() conversion."""

    def test_group_by_serialization(self) -> None:
        import pytest
        from chromadb.execution.expression.operator import (
            GroupBy,
            MinK,
            MaxK,
            Key,
            Aggregate,
        )

        group_by = GroupBy(keys=Key("category"), aggregate=MinK(keys=Key.SCORE, k=3))
        assert group_by.to_dict() == {
            "keys": ["category"],
            "aggregate": {"$min_k": {"keys": ["#score"], "k": 3}},
        }
        group_by = GroupBy(
            keys=[Key("year"), Key("category")],
            aggregate=MaxK(keys=[Key.SCORE, Key("priority")], k=5),
        )
        assert group_by.to_dict() == {
            "keys": ["year", "category"],
            "aggregate": {"$max_k": {"keys": ["#score", "priority"], "k": 5}},
        }
        original = GroupBy(keys=[Key("category")], aggregate=MinK(keys=[Key.SCORE], k=3))
        assert GroupBy.from_dict(original.to_dict()).to_dict() == original.to_dict()
        empty_group_by = GroupBy()
        assert empty_group_by.to_dict() == {}
        assert GroupBy.from_dict({}).to_dict() == {}
        with pytest.raises(ValueError, match="requires 'keys' field"):
            GroupBy.from_dict({"aggregate": {"$min_k": {"keys": ["#score"], "k": 3}}})
        with pytest.raises(ValueError, match="requires 'aggregate' field"):
            GroupBy.from_dict({"keys": ["category"]})
        with pytest.raises(ValueError, match="keys cannot be empty"):
            GroupBy.from_dict(
                {"keys": [], "aggregate": {"$min_k": {"keys": ["#score"], "k": 3}}}
            )
        with pytest.raises(ValueError, match="Unknown aggregate operator"):
            Aggregate.from_dict({"$unknown": {"keys": ["#score"], "k": 3}})
