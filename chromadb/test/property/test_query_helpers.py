from chromadb.utils.query_helper import Where, WhereDocument


def test_where_gt() -> None:
    where_filter = Where().gt("price", 100)
    assert where_filter.to_filter() == {"price": {"$gt": 100}}


def test_where_gte() -> None:
    where_filter = Where().gte("price", 100)
    assert where_filter.to_filter() == {"price": {"$gte": 100}}


def test_where_lt() -> None:
    where_filter = Where().lt("price", 100)
    assert where_filter.to_filter() == {"price": {"$lt": 100}}


def test_where_lte() -> None:
    where_filter = Where().lte("price", 100)
    assert where_filter.to_filter() == {"price": {"$lte": 100}}


def test_where_ne() -> None:
    where_filter = Where().ne("price", 100)
    assert where_filter.to_filter() == {"price": {"$ne": 100}}


def test_where_eq() -> None:
    where_filter = Where().eq("price", 100)
    assert where_filter.to_filter() == {"price": {"$eq": 100}}


# def test_where_in() -> None:
#     where_filter = Where().in_("category", ["electronics", "books"])
#     assert where_filter.to_filter() == {"category": {"$in": ["electronics", "books"]}}


def test_where_and() -> None:
    where_filter = Where().and_(
        Where().eq("category", "electronics"), Where().gt("price", 100)
    )
    assert where_filter.to_filter() == {
        "$and": [{"category": {"$eq": "electronics"}}, {"price": {"$gt": 100}}]
    }


def test_where_or() -> None:
    where_filter = Where().or_(
        Where().eq("category", "electronics"), Where().eq("category", "books")
    )
    assert where_filter.to_filter() == {
        "$or": [{"category": {"$eq": "electronics"}}, {"category": {"$eq": "books"}}]
    }


def test_where_and_or_combination() -> None:
    where_filter = Where().and_(
        Where().eq("category", "electronics"),
        Where().or_(Where().gt("price", 100), Where().lt("price", 50)),
    )
    assert where_filter.to_filter() == {
        "$and": [
            {"category": {"$eq": "electronics"}},
            {"$or": [{"price": {"$gt": 100}}, {"price": {"$lt": 50}}]},
        ]
    }


def test_where_document_contains() -> None:
    where_doc_filter = WhereDocument().contains("laptop")
    assert where_doc_filter.to_filter() == {"$contains": "laptop"}


def test_where_document_and() -> None:
    where_doc_filter = WhereDocument().and_(
        WhereDocument().contains("laptop"), WhereDocument().contains("macbook")
    )
    assert where_doc_filter.to_filter() == {
        "$and": [{"$contains": "laptop"}, {"$contains": "macbook"}]
    }


def test_where_document_or() -> None:
    where_doc_filter = WhereDocument().or_(
        WhereDocument().contains("laptop"), WhereDocument().contains("macbook")
    )
    assert where_doc_filter.to_filter() == {
        "$or": [{"$contains": "laptop"}, {"$contains": "macbook"}]
    }
