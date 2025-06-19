# Metadata Filtering

Chroma supports filtering queries by `metadata` and `document` contents. The `where` filter is used to filter by `metadata`. As usual, filtering is applied before the query; that is, the query is run against the filtered collection.

In order to filter on metadata, you must supply a `where` filter dictionary to the query. The dictionary must have the following structure:

```python
{
    "metadata_field": {
        <Operator>: <Value>
    }
}
```

#### NOTE:

A `where` filter must have, at any level of nesting, one key (a string) and one key only. The following examples are not well-formed:

```python
{"topic": "history", "source": "wikipedia"}
{"date": {"$gte": 2000, "$lte" 1900}}
{123: {"$gt": 100}}
```

If you wish to perform more complex queries, you can use the logical operators described below.

Filtering metadata supports the following operators:

- `$eq` - equal to (string, int, float)
- `$ne` - not equal to (string, int, float)
- `$gt` - greater than (int, float)
- `$gte` - greater than or equal to (int, float)
- `$lt` - less than (int, float)
- `$lte` - less than or equal to (int, float)

The syntax allows for a shorthand expression for the `$eq` operator as a simple key-value pair, as in the following example:

```python
{
    "metadata_field": "search_string"
}

# is equivalent to

{
    "metadata_field": {
        "$eq": "search_string"
    }
}
```

#### Using logical operators

You can also use the logical operators `$and` and `$or` to combine multiple filters.

An `$and` operator will return results that match all of the filters in the list.

```python
{
    "$and": [
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        },
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        }
    ]
}
```

An `$or` operator will return results that match any of the filters in the list.

```python
{
    "$or": [
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        },
        {
            "metadata_field": {
                <Operator>: <Value>
            }
        }
    ]
}
```

#### Using inclusion operators (`$in` and `$nin`)

The following inclusion operators are supported:

- `$in` - a value is in predefined list (string, int, float, bool)
- `$nin` - a value is not in predefined list (string, int, float, bool)

An `$in` operator will return results where the metadata attribute is part of a provided list:

```json
{
  "metadata_field": {
    "$in": ["value1", "value2", "value3"]
  }
}
```

An `$nin` operator will return results where the metadata attribute is not part of a provided list (or the attribute's key is not present):

```json
{
  "metadata_field": {
    "$nin": ["value1", "value2", "value3"]
  }
}
```

#### NOTE:

The values for the `$in` and `$nin` operators must be non-empty lists, and each value must be of the same _same type_. For example, the following is not valid:

```json
{
  "metadata_field": {
    "$in": ["value1", 2, True]
  }
}
```

{% Banner type="tip" %}

For additional examples and a demo how to use the inclusion operators, please see provided notebook [here](https://github.com/chroma-core/chroma/blob/main/examples/basic_functionality/in_not_in_filtering.ipynb)

{% /Banner %}
