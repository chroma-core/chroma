---
id: filtering
name: Filtering with Where
---

# Filtering with Where

Learn how to filter search results using Where expressions and the Key/K class to narrow down your search to specific documents, IDs, or metadata values.

## The Key/K Class

The `Key` class (aliased as `K` for brevity) provides a fluent interface for building filter expressions. Use `K` to reference document fields, IDs, and metadata properties.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import K

# K is an alias for Key - use K for more concise code
# Filter by metadata field
K("status") == "active"

# Filter by document content
K.DOCUMENT.contains("machine learning")

# Filter by document IDs
K.ID.is_in(["doc1", "doc2", "doc3"])
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { K } from 'chromadb';

// K is an alias for Key - use K for more concise code
// Filter by metadata field
K("status").eq("active");

// Filter by document content
K.DOCUMENT.contains("machine learning");

// Filter by document IDs
K.ID.isIn(["doc1", "doc2", "doc3"]);
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Filterable Fields

| Field | Usage | Description |
|-------|-------|-------------|
| `K.ID` | `K.ID.is_in(["id1", "id2"])` | Filter by document IDs |
| `K.DOCUMENT` | `K.DOCUMENT.contains("text")` | Filter by document content |
| `K("field_name")` | `K("status") == "active"` | Filter by any metadata field |

## Comparison Operators

**Supported operators:**
- `==` - Equality (all types: string, numeric, boolean)
- `!=` - Inequality (all types: string, numeric, boolean)
- `>` - Greater than (numeric only)
- `>=` - Greater than or equal (numeric only)
- `<` - Less than (numeric only)
- `<=` - Less than or equal (numeric only)

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Equality and inequality (all types)
K("status") == "published"     # String equality
K("views") != 0                # Numeric inequality
K("featured") == True          # Boolean equality

# Numeric comparisons (numbers only)
K("price") > 100               # Greater than
K("rating") >= 4.5             # Greater than or equal
K("stock") < 10                # Less than
K("discount") <= 0.25          # Less than or equal
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Equality and inequality (all types)
K("status").eq("published");     // String equality
K("views").ne(0);                // Numeric inequality
K("featured").eq(true);          // Boolean equality

// Numeric comparisons (numbers only)
K("price").gt(100);              // Greater than
K("rating").gte(4.5);            // Greater than or equal
K("stock").lt(10);               // Less than
K("discount").lte(0.25);         // Less than or equal
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
Chroma supports three data types for metadata: strings, numbers (int/float), and booleans. Order comparison operators (`>`, `<`, `>=`, `<=`) currently only work with numeric types.
{% /Note %}

## Set and String Operators

**Supported operators:**
- `is_in()` - Value matches any in the list
- `not_in()` - Value doesn't match any in the list
- `contains()` - String contains substring (case-sensitive, currently K.DOCUMENT only)
- `not_contains()` - String doesn't contain substring (currently K.DOCUMENT only)
- `regex()` - String matches regex pattern (currently K.DOCUMENT only)
- `not_regex()` - String doesn't match regex pattern (currently K.DOCUMENT only)

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Set membership operators (works on all fields)
K.ID.is_in(["doc1", "doc2", "doc3"])           # Match any ID in list
K("category").is_in(["tech", "science"])       # Match any category
K("status").not_in(["draft", "deleted"])       # Exclude specific values

# String content operators (currently K.DOCUMENT only)
K.DOCUMENT.contains("machine learning")        # Substring search in document
K.DOCUMENT.not_contains("deprecated")          # Exclude documents with text
K.DOCUMENT.regex(r"\bAPI\b")                   # Match whole word "API" in document

# Note: String pattern matching on metadata fields not yet supported
# K("title").contains("Python")                # NOT YET SUPPORTED
# K("email").regex(r".*@company\.com$")        # NOT YET SUPPORTED
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Set membership operators (works on all fields)
K.ID.isIn(["doc1", "doc2", "doc3"]);           // Match any ID in list
K("category").isIn(["tech", "science"]);       // Match any category
K("status").notIn(["draft", "deleted"]);       // Exclude specific values

// String content operators (currently K.DOCUMENT only)
K.DOCUMENT.contains("machine learning");       // Substring search in document
K.DOCUMENT.notContains("deprecated");          // Exclude documents with text
K.DOCUMENT.regex("\\bAPI\\b");                 // Match whole word "API" in document

// Note: String pattern matching on metadata fields not yet supported
// K("title").contains("Python")               // NOT YET SUPPORTED
// K("email").regex(".*@company\\.com$")       // NOT YET SUPPORTED
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
String operations like `contains()` and `regex()` are case-sensitive by default. The `is_in()` operator is efficient even with large lists.
{% /Note %}

## Logical Operators

**Supported operators:**
- `&` - Logical AND (all conditions must match)
- `|` - Logical OR (any condition can match)

Combine multiple conditions using these operators. Always use parentheses to ensure correct precedence.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# AND operator (&) - all conditions must match
(K("status") == "published") & (K("year") >= 2020)

# OR operator (|) - any condition can match
(K("category") == "tech") | (K("category") == "science")

# Combining with document and ID filters
(K.DOCUMENT.contains("AI")) & (K("author") == "Smith")
(K.ID.is_in(["id1", "id2"])) | (K("featured") == True)

# Complex nesting - use parentheses for clarity
(
    (K("status") == "published") & 
    ((K("category") == "tech") | (K("category") == "science")) &
    (K("rating") >= 4.0)
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// AND operator - all conditions must match
K("status").eq("published").and(K("year").gte(2020));

// OR operator - any condition can match
K("category").eq("tech").or(K("category").eq("science"));

// Combining with document and ID filters
K.DOCUMENT.contains("AI").and(K("author").eq("Smith"));
K.ID.isIn(["id1", "id2"]).or(K("featured").eq(true));

// Complex nesting - use chaining for clarity
K("status").eq("published")
  .and(
    K("category").eq("tech").or(K("category").eq("science"))
  )
  .and(K("rating").gte(4.0));
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="warning" %}
Always use parentheses around each condition when using logical operators. Python's operator precedence may not work as expected without them.
{% /Note %}

## Dictionary Syntax (MongoDB-style)

You can also use dictionary syntax instead of K expressions. This is useful when building filters programmatically.

**Supported dictionary operators:**
- Direct value - Shorthand for equality
- `$eq` - Equality
- `$ne` - Not equal
- `$gt` - Greater than (numeric only)
- `$gte` - Greater than or equal (numeric only)
- `$lt` - Less than (numeric only)
- `$lte` - Less than or equal (numeric only)
- `$in` - Value in list
- `$nin` - Value not in list
- `$contains` - String contains
- `$not_contains` - String doesn't contain
- `$regex` - Regex match
- `$not_regex` - Regex doesn't match
- `$and` - Logical AND
- `$or` - Logical OR

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Direct equality (shorthand)
{"status": "active"}                        # Same as K("status") == "active"

# Comparison operators
{"status": {"$eq": "published"}}            # Same as K("status") == "published"
{"count": {"$ne": 0}}                       # Same as K("count") != 0
{"price": {"$gt": 100}}                     # Same as K("price") > 100 (numbers only)
{"rating": {"$gte": 4.5}}                   # Same as K("rating") >= 4.5 (numbers only)
{"stock": {"$lt": 10}}                      # Same as K("stock") < 10 (numbers only)
{"discount": {"$lte": 0.25}}                # Same as K("discount") <= 0.25 (numbers only)

# Set membership operators
{"#id": {"$in": ["id1", "id2"]}}            # Same as K.ID.is_in(["id1", "id2"])
{"category": {"$in": ["tech", "ai"]}}       # Same as K("category").is_in(["tech", "ai"])
{"status": {"$nin": ["draft", "deleted"]}}  # Same as K("status").not_in(["draft", "deleted"])

# String operators (currently K.DOCUMENT only)
{"#document": {"$contains": "API"}}         # Same as K.DOCUMENT.contains("API")
# {"title": {"$not_contains": "draft"}}     # Not yet supported - metadata fields
# {"email": {"$regex": ".*@example\\.com"}} # Not yet supported - metadata fields
# {"version": {"$not_regex": "^beta"}}      # Not yet supported - metadata fields

# Logical operators
{"$and": [
    {"status": "published"},
    {"year": {"$gte": 2020}},
    {"#document": {"$contains": "AI"}}
]}                                          # Combines multiple conditions with AND

{"$or": [
    {"category": "tech"},
    {"category": "science"},
    {"featured": True}
]}                                          # Combines multiple conditions with OR

# Complex nested example
{
    "$and": [
        {"$or": [
            {"category": "tech"},
            {"category": "science"}
        ]},
        {"status": "published"},
        {"quality_score": {"$gte": 0.8}}
    ]
}
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Direct equality (shorthand)
{ status: "active" }                        // Same as K("status").eq("active")

// Comparison operators
{ status: { $eq: "published" } }            // Same as K("status").eq("published")
{ count: { $ne: 0 } }                       // Same as K("count").ne(0)
{ price: { $gt: 100 } }                     // Same as K("price").gt(100) (numbers only)
{ rating: { $gte: 4.5 } }                   // Same as K("rating").gte(4.5) (numbers only)
{ stock: { $lt: 10 } }                      // Same as K("stock").lt(10) (numbers only)
{ discount: { $lte: 0.25 } }                // Same as K("discount").lte(0.25) (numbers only)

// Set membership operators
{ "#id": { $in: ["id1", "id2"] } }          // Same as K.ID.isIn(["id1", "id2"])
{ category: { $in: ["tech", "ai"] } }       // Same as K("category").isIn(["tech", "ai"])
{ status: { $nin: ["draft", "deleted"] } }  // Same as K("status").notIn(["draft", "deleted"])

// String operators (currently K.DOCUMENT only)
{ "#document": { $contains: "API" } }       // Same as K.DOCUMENT.contains("API")
// { title: { $not_contains: "draft" } }    // Not yet supported - metadata fields
// { email: { $regex: ".*@example\\.com" } } // Not yet supported - metadata fields
// { version: { $not_regex: "^beta" } }     // Not yet supported - metadata fields

// Logical operators
{
  $and: [
    { status: "published" },
    { year: { $gte: 2020 } },
    { "#document": { $contains: "AI" } }
  ]
}                                           // Combines multiple conditions with AND

{
  $or: [
    { category: "tech" },
    { category: "science" },
    { featured: true }
  ]
}                                           // Combines multiple conditions with OR

// Complex nested example
{
  $and: [
    {
      $or: [
        { category: "tech" },
        { category: "science" }
      ]
    },
    { status: "published" },
    { quality_score: { $gte: 0.8 } }
  ]
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="info" %}
Each dictionary can only contain one field or one logical operator (`$and`/`$or`). For field dictionaries, only one operator is allowed per field.
{% /Note %}

## Common Filtering Patterns

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Filter by specific document IDs
search = Search().where(K.ID.is_in(["doc_001", "doc_002", "doc_003"]))

# Exclude already processed documents
processed_ids = ["doc_100", "doc_101"]
search = Search().where(K.ID.not_in(processed_ids))

# Full-text search in documents
search = Search().where(K.DOCUMENT.contains("quantum computing"))

# Combine document search with metadata
search = Search().where(
    K.DOCUMENT.contains("machine learning") & 
    (K("language") == "en")
)

# Price range filtering
search = Search().where(
    (K("price") >= 100) & 
    (K("price") <= 500)
)

# Multi-field filtering
search = Search().where(
    (K("status") == "active") &
    (K("category").is_in(["tech", "ai", "ml"])) &
    (K("score") >= 0.8)
)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Filter by specific document IDs
const search1 = new Search().where(K.ID.isIn(["doc_001", "doc_002", "doc_003"]));

// Exclude already processed documents
const processedIds = ["doc_100", "doc_101"];
const search2 = new Search().where(K.ID.notIn(processedIds));

// Full-text search in documents
const search3 = new Search().where(K.DOCUMENT.contains("quantum computing"));

// Combine document search with metadata
const search4 = new Search().where(
  K.DOCUMENT.contains("machine learning")
    .and(K("language").eq("en"))
);

// Price range filtering
const search5 = new Search().where(
  K("price").gte(100)
    .and(K("price").lte(500))
);

// Multi-field filtering
const search6 = new Search().where(
  K("status").eq("active")
    .and(K("category").isIn(["tech", "ai", "ml"]))
    .and(K("score").gte(0.8))
);
```
{% /Tab %}

{% /TabbedCodeBlock %}







## Edge Cases and Important Behavior

### Missing Keys
When filtering on a metadata field that doesn't exist for a document:
- Most operators (`==`, `>`, `<`, `>=`, `<=`, `is_in()`) evaluate to `false` - the document won't match
- `!=` evaluates to `true` - documents without the field are considered "not equal" to any value
- `not_in()` evaluates to `true` - documents without the field are not in any list

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# If a document doesn't have a "category" field:
K("category") == "tech"         # false - won't match
K("category") != "tech"         # true - will match
K("category").is_in(["tech"])   # false - won't match  
K("category").not_in(["tech"])  # true - will match
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// If a document doesn't have a "category" field:
K("category").eq("tech");        // false - won't match
K("category").ne("tech");        // true - will match
K("category").isIn(["tech"]);    // false - won't match  
K("category").notIn(["tech"]);   // true - will match
```
{% /Tab %}

{% /TabbedCodeBlock %}

### Mixed Types
Avoid storing different data types under the same metadata key across documents. Query behavior is undefined when comparing values of different types.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# DON'T DO THIS - undefined behavior
# Document 1: {"score": 95}      (numeric)
# Document 2: {"score": "95"}    (string)
# Document 3: {"score": true}    (boolean)

K("score") > 90  # Undefined results when mixed types exist

# DO THIS - consistent types
# All documents: {"score": <numeric>} or all {"score": <string>}
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// DON'T DO THIS - undefined behavior
// Document 1: {score: 95}       (numeric)
// Document 2: {score: "95"}     (string)
// Document 3: {score: true}     (boolean)

K("score").gt(90);  // Undefined results when mixed types exist

// DO THIS - consistent types
// All documents: {score: <numeric>} or all {score: <string>}
```
{% /Tab %}

{% /TabbedCodeBlock %}

### String Pattern Matching Limitations

**Currently, `contains()`, `not_contains()`, `regex()`, and `not_regex()` operators only work on `K.DOCUMENT`**. These operators do not yet support metadata fields.

Additionally, the pattern must contain at least 3 literal characters to ensure accurate results.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
# Currently supported - K.DOCUMENT only
K.DOCUMENT.contains("API")              # ✓ Works
K.DOCUMENT.regex(r"v\d\.\d\.\d")       # ✓ Works
K.DOCUMENT.contains("machine learning") # ✓ Works

# NOT YET SUPPORTED - metadata fields
K("title").contains("Python")           # ✗ Not supported yet
K("description").regex(r"API.*")        # ✗ Not supported yet

# Pattern length requirements (for K.DOCUMENT)
K.DOCUMENT.contains("API")              # ✓ 3 characters - good
K.DOCUMENT.contains("AI")               # ✗ Only 2 characters - may give incorrect results
K.DOCUMENT.regex(r"\d+")                # ✗ No literal characters - may give incorrect results
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
// Currently supported - K.DOCUMENT only
K.DOCUMENT.contains("API");              // ✓ Works
K.DOCUMENT.regex("v\\d\\.\\d\\.\\d");    // ✓ Works
K.DOCUMENT.contains("machine learning"); // ✓ Works

// NOT YET SUPPORTED - metadata fields
K("title").contains("Python");           // ✗ Not supported yet
K("description").regex("API.*");         // ✗ Not supported yet

// Pattern length requirements (for K.DOCUMENT)
K.DOCUMENT.contains("API");              // ✓ 3 characters - good
K.DOCUMENT.contains("AI");               // ✗ Only 2 characters - may give incorrect results
K.DOCUMENT.regex("\\d+");                // ✗ No literal characters - may give incorrect results
```
{% /Tab %}

{% /TabbedCodeBlock %}

{% Note type="warning" %}
String pattern matching currently only works on `K.DOCUMENT`. Support for metadata fields is not yet available. Also, patterns with fewer than 3 literal characters may return incorrect results.
{% /Note %}

{% Note type="info" %}
String pattern matching on metadata fields is not currently supported. Full support is coming in a future release, which will allow users to opt-in to additional indexes for string pattern matching on specific metadata fields.
{% /Note %}

## Complete Example

Here's a practical example combining different filter types:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

# Complex filter combining IDs, document content, and metadata
search = (Search()
    .where(
        # Exclude specific documents
        K.ID.not_in(["excluded_001", "excluded_002"]) &
        
        # Must contain specific content
        K.DOCUMENT.contains("artificial intelligence") &
        
        # Metadata conditions
        (K("status") == "published") &
        (K("quality_score") >= 0.75) &
        (
            (K("category") == "research") | 
            (K("category") == "tutorial")
        ) &
        (K("year") >= 2023)
    )
    .rank(Knn(query="latest AI research developments"))
    .limit(10)
    .select(K.DOCUMENT, "title", "author", "year")
)

results = collection.search(search)
```
{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn } from 'chromadb';

// Complex filter combining IDs, document content, and metadata
const search = new Search()
  .where(
    // Exclude specific documents
    K.ID.notIn(["excluded_001", "excluded_002"])
      
      // Must contain specific content
      .and(K.DOCUMENT.contains("artificial intelligence"))
      
      // Metadata conditions
      .and(K("status").eq("published"))
      .and(K("quality_score").gte(0.75))
      .and(
        K("category").eq("research")
          .or(K("category").eq("tutorial"))
      )
      .and(K("year").gte(2023))
  )
  .rank(Knn({ query: "latest AI research developments" }))
  .limit(10)
  .select(K.DOCUMENT, "title", "author", "year");

const results = await collection.search(search);
```
{% /Tab %}

{% /TabbedCodeBlock %}

## Tips and Best Practices

- **Use parentheses liberally** when combining conditions with `&` and `|` to avoid precedence issues
- **Filter before ranking** when possible to reduce the number of vectors to score
- **Be specific with ID filters** - using `K.ID.is_in()` with a small list is very efficient
- **String matching is case-sensitive** - normalize your data if case-insensitive matching is needed
- **Use the right operator** - `is_in()` for multiple exact matches, `contains()` for substring search

## Next Steps

- Learn about [ranking and scoring](./ranking) to order your filtered results
- See [practical examples](./examples) of filtering in real-world scenarios
- Explore [batch operations](./batch-operations) for running multiple filtered searches
