---
id: examples
name: Examples & Patterns
---

# Examples & Patterns

Complete end-to-end examples demonstrating real-world use cases of the Search API.

## Example 1: E-commerce Product Search

A complete example showing how to build a product search with filters, ranking, and pagination.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn, And

def search_products(collection, user_query, min_price=None, max_price=None, 
                   category=None, in_stock_only=True, page=0, page_size=20):
    """
    Search for products with semantic search and filters.
    
    Args:
        collection: Chroma collection
        user_query: Natural language search query (e.g., "wireless headphones")
        min_price: Minimum price filter
        max_price: Maximum price filter
        category: Product category filter
        in_stock_only: Only show in-stock items
        page: Page number (0-indexed)
        page_size: Results per page
    """
    
    # Build filter conditions
    from chromadb import And
    
    combined_filter = And([])
    
    if in_stock_only:
        combined_filter &= K("in_stock") == True
    
    if category:
        combined_filter &= K("category") == category
    
    if min_price is not None:
        combined_filter &= K("price") >= min_price
    
    if max_price is not None:
        combined_filter &= K("price") <= max_price
    
    # Build search
    search = Search().where(combined_filter)
    
    search = (search
        .rank(Knn(query=user_query))
        .limit(page_size, offset=page * page_size)
        .select(K.DOCUMENT, K.SCORE, "name", "price", "category", "rating", "image_url"))
    
    # Execute search
    results = collection.search(search)
    rows = results.rows()[0]
    
    # Format results for display
    products = []
    for row in rows:
        products.append({
            "id": row["id"],
            "name": row["metadata"]["name"],
            "description": row["document"][:200] + "...",
            "price": row["metadata"]["price"],
            "category": row["metadata"]["category"],
            "rating": row["metadata"]["rating"],
            "image_url": row["metadata"]["image_url"],
            "relevance_score": row["score"]
        })
    
    return products

# Example usage
products = search_products(
    collection,
    user_query="noise cancelling headphones for travel",
    min_price=50,
    max_price=300,
    category="electronics",
    page=0,
    page_size=20
)

for i, product in enumerate(products, 1):
    print(f"{i}. {product['name']}")
    print(f"   Price: ${product['price']:.2f} | Rating: {product['rating']}/5")
    print(f"   {product['description']}")
    print(f"   Relevance: {product['relevance_score']:.3f}")
    print()
```

{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn, type Collection } from 'chromadb';

interface ProductSearchOptions {
  userQuery: string;
  minPrice?: number;
  maxPrice?: number;
  category?: string;
  inStockOnly?: boolean;
  page?: number;
  pageSize?: number;
}

async function searchProducts(
  collection: Collection,
  options: ProductSearchOptions
) {
  const {
    userQuery,
    minPrice,
    maxPrice,
    category,
    inStockOnly = true,
    page = 0,
    pageSize = 20
  } = options;
  
  // Build filter conditions
  let combinedFilter = inStockOnly ? K("in_stock").eq(true) : undefined;
  
  if (category) {
    const categoryFilter = K("category").eq(category);
    combinedFilter = combinedFilter ? combinedFilter.and(categoryFilter) : categoryFilter;
  }
  
  if (minPrice !== undefined) {
    const minPriceFilter = K("price").gte(minPrice);
    combinedFilter = combinedFilter ? combinedFilter.and(minPriceFilter) : minPriceFilter;
  }
  
  if (maxPrice !== undefined) {
    const maxPriceFilter = K("price").lte(maxPrice);
    combinedFilter = combinedFilter ? combinedFilter.and(maxPriceFilter) : maxPriceFilter;
  }
  
  // Build search
  let search = new Search();
  if (combinedFilter) {
    search = search.where(combinedFilter);
  }
  
  search = search
    .rank(Knn({ query: userQuery }))
    .limit(pageSize, page * pageSize)
    .select(K.DOCUMENT, K.SCORE, "name", "price", "category", "rating", "image_url");
  
  // Execute search
  const results = await collection.search(search);
  const rows = results.rows()[0];
  
  // Format results for display
  const products = rows.map((row: any) => ({
    id: row.id,
    name: row.metadata?.name,
    description: row.document?.substring(0, 200) + "...",
    price: row.metadata?.price,
    category: row.metadata?.category,
    rating: row.metadata?.rating,
    imageUrl: row.metadata?.image_url,
    relevanceScore: row.score
  }));
  
  return products;
}

// Example usage
const products = await searchProducts(collection, {
  userQuery: "noise cancelling headphones for travel",
  minPrice: 50,
  maxPrice: 300,
  category: "electronics",
  page: 0,
  pageSize: 20
});

for (const [i, product] of products.entries()) {
  console.log(`${i + 1}. ${product.name}`);
  console.log(`   Price: $${product.price.toFixed(2)} | Rating: ${product.rating}/5`);
  console.log(`   ${product.description}`);
  console.log(`   Relevance: ${product.relevanceScore.toFixed(3)}`);
  console.log();
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

Example output:
```
1. Sony WH-1000XM5 Wireless Headphones
   Price: $279.99 | Rating: 4.8/5
   Premium noise cancelling headphones with exceptional sound quality, perfect for long flights and commutes. Features 30-hour battery life...
   Relevance: 0.234

2. Bose QuietComfort 45
   Price: $249.99 | Rating: 4.7/5
   Industry-leading noise cancellation with comfortable over-ear design. Ideal for frequent travelers with adjustable ANC levels...
   Relevance: 0.267
```

## Example 2: Content Recommendation System

Build a personalized content recommendation system that excludes already-seen items and respects user preferences.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn, Rrf

def get_recommendations(collection, user_id, user_preferences, 
                       seen_content_ids, num_recommendations=10):
    """
    Get personalized content recommendations for a user.
    
    Args:
        collection: Chroma collection
        user_id: User identifier
        user_preferences: Dict with user interests and preferences
        seen_content_ids: List of content IDs the user has already seen
        num_recommendations: Number of recommendations to return
    """
    
    # Build filter to exclude seen content and match preferences
    combined_filter = K.ID.not_in(seen_content_ids)
    
    # Filter by preferred categories
    if user_preferences.get("categories"):
        combined_filter &= K("category").is_in(user_preferences["categories"])
    
    # Filter by language preference
    if user_preferences.get("language"):
        combined_filter &= K("language") == user_preferences["language"]
    
    # Filter by minimum rating
    min_rating = user_preferences.get("min_rating", 3.5)
    combined_filter &= K("rating") >= min_rating
    
    # Only show published content
    combined_filter &= K("status") == "published"
    
    # Create hybrid search combining multiple signals
    # Signal 1: User interest embedding
    user_interest_query = " ".join(user_preferences.get("interests", ["general"]))
    
    # Signal 2: Similar to user's favorite content
    favorite_topics_query = " ".join(user_preferences.get("favorite_topics", []))
    
    # Use RRF to combine both signals
    hybrid_rank = Rrf(
        ranks=[
            Knn(query=user_interest_query, return_rank=True, limit=200),
            Knn(query=favorite_topics_query, return_rank=True, limit=200)
        ],
        weights=[0.6, 0.4],  # User interests weighted higher
        k=60
    )
    
    search = (Search()
        .where(combined_filter)
        .rank(hybrid_rank)
        .limit(num_recommendations)
        .select(K.DOCUMENT, K.SCORE, "title", "category", "author", 
                "rating", "published_date", "thumbnail_url"))
    
    results = collection.search(search)
    rows = results.rows()[0]
    
    # Format recommendations
    recommendations = []
    for row in rows:
        recommendations.append({
            "id": row["id"],
            "title": row["metadata"]["title"],
            "description": row["document"][:150] + "...",
            "category": row["metadata"]["category"],
            "author": row["metadata"]["author"],
            "rating": row["metadata"]["rating"],
            "published_date": row["metadata"]["published_date"],
            "thumbnail_url": row["metadata"]["thumbnail_url"],
            "relevance_score": row["score"]
        })
    
    return recommendations

# Example usage
user_preferences = {
    "interests": ["machine learning", "artificial intelligence", "data science"],
    "favorite_topics": ["neural networks", "deep learning", "transformers"],
    "categories": ["technology", "science", "research"],
    "language": "en",
    "min_rating": 4.0
}

seen_content = ["content_001", "content_045", "content_123"]

recommendations = get_recommendations(
    collection,
    user_id="user_42",
    user_preferences=user_preferences,
    seen_content_ids=seen_content,
    num_recommendations=10
)

print("Personalized Recommendations:")
for i, rec in enumerate(recommendations, 1):
    print(f"\n{i}. {rec['title']}")
    print(f"   Category: {rec['category']} | Author: {rec['author']}")
    print(f"   Rating: {rec['rating']}/5 | Published: {rec['published_date']}")
    print(f"   {rec['description']}")
    print(f"   Match Score: {rec['relevance_score']:.3f}")
```

{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn, Rrf, type Collection } from 'chromadb';

interface UserPreferences {
  interests?: string[];
  favoriteTopics?: string[];
  categories?: string[];
  language?: string;
  minRating?: number;
}

async function getRecommendations(
  collection: Collection,
  userId: string,
  userPreferences: UserPreferences,
  seenContentIds: string[],
  numRecommendations: number = 10
) {
  // Build filter to exclude seen content
  let combinedFilter = K.ID.notIn(seenContentIds);
  
  // Filter by preferred categories
  if (userPreferences.categories && userPreferences.categories.length > 0) {
    combinedFilter = combinedFilter.and(K("category").isIn(userPreferences.categories));
  }
  
  // Filter by language preference
  if (userPreferences.language) {
    combinedFilter = combinedFilter.and(K("language").eq(userPreferences.language));
  }
  
  // Filter by minimum rating
  const minRating = userPreferences.minRating ?? 3.5;
  combinedFilter = combinedFilter.and(K("rating").gte(minRating));
  
  // Only show published content
  combinedFilter = combinedFilter.and(K("status").eq("published"));
  
  // Create hybrid search combining multiple signals
  const userInterestQuery = (userPreferences.interests ?? ["general"]).join(" ");
  const favoriteTopicsQuery = (userPreferences.favoriteTopics ?? []).join(" ");
  
  // Use RRF to combine both signals
  const hybridRank = Rrf({
    ranks: [
      Knn({ query: userInterestQuery, returnRank: true, limit: 200 }),
      Knn({ query: favoriteTopicsQuery, returnRank: true, limit: 200 })
    ],
    weights: [0.6, 0.4],  // User interests weighted higher
    k: 60
  });
  
  const search = new Search()
    .where(combinedFilter)
    .rank(hybridRank)
    .limit(numRecommendations)
    .select(K.DOCUMENT, K.SCORE, "title", "category", "author", 
            "rating", "published_date", "thumbnail_url");
  
  const results = await collection.search(search);
  const rows = results.rows()[0];
  
  // Format recommendations
  const recommendations = rows.map((row: any) => ({
    id: row.id,
    title: row.metadata?.title,
    description: row.document?.substring(0, 150) + "...",
    category: row.metadata?.category,
    author: row.metadata?.author,
    rating: row.metadata?.rating,
    publishedDate: row.metadata?.published_date,
    thumbnailUrl: row.metadata?.thumbnail_url,
    relevanceScore: row.score
  }));
  
  return recommendations;
}

// Example usage
const userPreferences: UserPreferences = {
  interests: ["machine learning", "artificial intelligence", "data science"],
  favoriteTopics: ["neural networks", "deep learning", "transformers"],
  categories: ["technology", "science", "research"],
  language: "en",
  minRating: 4.0
};

const seenContent = ["content_001", "content_045", "content_123"];

const recommendations = await getRecommendations(
  collection,
  "user_42",
  userPreferences,
  seenContent,
  10
);

console.log("Personalized Recommendations:");
for (const [i, rec] of recommendations.entries()) {
  console.log(`\n${i + 1}. ${rec.title}`);
  console.log(`   Category: ${rec.category} | Author: ${rec.author}`);
  console.log(`   Rating: ${rec.rating}/5 | Published: ${rec.publishedDate}`);
  console.log(`   ${rec.description}`);
  console.log(`   Match Score: ${rec.relevanceScore.toFixed(3)}`);
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

Example output:
```
Personalized Recommendations:

1. Advanced Transformer Architectures in 2024
   Category: technology | Author: Dr. Sarah Chen
   Rating: 4.5/5 | Published: 2024-10-15
   An in-depth exploration of the latest transformer models and their applications in modern NLP tasks. This article covers attention mechanisms, positional encodings...
   Match Score: -0.0342

2. Practical Guide to Neural Network Optimization
   Category: research | Author: Prof. James Wilson
   Rating: 4.7/5 | Published: 2024-09-28
   Learn cutting-edge techniques for optimizing deep neural networks, including adaptive learning rates, batch normalization strategies, and efficient backpropagation...
   Match Score: -0.0389
```

## Example 3: Multi-Category Search with Batch Operations

Use batch operations to search across multiple categories simultaneously and compare results.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
from chromadb import Search, K, Knn

def search_across_categories(collection, user_query, categories, results_per_category=5):
    """
    Search across multiple categories in parallel using batch operations.
    
    Args:
        collection: Chroma collection
        user_query: User's search query
        categories: List of categories to search
        results_per_category: Number of results per category
    """
    
    # Build a search for each category
    searches = []
    for category in categories:
        search = (Search()
            .where(K("category") == category)
            .rank(Knn(query=user_query))
            .limit(results_per_category)
            .select(K.DOCUMENT, K.SCORE, "title", "category", "date"))
        searches.append(search)
    
    # Execute all searches in one batch
    results = collection.search(searches)
    
    # Process results by category
    category_results = {}
    for i, category in enumerate(categories):
        rows = results.rows()[i]
        category_results[category] = [
            {
                "id": row["id"],
                "title": row["metadata"]["title"],
                "description": row["document"][:100] + "...",
                "date": row["metadata"]["date"],
                "score": row["score"]
            }
            for row in rows
        ]
    
    return category_results

# Example usage
query = "latest developments in renewable energy"
categories = ["technology", "science", "news", "research"]

results_by_category = search_across_categories(
    collection,
    user_query=query,
    categories=categories,
    results_per_category=3
)

# Display results
for category, results in results_by_category.items():
    print(f"\n{'='*60}")
    print(f"Category: {category.upper()}")
    print('='*60)
    
    if not results:
        print("  No results found")
        continue
    
    for i, result in enumerate(results, 1):
        print(f"\n  {i}. {result['title']}")
        print(f"     Date: {result['date']}")
        print(f"     {result['description']}")
        print(f"     Relevance: {result['score']:.3f}")
```

{% /Tab %}

{% Tab label="typescript" %}
```typescript
import { Search, K, Knn, type Collection } from 'chromadb';

async function searchAcrossCategories(
  collection: Collection,
  userQuery: string,
  categories: string[],
  resultsPerCategory: number = 5
) {
  // Build a search for each category
  const searches = categories.map(category =>
    new Search()
      .where(K("category").eq(category))
      .rank(Knn({ query: userQuery }))
      .limit(resultsPerCategory)
      .select(K.DOCUMENT, K.SCORE, "title", "category", "date")
  );
  
  // Execute all searches in one batch
  const results = await collection.search(searches);
  
  // Process results by category
  const categoryResults: Record<string, any[]> = {};
  for (const [i, category] of categories.entries()) {
    const rows = results.rows()[i];
    categoryResults[category] = rows.map((row: any) => ({
      id: row.id,
      title: row.metadata?.title,
      description: row.document?.substring(0, 100) + "...",
      date: row.metadata?.date,
      score: row.score
    }));
  }
  
  return categoryResults;
}

// Example usage
const query = "latest developments in renewable energy";
const categories = ["technology", "science", "news", "research"];

const resultsByCategory = await searchAcrossCategories(
  collection,
  query,
  categories,
  3
);

// Display results
for (const [category, results] of Object.entries(resultsByCategory)) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Category: ${category.toUpperCase()}`);
  console.log('='.repeat(60));
  
  if (results.length === 0) {
    console.log("  No results found");
    continue;
  }
  
  for (const [i, result] of results.entries()) {
    console.log(`\n  ${i + 1}. ${result.title}`);
    console.log(`     Date: ${result.date}`);
    console.log(`     ${result.description}`);
    console.log(`     Relevance: ${result.score.toFixed(3)}`);
  }
}
```
{% /Tab %}

{% /TabbedCodeBlock %}

Example output:
```
============================================================
Category: TECHNOLOGY
============================================================

  1. Solar Panel Efficiency Breakthrough
     Date: 2024-10-20
     New silicon-carbon composite cells achieve 31% efficiency, setting industry records. Researchers at MIT have developed...
     Relevance: 0.245

  2. Wind Turbine Design Innovations
     Date: 2024-10-15
     Advanced blade designs increase energy capture by 18% while reducing noise pollution. The new turbines feature...
     Relevance: 0.289

============================================================
Category: SCIENCE
============================================================

  1. Photosynthesis-Inspired Energy Storage
     Date: 2024-10-18
     Scientists develop bio-inspired battery system that mimics natural photosynthesis for efficient solar energy storage...
     Relevance: 0.256
```

## Best Practices

Based on these examples, here are key best practices:

1. **Build filters incrementally** - Construct complex filters by combining simpler conditions
2. **Use batch operations** - When searching multiple variations, use batch operations for better performance
3. **Select only needed fields** - Reduce data transfer by selecting only the fields you'll use
4. **Handle empty results gracefully** - Always check if results exist before processing
5. **Use hybrid search for personalization** - Combine multiple ranking signals with RRF for better recommendations
6. **Paginate large result sets** - Use limit and offset for efficient pagination
7. **Format results for your use case** - Transform raw results into application-specific formats

## Next Steps

- Review [Search Basics](./search-basics) for core concepts
- Learn about [Filtering](./filtering) for advanced filter expressions
- Explore [Ranking](./ranking) for custom scoring strategies
- See [Hybrid Search](./hybrid-search) for combining multiple ranking methods
