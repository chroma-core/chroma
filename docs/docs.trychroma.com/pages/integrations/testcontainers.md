---
title: Testcontainers
---

Testcontainers provides a Chroma module that allows to run ChromaDB in a container for testing purposes.

## Java

Install Chroma Java module:

Maven:

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>chromadb</artifactId>
    <version>1.20.4</version>
    <scope>test</scope>
</dependency>
```

Gradle:

```groovy
testImplementation 'org.testcontainers:chromadb:1.20.4'
```

Declare the `ChromaDBContainer` in your test:

```java
ChromaDBContainer chromadb = new ChromaDBContainer("chromadb/chroma:0.4.22");
chromadb.start();
```

Use `chroma.getEndpoint()` to connect your client library to the ChromaDB container.

## Go

Install Chroma Go module:

```bash
go get github.com/testcontainers/testcontainers-go/modules/chroma
```

Declare testcontainers `chroma` module in your test:

```go
ctr, err := chroma.Run(ctx, "chromadb/chroma:0.4.22")
```

Use `ctr.RESTEndpoint(context.Background())` to connect your client library to the ChromaDB container.


## Node.js

Install Chroma Node.js module:

```bash
npm install @testcontainers/chromadb --save-dev
```

Declare the `ChromaDBContainer` in your test:

```typescript
const container = await new ChromaDBContainer("chromadb/chroma:0.4.22").start();
```

Use `container.getHttpUrl()` to connect your client library to the ChromaDB container.

## Python

Install Chroma Python module:

```bash
pip install testcontainers[chroma]
```

Declare `ChromaContainer` and configure your client library: 

```python
with ChromaContainer() as chroma:
    config = chroma.get_config()
    client = chromadb.HttpClient(host=config["host"], port=config["port"])
    collection = client.get_or_create_collection("test")
    print(collection.name)
```

# Resources

* [Testcontainers](https://testcontainers.com/)
* [Testcontainers Chroma module](https://testcontainers.com/modules/chroma/)