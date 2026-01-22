---
id: reference
name: Reference
---

# Chroma Reference

## Client APIs

Chroma currently maintains 1st party clients for Python, Javascript, and Go. For other clients in other languages, use their repos for documentation.

`Client` - is the object that wraps a connection to a backing Chroma DB

`Collection` - is the object that wraps a collection

{% special_table %}
{% /special_table %}

|            | Client                    | Collection                        |
| ---------- | ------------------------- | --------------------------------- |
| Python     | [Client](./python/client) | [Collection](./python/collection) |
| Javascript | [Client](./js/client)     | [Collection](./js/collection)     |
| Go         | [Client](./go/client)     | [Collection](./go/collection)     |

---

## Backend API

Chroma's backend Swagger REST API docs are viewable by running Chroma and navigating to `http://localhost:8000/docs`.

```bash
pip install chromadb
chroma run
open http://localhost:8000/docs
```
