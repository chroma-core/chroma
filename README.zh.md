![Chroma](./docs/assets/chroma-wordmark-color.png#gh-light-mode-only)
![Chroma](./docs/assets/chroma-wordmark-white.png#gh-dark-mode-only)

<p align="center">
    <b>Chroma — 面向人工智能的开源数据基础设施 (AI-native Embedding Database)</b>
</p>

<p align="center">
  <a href="README.md">English</a> · <b>简体中文</b>
</p>

<p align="center">
  <a href="https://discord.gg/MMeYNTmh3x" target="_blank">
      <img src="https://img.shields.io/discord/1073293645303795742?cacheSeconds=3600" alt="Discord 社区">
  </a> |
  <a href="https://github.com/chroma-core/chroma/blob/master/LICENSE" target="_blank">
      <img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="开源协议">
  </a> |
  <a href="https://docs.trychroma.com/" target="_blank">
      官方文档 (Docs)
  </a> |
  <a href="https://www.trychroma.com/" target="_blank">
      官方网站 (Homepage)
  </a>
</p>

```bash
pip install chromadb # Python 客户端
# JavaScript / TypeScript 客户端: npm install chromadb
# 客户端/服务器独立运行模式: chroma run --path /chroma_db_path
```

## Chroma Cloud 全托管云服务

我们的全托管云服务 **Chroma Cloud** 提供 Serverless 架构下的向量检索（Vector Search）、混合检索（Hybrid Search）与全文检索（Full-Text Search）。它具备极致性能、高成本效益、弹性扩缩容且免除一切运维负担。只需 30 秒即可创建数据库并体验，附赠 5 美元免费测试额度。

👉 [立即开始使用 Chroma Cloud](https://trychroma.com/signup)

## 核心 API (Core API)

Chroma 的核心 API 仅需 4 个主要方法即可完成全部操作（可在 [💡 Google Colab 在线 Notebook](https://colab.research.google.com/drive/1QEzFyqnoFxq7LUGyP1vzR4iLt9PpCDXv?usp=sharing) 中免安装运行体验）：

```python
import chromadb

# 初始化内存版 Chroma 客户端，便于快速原型验证；也可极简切换为持久化磁盘存储！
client = chromadb.Client()

# 创建集合 Collection（同时支持 get_collection, get_or_create_collection, delete_collection）
collection = client.create_collection("all-my-documents")

# 向集合中添加文档（支持文档新增、更新与删除；基于行的 API 即将推出）
collection.add(
    documents=["这是第一篇示例文档", "这是第二篇示例文档"],  # Chroma 自动处理分词、向量嵌入与索引构建，也可以传入自定义的 embedding 向量
    metadatas=[{"source": "notion"}, {"source": "google-docs"}],  # 用于过滤查询的元数据字典
    ids=["doc1", "doc2"],  # 每篇文档的唯一标识 ID
)

# 查询检索最相似的 Top-2 结果（也支持通过 .get(ids=...) 按 ID 精确获取）
results = collection.query(
    query_texts=["这是一篇用于检索的查询文本"],
    n_results=2,
    # where={"metadata_field": "is_equal_to_this"},  # 可选元数据过滤条件
    # where_document={"$contains":"search_string"}   # 可选文档文本内容过滤条件
)
```

欢迎访问 [Chroma 官方技术文档](https://docs.trychroma.com) 深入探索全量高级特性与开发指南。

## 参与贡献 (Get Involved)

Chroma 是一个蓬勃快速发展的开源项目，我们热烈欢迎社区贡献者提交 PR 与优化建议：
- [加入 Discord 社区交流](https://discord.com/invite/chromadb) — 进入 `#contributing` 讨论频道
- [查看 🛣️ 路线图 (Roadmap) 并提出建议](https://docs.trychroma.com/docs/overview/oss#roadmap)
- [认领 Issue 并提交 PR](https://github.com/chroma-core/chroma/issues) — 推荐从 [`Good first issue` 标签任务](https://github.com/chroma-core/chroma/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) 开始
- [阅读官方贡献指南](https://docs.trychroma.com/docs/overview/oss#contributing)

**版本发布节奏 (Release Cadence)**  
团队通常在每周一发布 `pypi` 与 `npm` 客户端的新 Tag 版本；紧急热修复（Hotfixes）会在周内随时发布。

## 开源许可证 (License)

遵循 [Apache 2.0](./LICENSE) 协议开源。

---

> 💡 **文档维护说明**：本中文文档由社区志愿者（@JasonYeYuhe）翻译维护，最后同步更新于 2026年8月31日。如发现内容与官方英文原版存在差异或新特性滞后，欢迎提交 PR 共同完善！
