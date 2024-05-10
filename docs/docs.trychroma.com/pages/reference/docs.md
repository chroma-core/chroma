---
title: Docs Development
---

We welcome all contributions to improving our open source docs!

Here are some ways you can help:
- Fix typos and grammar errors
- Improve the language and clarity of the docs
- Add missing information

Great sections to contribute to you include:
<!-- - [💡 Examples](/examples) -->
- [🔌 Integrations](/integrations)
- [☁️ Deployment options and Administration guides](/deployment)

# How sidebars work

The sidebar is generated from the `_sidenav.js` relative folder. For your page to show up, it has to be added to that list.

The table of contents for any given page are collected from the header elements on that page. They are "tab-aware" and will only show headings that are visible.

# Kitchen sink

Please view this page on Github to see the underlying code.

### Tabs

{% tabs %}
{% tab label="one" %}
one
{% /tab %}
{% tab label="two" %}
two
{% /tab %}
{% /tabs %}

### Code blocks and tabs

Code block

```javascript
console.log('Hello, world!');
```

Code tabs
{% codetabs customHeader="sh" %}
{% codetab label="yarn" %}
```bash {% codetab=true %}
yarn install chromadb chromadb-default-embed # [!code $]
```
{% /codetab %}
{% codetab label="npm" %}
```bash {% codetab=true %}
npm install --save chromadb chromadb-default-embed # [!code $]
```
{% /codetab %}
{% /codetabs %}

### Math

{% math latexText="d = \\sum\\left(A_i-B_i\\right)^2" %}{% /math %}


### Tables

#### Basic table

| Name  | Age | Location |
|-------|-----|----------|
| Alice | 24  | Seattle  |

#### Nicer looking table

{% special_table %}
{% /special_table %}

|      Topic        |
|--------------|
| [👀 Observability](/deployment/observability) |
| [✈️ Migration](/deployment/migration) |
| 🚧 *More Coming Soon* |

### Lists

- Item 1
- Item 2

### Images

![Alt text](https://source.unsplash.com/random/800x600)

### Links

[Click me](https://www.example.com)

### Emojis

🎉🎉🎉

### Alerts

{% note type="default" title="Default" %}
I am alive
{% /note %}

{% note type="caution" title="Caution" %}
I am alive
{% /note %}

{% note type="warning" title="Warning" %}
I am alive
{% /note %}

{% note type="tip" title="Tip" %}
I am alive
{% /note %}


### text

# Heading 1
## Heading 2
### Heading 3
#### Heading 4

### Horizontal rules

---

### Code

`code text`
