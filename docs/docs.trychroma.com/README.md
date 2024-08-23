# Chroma Docs

### Design

Chroma's docs are designed to be as extensible as possible with little-to-no magic.

This repo is a [NextJS](https://nextjs.org/) + [Markdoc](https://markdoc.dev/) project.

It also incldues [Shadcn](https://ui.shadcn.com/) with [Tailwind](https://tailwindcss.com/) for components and styling.

# Features todo
- [ ] add accordion for troubleshooting
- [ ] keep tab state in query params
- [ ] bring in codgen and make it easy
- [ ] swag element in light/dark mode. eg the graphic on the index route
- [ ] turn on algolia once indexed
- [ ] turn back on "edit on github" button when public

# Content todo
- [ ] add more examples


### Features
- Breadcrumbs
- Table of Contents
- Sidenav with a "folder" structure
- Search
- Dark/Light Mode
- Responsive
- Global and Local state management with localstorage persistence
- Tabs
- Code styling with Prism
- Toasts

### Content structure

Chroma's documentation must be:
- well structured
- easy to understand
- easy to navigate
- easy to search

Too much of documentation, in AI in particular, is written in a way that is confusing and just downright poor techincal communication.

Chroma's docs are designed to "ladder complexity" and guide users through a beginner-intermediate-advanced journey.

Chroma's docs should heavily use examples and graphics to help developers understand learn quickly. No one reads walls of text.

### Install and Running the docs

```bash
yarn # install
yarn dev # run nextjs
```

### Continuous Deployment

The docs are deployed to Vercel.


<!-- TODO: codegen -->


### JS Docs Autogen

```
yarn
yarn gen-js
```

### Python Docs Autogen

```
pip install -r requirements.txt
yarn gen-python
```

