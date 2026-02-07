---
id: web
name: Web Sync
---

Web Sync allows you to easily sync content from any publicly accessible website into your Chroma Cloud database. Given a starting URL, Sync will crawl the website and its links up to a specified depth, extracting the content as Markdown, chunking it, and inserting it into your Chroma database with embeddings.

# Walkthrough

If you do not already have a Chroma Cloud account, you will need to create one at [trychroma.com](https://www.trychroma.com). After creating an account, you can create a database by specifying a name:

{% MarkdocImage lightSrc="/sync/sync_web_new_db.png" darkSrc="/sync/sync_web_new_db.png" alt="Create database screen" /%}

Then, select the Web source during onboarding:

{% MarkdocImage lightSrc="/sync/sync_web_onboarding.png" darkSrc="/sync/sync_web_onboarding.png" alt="Onboarding screen" /%}

Next, configure the Web source by providing a starting URL:

{% MarkdocImage lightSrc="/sync/sync_web_url_config.png" darkSrc="/sync/sync_web_url_config.png" alt="Web source config" /%}

Optionally, you can configure other parameters like the page limit and include path regexes. Here, we're scraping a maximum of 50 pages under `https://docs.trychroma.com/cloud` (all our cloud docs):

{% MarkdocImage lightSrc="/sync/sync_web_advanced_config.png" darkSrc="/sync/sync_web_advanced_config.png" alt="Web source config" /%}

You can also change the default collection name if you want. After clicking "Create Sync Source", an initial sync will start:

{% MarkdocImage lightSrc="/sync/sync_web_progress.png" darkSrc="/sync/sync_web_progress.png" alt="Web sync in progress" /%}

After it finishes, you'll be redirected to the created collection.
