# xAI

This folder contains basic examples of using Chroma with the xAI SDK.

## Chat with your Documents

Add PDF documents to the `docs` directory. When the program starts, it will chunk and embed your documents and add them to a Chroma collection. Each embedding will have a metadata field indicating what document it came from.

The prompt is designed to use information from your documents to answer questions. Feel free to edit it for a different behavior.

### Running the example

You will need an [xAI key](https://developers.x.ai/api/api-key/) to run this demo.

```bash
export XAI_API_KEY=[Your API key goes here]
```

Install dependencies and run the example:

```bash
# Install dependencies
pip install -r requirements.txt

# Run the chatbot
python rag_chat_with_your_docs.py
```

Chroma will persist its data in the `chroma_data` directory. If you want to restart the example, or remove from you chat documents that were previously inserted, delete your `chrom_data` directory.

```bash
rm -rf chroma_data
```

