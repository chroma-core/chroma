# Chat with your documents

This folder contains a (very) minimal, self-contained example of how to make an application to chat with your documents, using Chroma and OpenAI's API.
It uses the 2022 and 2023 U.S state of the union addresses as example documents.

## How it works

The basic flow is as follows:

0. The text documents in the `documents` folder are loaded line by line, then embedded and stored in a Chroma collection.

1. When the user submits a question, it gets embedded using the same model as the documents, and the lines most relevant to the query are retrieved by Chroma.
2. The user-submitted question is passed to OpenAI's API, along with the extra context retrieved by Chroma. The OpenAI API generates generates a response.
3. The response is displayed to the user, along with the lines used as extra context.

## Running the example

You will need an OpenAI API key to run this demo. You can [get one here](https://platform.openai.com/account/api-keys).

Install dependencies and run the example:

```bash
# Install dependencies
pip install -r requirements.txt

# Load the example documents into Chroma
python load_data.py

# Run the chatbot
python main.py
```

Example output:

```
Query: What was said about the pandemic?

Thinking...

Based on the given context, several points were made about the pandemic. First, it is described as punishing, indicating the severity and impact it had on various aspects of life. It is mentioned that schools were closed and everything was being shut down in response to the COVID crisis, suggesting the significant measures taken to combat the virus.

The context then shifts to discussing the progress made in the fight against the pandemic itself. While no specific details are provided, it is implied that there has been progress, though the extent of it is unclear.

Additionally, it is stated that children were already facing struggles before the pandemic, such as bullying, violence, trauma, and the negative effects of social media. This suggests that these issues were likely exacerbated by the pandemic.

The context then mentions a spike in violent crime in 2020, which is attributed to the first year of the pandemic. This implies that there was an increase in violent crime during that time period, but the underlying causes or specific details are not provided.

Lastly, it is mentioned that the pandemic also disrupted global supply chains. Again, no specific details are given, but this suggests that the pandemic had negative effects on the movement and availability of goods and resources at a global level.

In conclusion, based on the provided context, it is stated that the pandemic has been punishing and has resulted in the closure of schools and the shutdown of various activities. Progress is mentioned in fighting against the pandemic, though the specifics are not given. The pandemic is also said to have worsened pre-existing issues such as bullying and violence among children, and disrupted global supply chains.
```

You can replace the example text documents in the `documents` folder with your own documents, and the chatbot will use those instead.
