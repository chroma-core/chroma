import weaviate

client = weaviate.Client("http://localhost:8080") # or another location where your Weaviate instance is running

class_obj = {
  "class": "EmbeddingRow",
  "description": "A publication with an online source",
  "properties": [
    { 
      "dataType": [
        "string"
      ],
      "description": "dataset",
      "name": "dataset"
    },
    { 
      "dataType": [
        "string"
      ],
      "description": "model_space",
      "name": "model_space"
    },
    { 
      "dataType": [
        "string"
      ],
      "description": "input_uri",
      "name": "input_uri"
    },
    { 
      "dataType": [
        "string"
      ],
      "description": "inference_class",
      "name": "inference_class"
    },
    { 
      "dataType": [
        "string"
      ],
      "description": "label_class",
      "name": "label_class"
    },  
    ]
}
# client.schema.create_class(class_obj)

first_object_props = {
    "dataset": "dataset1",
    "model_space": "model_space1",
    "input_uri": "input_uri1",
    "inference_class": "inference_class1",
    "label_class": "label_class1",
    # "writesFor": [{
    #     "beacon": "weaviate://localhost/f81bfe5e-16ba-4615-a516-46c2ae2e5a80"
    # }]
}

# Python client specific configurations can be set with `client.batch.configure`
# the settings can be applied to both `objects` AND `references`.
# You have to only set them once.
client.batch.configure(
  # `batch_size` takes an `int` value to enable auto-batching
  # (`None` is used for manual batching)
  batch_size=100, 
  # dynamically update the `batch_size` based on import speed
  dynamic=False,
  # `timeout_retries` takes an `int` value to retry on time outs
  timeout_retries=3,
  # checks for batch-item creation errors
  # this is the default in weaviate-client >= 3.6.0
  callback=weaviate.util.check_batch_result,
)

with client.batch as batch:
  batch.add_data_object(first_object_props, 'EmbeddingRow', "36ddd591-2dee-4e7e-a3cc-eb86d30a4303", vector=[0.1, 0.2, 0.3])

print(client.schema.get())
print(client.data_object.get())

all_objects = client.data_object.get(class_name="EmbeddingRow")
print(all_objects)