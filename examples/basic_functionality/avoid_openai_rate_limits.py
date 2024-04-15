from time import sleep, time

# When you're creating OpenAI embeddings, you will run up against the rate limit.  In this example, the rate limit is 10,000 requests per minut.  
# If the collection is too big, we will run against the 10,000 requests per minute limit, and get a rate limit error.
# Call collection.add with a slice of 1000 of the data at each time.  Then sleep for 45 seconds.

def add_embeddings(tech_data_list_description, tech_data_list_uuid, tech_data_meta):
  '''
  tech_data_list_description - The list of data to be added that will be vectorized.
  tech_data_list_uuid - The id's.  In our case will be the uuids.
  tech_data_meta - The metadata.  In our case will be the source of the data.
  '''

  
  while len(tech_data_list_description) > 1000:
      time_start = time()
      print(f"Start Loop: {len(tech_data_list_description)}")
      collection.add(
          documents=tech_data_list_description[:1000],       # The list of data to be added that will be vectorized.
          ids=tech_data_list_uuid[:1000],                    # The id's.  In our case will be the uuids.
          metadatas=tech_data_meta[:1000]             # The metadata.  In our case will be the source of the data.
      )
      tech_data_list_description = tech_data_list_description[1000:]
      tech_data_list_uuid = tech_data_list_uuid[1000:]
      tech_data_meta = tech_data_meta[1000:]
      print(f"Technologies To Go: {len(tech_data_list_description)}")
      time_end = time()
      time_elapsed = time_end - time_start
      time_to_sleep = 60 - time_elapsed
      print(f"Time to sleep: {time_to_sleep}")
      print(f"====================")
      sleep(time_to_sleep)  
  
  # Add the last of the data, anything that didn't chunk into 1000 parts.
  collection.add(
      documents=tech_data_list_description,       # The list of data to be added that will be vectorized.
      ids=tech_data_list_uuid,                    # The id's.  In our case will be the uuids.
      metadatas=tech_data_meta             # The metadata.  In our case will be the source of the data.
  )
