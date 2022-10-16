# Chroma Server

## Development

Set up a virtual environment and install the project's requirements
and dev requirements:

```
python3 -m venv venv      # Only need to do this once
source venv/bin/activate  # Do this each time you use a new shell for the project
pip install -r requirements.txt
pip install -r requirements_dev.txt
```

To run tests, run `pytest`.

To run the server locally, in development mode, run `uvicorn chroma_server:app --reload`

## Building


