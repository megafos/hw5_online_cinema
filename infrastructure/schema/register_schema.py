import json
import os
import time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


schema_path = os.getenv(
    "SCHEMA_PATH", 
    "schemas/movie_event.avsc"
)

registry_url = os.getenv(
    "SCHEMA_REGISTRY_URL",
    "http://localhost:8081"
)

schema_text = Path(schema_path).read_text(encoding="utf-8")
url = f"{registry_url}/subjects/movie-events-value/versions"

payload = {
    "schemaType": "AVRO",
    "schema": schema_text
}

body = json.dumps(payload).encode("utf-8")

request = Request(
    url=url,
    data=body,
    headers={
        "Content-Type": "application/vnd.schemaregistry.v1+json"
    },
    method="POST"
)


max_attempts = 10
for attempt in range(1, max_attempts + 1):
    try:
        with urlopen(request, timeout=5) as response:
            response_body = response.read()
            data = json.loads(response_body)

            schema_id = data["id"]
            print(f"Schema registered with ID: {schema_id}")

        break
    except HTTPError as error:
        if error.code not in (500, 502, 503, 504):
            raise

        if attempt < max_attempts:
            time.sleep(2)
        else:
            raise
    except URLError as error:
        print(error)
        if attempt < max_attempts:
            time.sleep(2)
        else:
            raise

