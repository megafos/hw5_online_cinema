import os
import sys
import logging
import random
from generator import generate_user_activity
from producer import send_event


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)

def main():
    users = [f"user_{i}" for i in range(1, 21)]
    movies = [f"movie_{i}" for i in range(1, 21)]
    devices = ["MOBILE", "DESKTOP", "TV", "TABLET"]

    published_count = 0
    for i in range(50):
        user_id = random.choice(users)
        movie_id = random.choice(movies)
        device_type = random.choice(devices)

        events = generate_user_activity(
            user_id=user_id,
            movie_id=movie_id,
            device_type=device_type
        )

        for event in events:
            send_event(event)
            published_count += 1

    print(f"Successfully published: {published_count} events")


if __name__ == "__main__":
    main()