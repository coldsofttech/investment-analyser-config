import random
import time
from functools import wraps


def retry(max_retries=5, delay=1.0, backoff=2.0, jitter=True, max_delay=60.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            curr_delay = delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"⚠️ Warning: {func.__name__} failed: {e} (attempt: {attempt + 1}/{max_retries}).")
                    sleep_time = curr_delay

                    if jitter:
                        jitter_factor = random.uniform(0.8, 1.2)
                        sleep_time += jitter_factor

                    sleep_time = min(sleep_time, max_delay)
                    print(f"⏳ Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                    curr_delay *= backoff
            raise RuntimeError(f"❌ {func.__name__} failed after {max_retries} retries.")

        return wrapper

    return decorator
