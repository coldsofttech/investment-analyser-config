import random
import time
from functools import wraps


def retry(max_retries=5, delay=0.0, backoff=2.0, jitter=True, max_delay=60.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            external_attempt = kwargs.get("attempt", 0)
            for internal_retry in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # curr_delay = delay * (backoff ** external_attempt)
                    # curr_delay = delay + external_attempt * (external_attempt + 1) // 2
                    curr_delay = 0.9 * (external_attempt * (external_attempt + 1)) // 2
                    curr_delay = min(curr_delay, max_delay)
                    if jitter:
                        jitter_factor = random.uniform(0.1, 0.5)
                        curr_delay += jitter_factor

                    # print(
                    #     f"⚠️ Warning: {func.__name__} failed: {e} (internal retry: "
                    #     f"{internal_retry}, external attempt: {external_attempt})."
                    # )
                    print(f"⏳ Retrying in {curr_delay:.2f} seconds...")
                    time.sleep(curr_delay)
            raise RuntimeError(f"❌ {func.__name__} failed after {max_retries} retries.")

        return wrapper

    return decorator
