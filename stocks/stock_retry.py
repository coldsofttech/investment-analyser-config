import time
from functools import wraps


def retry(max_retries=5, delay=1.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"⚠️ Warning: {func.__name__} failed: {e} (attempt: {attempt + 1}).")
                    time.sleep(delay)
            raise RuntimeError(f"❌ {func.__name__} failed after {max_retries} retries.")

        return wrapper

    return decorator
