import time
import traceback

from titanpublic import shared_logic


@shared_logic.cache(None)
def bad_func(x):
    time.sleep(1)
    if x == 0:
        return 0
    raise ValueError("1")

for x in range(10):
    try:
        print(bad_func(x%2))
    except ValueError as err:
        print(repr(err))
