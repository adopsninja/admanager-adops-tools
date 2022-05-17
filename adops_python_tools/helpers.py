import random
import string
from typing import List


def item_chunks(list: List, number: int):
    for item in range(0, len(list), number):
        yield list[item : item + number]

def random_id():
    return "".join(random.choices(string.digits, k=6))
