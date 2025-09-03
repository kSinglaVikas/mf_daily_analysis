from __future__ import annotations
from typing import Iterable


def chunked(iterable: Iterable, n: int):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= n:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
