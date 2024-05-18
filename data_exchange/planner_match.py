import re
from typing import Optional


PATTERN_CADS = re.compile(r"\b(CADS|Selektivvertrag|Selektivertrag)\b")


def is_cads(sequence: Optional[str]) -> bool:
    """Check whether a certain string contains the word CADS or Selektivvertrag."""
    if sequence:
        return bool(PATTERN_CADS.search(sequence))
    return False
