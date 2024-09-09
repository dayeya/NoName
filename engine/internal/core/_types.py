#
# FinalWall - Core types for the system.
# Author: Dayeya
#

from enum import Enum
from typing import Callable
from dataclasses import dataclass

from ..events.classifier import Classifier


SQL_INJECTION = 0x00000001  # 1
XSS = 0x00000010  # 2
LFI = 0x00000100  # 4
RFI = 0x00001000  # 8
BRUTEFORCE = 0x00010000  # 16
UNAUTHORIZED_ACCESS = 0x00100000  # 32
ANONYMOUS = 0x01000000  # 64
GEOLOCATION = 0x10000000  # 128


@dataclass(slots=True)
class Check:
    """
    A class representing a check to be done.
    """
    fn: Callable
    args: tuple


@dataclass(slots=True)
class CheckResult:
    """
    A class representing a result of a check.

    Attributes
    ----------
    result - boolean indicating if the check passed or not.
    classifiers - If an attack is detected, the check function will return the classifier of the attack.
    """
    result: bool
    classifiers: list[Classifier]


class WafResponseCode(Enum):
    """HTTP codes for unique responses."""
    regular = (b"200", b"OK"),
    rateLimited = (b"429", b"Too Many Requests")