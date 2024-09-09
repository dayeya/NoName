#
# FinalWall - A classifier representation.
# Author: Dayeya
#

from enum import StrEnum


class Classifier(StrEnum):
    """
    Classifier is a part of the classification process of each Security Log. It tells the Waf why the event was triggered.
    Additionally, a classifier is the vulnerability identifier and the rationale behind the BLOCK action.

    Variants
    -------------
    Sql Injection - The transaction showed signs of SQL signatures.
    XSS - The transaction showed signs of XSS signatures.
    Unauthorized Access - The transactions `request-URI` or namely tx.url aims to access a forbidden location.
    Banned Access - The transaction was made by an already banned client.
    Banned Geolocation - The transaction was made by a client living in a banned geolocation.
    Anonymity - The transactions owner identifies with a banned software such as TOR.
    """
    SqlInjection = "Sql Injection"
    XSS = "Cross Site Scripting"
    RFI = "Remote File Inclusion"
    LFI = "Local File Inclusion"
    UnauthorizedAccess = "Unauthorized Access"
    BannedAccess = "Banned Access"
    BannedGeolocation = "Banned Geolocation"
    Anonymity = "Anonymization"
    RateLimited = "Too Many Requests"


__all__ = [
    "Classifier"
]
