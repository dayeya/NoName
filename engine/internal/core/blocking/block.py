#
# FinalWall - A simple BLOCK action handler.
# Author: Dayeya
#

import re
from pathlib import Path
from urllib.parse import urlunparse
from jinja2 import Environment, FileSystemLoader

from .._types import WafResponseCode
from ..transaction import Transaction
from engine.singleton import Singleton


ROOT_DIR = Path(__file__).parent
TEMPLATES_PATH = ROOT_DIR / "templates"
BLOCK_REGEX: re.Pattern[str] = re.compile(r"/block[?]token=([a-z0-9]{8})")


def create_redirection(location: bytes) -> bytes:
    redirect = b"HTTP/1.1 302 Found\r\n"
    redirect += b"Location: " + location + b"\r\n\r\n"
    return redirect


def contains_block(tx: Transaction) -> str | None:
    """
    Uses regex to get the token of a transaction seeking its security page.
    :param tx:
    :return:
    """
    resource: bytes = urlunparse(tx.url)
    m: re.Match[str] = re.match(BLOCK_REGEX, resource)
    if not m:
        return None
    return m.group(1)


class TemplateEnv(Environment, metaclass=Singleton):
    """
    A wrapper class to jinja2.Environment.
    """
    def __init__(self):
        super().__init__(loader=FileSystemLoader(TEMPLATES_PATH))

    def render_kwargs(self, **kwargs) -> bytes:
        """
        Creates a complete template with the given kwargs.
        :param kwargs:
        :return:
        """
        general_template = self.get_template("general_template.html")
        return general_template.render(**kwargs).encode("utf-8")


def create_security_page(info, waf_response: WafResponseCode):
    """Builds a security page based on parameters inside info."""
    code, message = waf_response.value[0]
    html: bytes = TemplateEnv().render_kwargs(**info)
    content_length = str(len(html)).encode("utf-8")
    response = b"HTTP/1.1 " + code + b" " + message + b"\r\n"
    response += b"Content-Type: text/html; charset=utf-8\r\n"
    response += b"Content-Length: " + content_length + b"\r\n"
    response += b"Connection: close\r\n\r\n"
    response += html
    return response
