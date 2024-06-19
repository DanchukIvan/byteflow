from copy import deepcopy

from yarl import URL

from yass.core import SingletonMixin

__all__ = ["build_proxy_url", "get_proxy_list", "remove_proxy_url"]


class _ProxyList(SingletonMixin, list[str]):
    """
    A shared list of proxies used to process “raw” resources (sites without an API),
    since in API resources, as a rule, the intensity of calls to the service from the
    client is tracked through an authorization token and the use of proxy servers is
    pointless for a single authorization token.
    Contains all registered proxy addresses.

    """


_PROXY_LIST = _ProxyList()


def build_proxy_url(
    *,
    url_address: str = "",
    port: int | None = None,
    username: str = "",
    password: str = "",
    display_url: bool = False,
) -> str:
    """
    Conventional method of registering a link to a proxy server. If necessary,
    it allows you to output the resulting link to standard output.

    Args:
        url_address (str, optional): url address of the proxy server or service that provides the proxy.
                                The exact form of this address depends on the provider providing the proxy server.
        port (int | None, optional): proxy server port. Defaults to None.
        username (str, optional): login for authorization in the proxy server.
        password (str, optional): password for authorization in the proxy server.
        display_url (bool, optional): print the resulting url to stdout. Defaults to False.

    Returns:
        str: url string for accessing the proxy server.
    """
    url_string: URL = (
        URL(url_address)
        .with_port(port)
        .with_password(password)
        .with_user(username)
    )
    _PROXY_LIST.append(url_string.human_repr())
    if display_url:
        print(f"Prepared proxy url {url_string.human_repr()}")
    return url_string.human_repr()


def get_proxy_list() -> _ProxyList:
    """
    The function returns a copy of the current proxy list.

    Returns:
        _ProxyList: a copy of the current proxy list.
    """
    return deepcopy(_PROXY_LIST)


def remove_proxy_url(position: int) -> None:
    """
    Removes a link to a proxy server by position in the list.

    Args:
        position (int): position of the link to the proxy server. Please note that the first link is at position number zero.
    """
    del _PROXY_LIST[position]
