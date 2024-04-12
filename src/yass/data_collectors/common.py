from yarl import URL

from yass.core import SingletonMixin

__all__ = ["ProxyList", "build_proxyurl", "get_builded_proxy"]


class ProxyList(SingletonMixin, list[str]): ...


_PROXY_LIST = ProxyList()


def build_proxyurl(
    *,
    address: str = "",
    port: int | None = None,
    username: str = "",
    password: str = "",
    display_url: bool = False,
) -> str:
    url_string: URL = (
        URL(address)
        .with_port(port)
        .with_password(password)
        .with_user(username)
    )
    _PROXY_LIST.append(url_string.human_repr())
    if display_url:
        print(f"Prepared proxy url {url_string.human_repr()}")
    return url_string.human_repr()


def get_builded_proxy() -> ProxyList:
    return _PROXY_LIST
