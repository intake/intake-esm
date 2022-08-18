"""
Useful for:
* users learning intake-esm
* building tutorials in the documentation.
"""
from __future__ import annotations

DEFAULT_CATALOGS = {
    'aws_cesm2_le': 'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/AWS-CESM2-LENS.json',
    'aws_cmip6': 'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/AWS-CMIP6.json',
    'google_cmip6': 'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/GOOGLE-CMIP6.json',
}


def get_url(name: str) -> str:
    """
    Get a small-catalog URL from the online repository (requires internet).
    If a local copy is found then always use that to avoid network traffic.
    Available catalogs:

    * ``"aws_cesm2_le"``
    * ``"aws_cmip6"``
    * ``"google_cmip6"''

    Parameters
    ----------
    name : str
        Name of the catalog. e.g. 'aws_cmip6'

    Returns
    -------
    str
        URL of the catalog.
    """

    if name in DEFAULT_CATALOGS:
        return DEFAULT_CATALOGS[name]

    raise KeyError(
        f'KeyError: {name} is an unknown key. Only small-catalogs in our `tutorial-catalogs`'
        f'directory are supported with this method. Valid values include: {get_available_cats()} .'
    )


def get_available_cats() -> list[str]:
    """
    Get a list of all supported small-catalog key names that map to URL from the online repository.

    Returns
    -------
    list[str]
        List of all supported small-catalog key names.
    """

    return list(DEFAULT_CATALOGS.keys())
