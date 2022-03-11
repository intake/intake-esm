"""
Useful for:
* users learning intake-esm
* building tutorials in the documentation.
"""

tutorial_catalogs = {
    'aws_cesm2_le': 'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/AWS-CESM2-LENS.json',
    'aws_cmip6': 'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/AWS-CMIP6.json',
    'google_cmip6':'https://raw.githubusercontent.com/intake/intake-esm/main/tutorial-catalogs/GOOGLE-CMIP6.json'
}


def get_url(
    name
):
    """
    Get a small-catalogue  URL from the online repository (requires internet).
    If a local copy is found then always use that to avoid network traffic.
    Available catalogues:
    * ``"aws_cesm2_le"``
    * ``"aws_cmip6"``
    * ``"google_cmip6"''

    Parameters
    ----------
    name : str
        Name of the catalog.
        e.g. 'aws_cmip6'
    """

    cat_url = tutorial_catalogs[name]
    
    return cat_url
