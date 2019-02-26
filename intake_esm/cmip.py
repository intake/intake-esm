import os

import pandas as pd
from intake_xarray.netcdf import NetCDFSource

from .common import Collection, StorageResource
from .config import INTAKE_ESM_CONFIG_FILE, SETTINGS


class CMIPCollection(Collection):
    def __init__(self):
        raise NotImplementedError

    def build(self):
        raise NotImplementedError


class CMIPSource(NetCDFSource):
    """ Read CMIP data sets into xarray datasets
    """

    def __init__(self):
        raise NotImplementedError
