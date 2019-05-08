""" Implementation for The ECMWF ERA5 Reanalyses data holdings """
import logging

import pandas as pd

from .common import BaseSource, Collection

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


class ERA5Collection(Collection):
    def __init__(self, collection_spec):
        super(ERA5Collection, self).__init__(collection_spec)
        self.df = pd.DataFrame(columns=self.columns)

    def build(self):
        self._validate()


class ERA5Source(BaseSource):
    name = 'era5'
    partition_access = True
