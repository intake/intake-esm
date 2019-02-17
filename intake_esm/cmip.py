import os

import pandas as pd

from .common import Collection, StorageResource
from .config import INTAKE_ESM_CONFIG_FILE, SETTINGS


class CMIPCollection(Collection):
    def __init__(self, collection_name, collection_type, collection_vals):
        super(CMIPCollection, self).__init__(collection_name, collection_type, collection_vals)
        self.df = pd.DataFrame()

    def _validate(self):
        pass


class CMIPSource:
    pass
