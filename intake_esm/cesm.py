import os
from abc import ABC, abstractclassmethod

import pandas as pd

from .common import Collection
from .config import INTAKE_ESM_CONFIG_FILE, SETTINGS


class CESMCollection(Collection):
    def __init__(self, collection_name, collection_type, collection_vals):
        super(CESMCollection, self).__init__(collection_name, collection_type, collection_vals)
        self.df = pd.DataFrame()
        self.component_streams = self.collection_definition.get("component_streams", None)
        self.replacements = self.collection_definition.get("replacements", {})
        self.overwrite_existing = self.collection_definition.get("overwrite_existing", True)
        self.include_cache_dir = self.collection_definition.get("include_cache_dir", True)
        if self.db_dir:
            os.makedirs(self.db_dir, exist_ok=True)
            self.collection_db_file = f"{self.db_dir}/{self.collection_name}.csv"

    def _validate(self):
        for req_col in ["files", "sequence_order"]:
            if req_col not in self.columns:
                raise ValueError(f"Missing required column:{req_col}")
