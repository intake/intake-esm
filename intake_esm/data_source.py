import pandas as pd
from intake.source.base import DataSource

from ._types import ESMGroupedDataSourceModel, ESMSingleDataSourceModel


class ESMSingleDataSource(DataSource):
    name = 'esm_single_data_source'
    version = '1.0'
    container = 'xarray'
    partition_access = True

    def __init__(self, model: ESMSingleDataSourceModel) -> 'ESMSingleDataSource':
        super().__init__(**model.kwargs)
        self.model = model
        self.df = pd.DataFrame.from_records([self.model.record])
        self._ds = None

    def __repr__(self) -> str:
        return f'<{type(self).__name__}  (name: {self.model.key}, asset: {len(self.df)}>)'


class ESMGroupedDataSource(DataSource):
    name = 'esm_grouped_data_source'
    version = '1.0'
    container = 'xarray'
    partition_access = True

    def __init__(self, model: ESMGroupedDataSourceModel) -> 'ESMGroupedDataSource':
        super().__init__(**model.kwargs)
        self.model = model
        self.df = pd.DataFrame.from_records(self.model.records)
        self._ds = None

    def __repr__(self) -> str:
        return f'<{type(self).__name__}  (name: {self.model.key}, asset(s): {len(self.df)})>'
