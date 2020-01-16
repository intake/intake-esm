import copy

import fsspec
import intake

from .merge_util import _aggregate, _create_asset_info_lookup, _open_asset, _to_nested_dict


class AbstractESMEntry(intake.catalog.entry.CatalogEntry):
    def __init__(self, df, keys=[], col_data={}):
        self.df = df
        self._keys = keys
        self._groups = {}
        self._col_data = col_data
        self.asset_loading_info = self._get_asset_loading_info()
        self.asset_agg_info = self._get_asset_aggregation_info()
        self.zarr_kwargs = {}
        self.cdf_kwargs = {}
        self.preprocess = None
        self._mapper_dict = {}
        super().__init__()

    def __len__(self):
        return len(self.df)

    def __getitem__(self, key):
        raise NotImplementedError

    def _get_asset_loading_info(self):
        path_column_name = self._col_data['assets']['column_name']
        if 'format' in self._col_data['assets']:
            use_format_column = False
            data_format = None
        else:
            use_format_column = True
            data_format = self._col_data['assets']['format']

        return {
            'path_column_name': path_column_name,
            'use_format_column': use_format_column,
            'data_format': data_format,
        }

    def _get_asset_aggregation_info(self):
        return {}

    def to_xarray(
        self, zarr_kwargs={}, cdf_kwargs={'chunks': {}}, preprocess=None, storage_options={}
    ):
        if (
            'chunks' in cdf_kwargs
            and not cdf_kwargs['chunks']
            and self._col_data['assets'].get('format') != 'zarr'
        ):
            print(
                '\nxarray will load netCDF datasets with dask using a single chunk for all arrays.'
            )
            print('For effective chunking, please provide chunks in cdf_kwargs.')
            print("For example: cdf_kwargs={'chunks': {'time': 36}}\n")

        self.zarr_kwargs = zarr_kwargs
        self.cdf_kwargs = cdf_kwargs
        self.storage_options = storage_options
        if preprocess is not None and not callable(preprocess):
            raise ValueError('preprocess argument must be callable')

        self.preprocess = preprocess
        return self._open_dataset()

    def _get_mapper_dict(self):
        if not self._mapper_dict:
            mapper_dict = {
                path: fsspec.get_mapper(path, **self.storage_options)
                for path in self.df[self.asset_loading_info['path_column_name']]
            }  # replace path column with mapper (dependent on filesystem type)
            self._mapper_dict = mapper_dict

        return self._mapper_dict

    def _open_dataset(self):
        ...

    def keys(self):
        ...


class SingleEntry(AbstractESMEntry):
    def keys(self):
        columns = self.df.columns.tolist()
        columns.remove(self.asset_loading_info['path_column_name'])
        print('.'.join(columns))
        return self._keys

    def _open_dataset(self):

        mapper_dict = self._get_mapper_dict()
        nd = self.df.iloc[0][self.asset_loading_info['path_column_name']]
        if self.asset_loading_info['use_format_column']:
            data_format = self.asset_loading_info['format_column_name']
        else:
            data_format = self._col_data['assets']['format']

        ds = _open_asset(
            mapper_dict[nd],
            data_format=data_format,
            zarr_kwargs=self.zarr_kwargs,
            cdf_kwargs=self.cdf_kwargs,
            preprocess=self.preprocess,
        )
        return ds


class AggregateEntry(AbstractESMEntry):
    def _get_asset_aggregation_info(self):
        groupby_attrs = []
        variable_column_name = None
        aggregations = []
        aggregation_dict = {}
        agg_columns = []

        if 'aggregation_control' in self._col_data:
            variable_column_name = self._col_data['aggregation_control']['variable_column_name']
            groupby_attrs = self._col_data['aggregation_control'].get('groupby_attrs', [])
            aggregations = self._col_data['aggregation_control'].get('aggregations', [])
            # Sort aggregations to make sure join_existing is always done before join_new
            aggregations = sorted(aggregations, key=lambda i: i['type'], reverse=True)
            for agg in aggregations:
                key = agg['attribute_name']
                rest = agg.copy()
                del rest['attribute_name']
                aggregation_dict[key] = rest

        agg_columns = list(aggregation_dict.keys())

        if not groupby_attrs:
            groupby_attrs = self.df.columns.tolist()

        # filter groupby_attrs to ensure no columns with all nans
        def _allnan_or_nonan(column):
            if self.df[column].isnull().all():
                return False
            elif self.df[column].isnull().any():
                raise ValueError(
                    f'The data in the {column} column should either be all NaN or there should be no NaNs'
                )
            else:
                return True

        groupby_attrs = list(filter(_allnan_or_nonan, groupby_attrs))
        agg_info = {
            'groupby_attrs': groupby_attrs,
            'variable_column_name': variable_column_name,
            'aggregations': aggregations,
            'aggregation_dict': aggregation_dict,
            'agg_columns': agg_columns,
        }
        return agg_info

    def keys(self):
        if not self._groups:
            self._groups = self.df.groupby(self.asset_agg_info['groupby_attrs'])

        _keys = self._groups.groups.keys()
        self._keys = _keys
        # self._keys = ['.'.join(key) for key in _keys]
        print('.'.join(self.asset_agg_info['groupby_attrs']))
        return self._keys

    # def _open_dataset(self):
    #    mapper_dict = self._get_mapper_dict()
    #    return mapper_dict

    def __opendataset__(self, key):
        aggregation_dict = copy.deepcopy(self.asset_agg_info['aggregation_dict'])
        agg_columns = copy.deepcopy(self.asset_agg_info['agg_columns'])
        drop_cols = []
        if not self._groups:
            self.keys()
        assert key in self._groups.groups.keys()
        df = self.df.iloc[self._groups.groups[key]]
        for col in agg_columns:
            if df[col].isnull().all():
                drop_cols.append(col)
                del aggregation_dict[col]
            elif df[col].isnull().any():
                raise ValueError(
                    f'The data in the {col} column for {key} group should either be all NaN or there should be no NaNs'
                )
        agg_columns = list(filter(lambda x: x not in drop_cols, agg_columns))
        # the number of aggregation columns determines the level of recursion
        n_agg = len(agg_columns)

        mi = df.set_index(agg_columns)
        nd = _to_nested_dict(mi[self.asset_loading_info['path_column_name']])

        if self.asset_loading_info['use_format_column']:
            format_column_name = self._col_data['assets']['format_column_name']
            lookup = _create_asset_info_lookup(
                df,
                path_column_name=self.asset_loading_info['path_column_name'],
                variable_column_name=self.asset_agg_info['variable_column_name'],
                format_column_name=format_column_name,
            )

        else:
            lookup = _create_asset_info_lookup(
                df,
                path_column_name=self.asset_loading_info['path_column_name'],
                variable_column_name=self.asset_agg_info['variable_column_name'],
                data_format=self._col_data['assets']['format'],
            )

        mapper_dict = self._get_mapper_dict()
        ds = _aggregate(
            aggregation_dict,
            agg_columns,
            n_agg,
            nd,
            lookup,
            mapper_dict,
            self.zarr_kwargs,
            self.cdf_kwargs,
            self.preprocess,
        )
        return ds

    def aggregate(self):
        ...

    def to_dataset_dict(self):
        ...
