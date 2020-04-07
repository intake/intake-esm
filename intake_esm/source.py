import copy

from intake.source.base import DataSource, Schema

from .merge_util import _aggregate, _path_to_mapper, _to_nested_dict

_DATA_FORMAT_KEY = '_data_format_'


class ESMGroupDataSource(DataSource):
    version = '1.0'
    container = 'xarray'
    name = 'esm_group'
    partition_access = True

    def __init__(
        self,
        df,
        aggregation_dict,
        path_column,
        variable_column,
        data_format=None,
        format_column=None,
        cdf_kwargs={'chunks': {}},
        zarr_kwargs={},
        storage_options={},
        preprocess=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cdf_kwargs = cdf_kwargs
        self.zarr_kwargs = zarr_kwargs
        self.storage_options = storage_options
        self.preprocess = preprocess
        self._ds = None
        self.df = df.copy()
        self.aggregation_columns, self.aggregation_dict = _sanitize_aggregations(
            df, aggregation_dict
        )
        self.path_column = path_column
        self.variable_column = variable_column
        if format_column is not None:
            self.df[_DATA_FORMAT_KEY] = df[format_column]
        else:
            if data_format is None:
                raise ValueError(f'Please specify either `data_format` or `format_column`')
            else:
                self.df[_DATA_FORMAT_KEY] = [data_format] * len(df)

        self.data_format = data_format
        self.format_column = format_column

    def _get_schema(self):

        if self._ds is None:
            self._open_dataset()

            metadata = {
                'dims': dict(self._ds.dims),
                'data_vars': {k: list(self._ds[k].coords) for k in self._ds.data_vars.keys()},
                'coords': tuple(self._ds.coords.keys()),
            }
            self._schema = Schema(
                datashape=None, dtype=None, shape=None, npartitions=None, extra_metadata=metadata,
            )
        return self._schema

    def _open_dataset(self):
        nd = create_nested_dict(self.df, self.path_column, self.aggregation_columns)
        lookup = create_asset_info_lookup(self.df, self.path_column, self.variable_column)
        n_agg = len(self.aggregation_columns)
        # replace path column with mapper (dependent on filesystem type)
        mapper_dict = {
            path: _path_to_mapper(path, self.storage_options) for path in self.df[self.path_column]
        }
        ds = _aggregate(
            self.aggregation_dict,
            self.aggregation_columns,
            n_agg,
            nd,
            lookup,
            mapper_dict,
            self.zarr_kwargs,
            self.cdf_kwargs,
            self.preprocess,
        )
        ds.attrs['intake_esm_dataset_key'] = self.name
        self._ds = ds
        return ds

    def to_dask(self):
        """Return xarray object (which will have chunks)"""
        self._load_metadata()
        return self._ds

    def close(self):
        """Delete open files from memory"""
        self._ds = None
        self._schema = None


def create_nested_dict(df, path_column, aggregation_columns):
    mi = df.set_index(aggregation_columns)
    nd = _to_nested_dict(mi[path_column])
    return nd


def create_asset_info_lookup(df, path_column, variable_column):
    return dict(zip(df[path_column], tuple(zip(df[variable_column], df[_DATA_FORMAT_KEY]))))


def _sanitize_aggregations(df, aggregation_dict):
    _aggregation_dict = copy.deepcopy(aggregation_dict)
    agg_columns = list(_aggregation_dict.keys())
    drop_cols = []
    for col in agg_columns:
        if df[col].isnull().all():
            drop_cols.append(col)
            del _aggregation_dict[col]
        elif df[col].isnull().any():
            raise ValueError(
                f'The data in the {col} column should either be all NaN or there should be no NaNs'
            )

    aggregation_columns = list(filter(lambda x: x not in drop_cols, agg_columns))

    return aggregation_columns, _aggregation_dict
