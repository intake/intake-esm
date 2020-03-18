import copy
import itertools
import json
import logging

import dask
import intake
import numpy as np
import pandas as pd

from .merge_util import _aggregate, _create_asset_info_lookup, _path_to_mapper, _to_nested_dict
from .utils import _fetch_and_parse_json, _fetch_catalog, _get_dask_client, logger


class esm_datastore(intake.catalog.Catalog):
    """ An intake plugin for parsing an ESM (Earth System Model) Collection/catalog and loading assets
    (netCDF files and/or Zarr stores) into xarray datasets.

    The in-memory representation for the catalog is a Pandas DataFrame.

    Parameters
    ----------

    esmcol_path : str
        Path or URL to an ESM collection JSON file
    progressbar : bool
         If True, will print a progress bar to standard error (stderr)
         when loading assets into :py:class:`~xarray.Dataset`.
    log_level: str
        Level of logging to report. Accepted values include:

        - CRITICAL
        - ERROR
        - WARNING
        - INFO
        - DEBUG
        - NOTSET
    **kwargs :
        Additional keyword arguments are passed through to the
        :py:class:`~intake.catalog.Catalog` base class.



    Examples
    --------

    At import time, this plugin is available in intake's registry as `esm_datastore` and
    can be accessed with `intake.open_esm_datastore()`:

    >>> import intake
    >>> url = "https://raw.githubusercontent.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json"
    >>> col = intake.open_esm_datastore(url)
    >>> col.df.head()
    activity_id institution_id source_id experiment_id  ... variable_id grid_label                                             zstore dcpp_init_year
    0  AerChemMIP            BCC  BCC-ESM1        ssp370  ...          pr         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
    1  AerChemMIP            BCC  BCC-ESM1        ssp370  ...        prsn         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
    2  AerChemMIP            BCC  BCC-ESM1        ssp370  ...         tas         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
    3  AerChemMIP            BCC  BCC-ESM1        ssp370  ...      tasmax         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
    4  AerChemMIP            BCC  BCC-ESM1        ssp370  ...      tasmin         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN

    """

    name = 'esm_datastore'
    container = 'xarray'

    def __init__(self, esmcol_path, progressbar=True, log_level='CRITICAL', **kwargs):
        """Main entry point.
        """

        super().__init__(**kwargs)

        numeric_log_level = getattr(logging, log_level.upper(), None)
        if not isinstance(numeric_log_level, int):
            raise ValueError(f'Invalid log level: {log_level}')
        logger.setLevel(numeric_log_level)
        self.progressbar = progressbar
        self._col_data, self.esmcol_path = _fetch_and_parse_json(esmcol_path)
        self.df = _fetch_catalog(self._col_data, self.esmcol_path)
        self._entries = {}
        self._ds = None
        self.zarr_kwargs = None
        self.cdf_kwargs = None
        self.preprocess = None
        self.aggregate = None

    def search(self, require_all_on=None, **query):
        """Search for entries in the catalog.

        Parameters
        ----------
        require_all_on : str, list
           name of columns to use when enforcing the query criteria.
           For example: `col.search(experiment_id=['piControl', 'historical'], require_all_on='source_id')`
           returns all assets with `source_id` that has both `piControl` and `historical`
           experiments.

        Returns
        -------
        cat : :py:class:`~intake_esm.core.esm_datastore`
          A new Catalog with a subset of the entries in this Catalog.

        Examples
        --------
        >>> import intake
        >>> col = intake.open_esm_datastore("pangeo-cmip6.json")
        >>> col.df.head(3)
        activity_id institution_id source_id  ... grid_label                                             zstore dcpp_init_year
        0  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
        1  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
        2  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN

        >>> cat = col.search(source_id=['BCC-CSM2-MR', 'CNRM-CM6-1', 'CNRM-ESM2-1'],
        ...                 experiment_id=['historical', 'ssp585'], variable_id='pr',
        ...                table_id='Amon', grid_label='gn')
        >>> cat.df.head(3)
            activity_id institution_id    source_id  ... grid_label                                             zstore dcpp_init_year
        260        CMIP            BCC  BCC-CSM2-MR  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r1i...            NaN
        346        CMIP            BCC  BCC-CSM2-MR  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r2i...            NaN
        401        CMIP            BCC  BCC-CSM2-MR  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r3i...            NaN

        """

        ret = copy.copy(self)
        ret.df = _get_subset(self.df, require_all_on=require_all_on, **query)
        return ret

    def serialize(self, name, directory=None, catalog_type='dict'):
        """Serialize collection/catalog to corresponding json and csv files.

        Parameters
        ----------
        name : str
            name to use when creating ESM collection json file and csv catalog.
        directory : str, PathLike, default None
            The path to the local directory. If None, use the current directory
        catalog_type: str, default 'dict'
            Whether to save the catalog table as a dictionary in the JSON file or as a separate CSV file.

        Notes
        -----
        Large catalogs can result in large JSON files. To keep the JSON file size manageable, call with
        `catalog_type='file'` to save catalog as a separate CSV file.

        Examples
        --------
        >>> import intake
        >>> col = intake.open_esm_datastore("pangeo-cmip6.json")
        >>> col_subset = col.search(source_id="BCC-ESM1", grid_label="gn",
        ...                      table_id="Amon", experiment_id="historical")
        >>> col_subset.serialize(name="cmip6_bcc_esm1", catalog_type='file')
        Writing csv catalog to: cmip6_bcc_esm1.csv.gz
        Writing ESM collection json file to: cmip6_bcc_esm1.json
        """

        def _clear_old_catalog(catalog_data):
            """ Remove any old references to the catalog."""
            for key in {'catalog_dict', 'catalog_file'}:
                _ = catalog_data.pop(key, None)
            return catalog_data

        from pathlib import Path

        csv_file_name = Path(f'{name}.csv.gz')
        json_file_name = Path(f'{name}.json')
        if directory:
            directory = Path(directory)
            directory.mkdir(parents=True, exist_ok=True)
            csv_file_name = directory / csv_file_name
            json_file_name = directory / json_file_name

        collection_data = self._col_data.copy()
        collection_data = _clear_old_catalog(collection_data)
        collection_data['id'] = name

        catalog_length = len(self.df)
        if catalog_type == 'file':
            collection_data['catalog_file'] = csv_file_name.as_posix()
            print(f'Writing csv catalog with {catalog_length} entries to: {csv_file_name}')
            self.df.to_csv(csv_file_name, compression='gzip', index=False)
        else:
            print(f'Writing catalog with {catalog_length} entries into: {json_file_name}')
            collection_data['catalog_dict'] = self.df.to_dict(orient='records')

        print(f'Writing ESM collection json file to: {json_file_name}')
        with open(json_file_name, 'w') as outfile:
            json.dump(collection_data, outfile)

    def nunique(self):
        """Count distinct observations across dataframe columns
        in the catalog.

        Examples
        --------
        >>> import intake
        >>> col = intake.open_esm_datastore("pangeo-cmip6.json")
        >>> col.df.head(3)
        activity_id institution_id source_id  ... grid_label                                             zstore dcpp_init_year
        0  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
        1  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
        2  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
        >>> col.nunique()
        activity_id          10
        institution_id       23
        source_id            48
        experiment_id        29
        member_id            86
        table_id             19
        variable_id         187
        grid_label            7
        zstore            27437
        dcpp_init_year       59
        dtype: int64
        """
        return self.df.nunique()

    def unique(self, columns=None):
        """Return unique values for given columns in the
        catalog.

        Parameters
        ----------
        columns : str, list
           name of columns for which to get unique values

        Returns
        -------
        info : dict
           dictionary containing count, and unique values

        Examples
        --------
        >>> import intake
        >>> import pprint
        >>> col = intake.open_esm_datastore("pangeo-cmip6.json")
        >>> col.df.head(3)
        activity_id institution_id source_id  ... grid_label                                             zstore dcpp_init_year
        0  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
        1  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
        2  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
        >>> uniques = col.unique(columns=["activity_id", "source_id"])
        >>> pprint.pprint(uniques)
        {'activity_id': {'count': 10,
                        'values': ['AerChemMIP',
                                    'C4MIP',
                                    'CMIP',
                                    'DAMIP',
                                    'DCPP',
                                    'HighResMIP',
                                    'LUMIP',
                                    'OMIP',
                                    'PMIP',
                                    'ScenarioMIP']},
        'source_id': {'count': 17,
                    'values': ['BCC-ESM1',
                                'CNRM-ESM2-1',
                                'E3SM-1-0',
                                'MIROC6',
                                'HadGEM3-GC31-LL',
                                'MRI-ESM2-0',
                                'GISS-E2-1-G-CC',
                                'CESM2-WACCM',
                                'NorCPM1',
                                'GFDL-AM4',
                                'GFDL-CM4',
                                'NESM3',
                                'ECMWF-IFS-LR',
                                'IPSL-CM6A-ATM-HR',
                                'NICAM16-7S',
                                'GFDL-CM4C192',
                                'MPI-ESM1-2-HR']}}

        """
        return _unique(self.df, columns)

    def __repr__(self):
        """Make string representation of object."""
        info = self.nunique().to_dict()
        output = []
        for key, values in info.items():
            output.append(f'{values} {key}(s)\n')
        output = '\n\t> '.join(output)
        items = len(self.df.index)
        return f'{self._col_data["id"]}-ESM Collection with {items} entries:\n\t> {output}'

    def to_dataset_dict(
        self,
        zarr_kwargs={},
        cdf_kwargs={'chunks': {}},
        preprocess=None,
        aggregate=True,
        storage_options={},
        progressbar=None,
    ):
        """Load catalog entries into a dictionary of xarray datasets.

        Parameters
        ----------
        zarr_kwargs : dict
            Keyword arguments to pass to `xarray.open_zarr()` function
        cdf_kwargs : dict
            Keyword arguments to pass to `xarray.open_dataset()` function
        preprocess : callable, optional
            If provided, call this function on each dataset prior to aggregation.
        aggregate : bool, optional
            If "False", no aggregation will be done.
        storage_options : dict, optional
            Parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.
            progressbar : bool
        progressbar : bool
            If True, will print a progress bar to standard error (stderr)
            when loading assets into :py:class:`~xarray.Dataset`.

        Returns
        -------
        dsets : dict
           A dictionary of xarray :py:class:`~xarray.Dataset`s.

        Examples
        --------
        >>> import intake
        >>> col = intake.open_esm_datastore("glade-cmip6.json")
        >>> cat = col.search(source_id=['BCC-CSM2-MR', 'CNRM-CM6-1', 'CNRM-ESM2-1'],
        ...                       experiment_id=['historical', 'ssp585'], variable_id='pr',
        ...                       table_id='Amon', grid_label='gn')
        >>> dsets = cat.to_dataset_dict()
        --> The keys in the returned dictionary of datasets are constructed as follows:
        'activity_id.institution_id.source_id.experiment_id.table_id.grid_label'
        --> There will be 2 group(s)
        >>> dsets.keys()
        dict_keys(['CMIP.BCC.BCC-CSM2-MR.historical.Amon.gn', 'ScenarioMIP.BCC.BCC-CSM2-MR.ssp585.Amon.gn'])
        >>> dsets['CMIP.BCC.BCC-CSM2-MR.historical.Amon.gn']
        <xarray.Dataset>
        Dimensions:    (bnds: 2, lat: 160, lon: 320, member_id: 3, time: 1980)
        Coordinates:
        * lon        (lon) float64 0.0 1.125 2.25 3.375 ... 355.5 356.6 357.8 358.9
        * lat        (lat) float64 -89.14 -88.03 -86.91 -85.79 ... 86.91 88.03 89.14
        * time       (time) object 1850-01-16 12:00:00 ... 2014-12-16 12:00:00
        * member_id  (member_id) <U8 'r1i1p1f1' 'r2i1p1f1' 'r3i1p1f1'
        Dimensions without coordinates: bnds
        Data variables:
            lat_bnds   (lat, bnds) float64 dask.array<chunksize=(160, 2), meta=np.ndarray>
            lon_bnds   (lon, bnds) float64 dask.array<chunksize=(320, 2), meta=np.ndarray>
            time_bnds  (time, bnds) object dask.array<chunksize=(1980, 2), meta=np.ndarray>
            pr         (member_id, time, lat, lon) float32 dask.array<chunksize=(1, 600, 160, 320), meta=np.ndarray>
        """

        # set _schema to None to remove any previously cached dataset
        self._schema = None

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
        self.aggregate = aggregate

        self.storage_options = storage_options
        if preprocess is not None and not callable(preprocess):
            raise ValueError('preprocess argument must be callable')

        self.preprocess = preprocess
        if progressbar is not None:
            self.progressbar = progressbar

        return self._open_dataset()

    def _open_dataset(self):

        path_column_name = self._col_data['assets']['column_name']
        if 'format' in self._col_data['assets']:
            use_format_column = False
        else:
            use_format_column = True

        # replace path column with mapper (dependent on filesystem type)
        mapper_dict = {
            path: _path_to_mapper(path, self.storage_options) for path in self.df[path_column_name]
        }

        groupby_attrs = []
        variable_column_name = None
        aggregations = []
        aggregation_dict = {}
        agg_columns = []
        if self.aggregate:
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

        groups = self.df.groupby(groupby_attrs)

        if agg_columns:
            keys = '.'.join(groupby_attrs)
        else:
            keys = groupby_attrs.copy()
            keys.remove(path_column_name)
            keys = '.'.join(keys)

        dsets = []
        total = len(groups)
        load_group_dataset_delayed = dask.delayed(_load_group_dataset)
        tasks = [
            load_group_dataset_delayed(
                key,
                df,
                self._col_data,
                agg_columns,
                aggregation_dict,
                path_column_name,
                variable_column_name,
                use_format_column,
                mapper_dict,
                self.zarr_kwargs,
                self.cdf_kwargs,
                self.preprocess,
            )
            for key, df in groups
        ]

        client = _get_dask_client()

        if self.progressbar:
            print(
                f"""\n--> The keys in the returned dictionary of datasets are constructed as follows:\n\t'{keys}'
                \n--> There is/are {total} group(s)"""
            )

        if client is None:
            from multiprocessing.pool import ThreadPool

            with dask.config.set(pool=ThreadPool(total)):
                logger.info(f'Using {total} threads for loading dataset groups')
                if self.progressbar:
                    from dask.diagnostics import ProgressBar

                    p = ProgressBar()
                    p.register()
                dsets = dask.compute(*tasks)
                if self.progressbar:
                    p.unregister()

        else:
            logger.info(f'Using dask cluster: {client} for loading dataset groups')
            futures = client.compute(tasks)

            if self.progressbar:
                from distributed import progress

                progress(futures)

            dsets = client.gather(futures)

        self._ds = {group_id: ds for (group_id, ds) in dsets}
        return self._ds


def _unique(df, columns):
    if isinstance(columns, str):
        columns = [columns]
    if not columns:
        columns = df.columns

    info = {}
    for col in columns:
        uniques = df[col].unique().tolist()
        info[col] = {'count': len(uniques), 'values': uniques}
    return info


def _load_group_dataset(
    key,
    df,
    col_data,
    agg_columns,
    aggregation_dict,
    path_column_name,
    variable_column_name,
    use_format_column,
    mapper_dict,
    zarr_kwargs,
    cdf_kwargs,
    preprocess,
):

    aggregation_dict = copy.deepcopy(aggregation_dict)
    agg_columns = agg_columns.copy()
    drop_cols = []
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

    if agg_columns:
        mi = df.set_index(agg_columns)
        nd = _to_nested_dict(mi[path_column_name])
        group_id = '.'.join(key)
    else:
        nd = df.iloc[0][path_column_name]
        # Cast key from tuple to list
        key = list(key)
        # Remove path from the list
        key.remove(nd)
        group_id = '.'.join(key)

    if use_format_column:
        format_column_name = col_data['assets']['format_column_name']
        lookup = _create_asset_info_lookup(
            df, path_column_name, variable_column_name, format_column_name=format_column_name
        )
    else:
        lookup = _create_asset_info_lookup(
            df, path_column_name, variable_column_name, data_format=col_data['assets']['format']
        )

    ds = _aggregate(
        aggregation_dict,
        agg_columns,
        n_agg,
        nd,
        lookup,
        mapper_dict,
        zarr_kwargs,
        cdf_kwargs,
        preprocess,
    )

    return group_id, ds


def _get_subset(df, require_all_on=None, **query):
    if not query:
        return pd.DataFrame(columns=df.columns)
    condition = np.ones(len(df), dtype=bool)

    query = _normalize_query(query)

    for key, val in query.items():
        if isinstance(val, (tuple, list)):
            condition_i = np.zeros(len(df), dtype=bool)
            for val_i in val:
                condition_i = condition_i | (df[key] == val_i)
            condition = condition & condition_i
        elif val is not None:
            condition = condition & (df[key] == val)
    query_results = df.loc[condition]

    if require_all_on:

        if isinstance(require_all_on, str):
            require_all_on = [require_all_on]

        _query = query.copy()

        # Make sure to remove columns that were already
        # specified in the query when specified in `require_all_on`. For example,
        # if query = dict(variable_id=["A", "B"], source_id=["FOO", "BAR"])
        # and require_all_on = ["source_id"], we need to make sure `source_id` key is
        # not present in _query for the logic below to work
        for key in require_all_on:
            _query.pop(key, None)

        keys = list(_query.keys())

        grouped = query_results.groupby(require_all_on)
        values = [tuple(v) for v in _query.values()]

        condition = set(itertools.product(*values))

        results = []
        for key, group in grouped:
            index = group.set_index(keys).index
            if not isinstance(index, pd.MultiIndex):
                index = set([(element,) for element in index.to_list()])
            else:
                index = set(index.to_list())

            if index == condition:
                results.append(group)

        if len(results) >= 1:
            return pd.concat(results).reset_index(drop=True)

        else:
            return pd.DataFrame(columns=df.columns)

    else:
        return query_results.reset_index(drop=True)


def _normalize_query(query):
    q = query.copy()
    for key, val in q.items():
        if isinstance(val, str):
            q[key] = [val]
    return q
