import itertools
import json
import logging
from collections.abc import Iterable

import dask
import intake
import numpy as np
import pandas as pd
from tqdm import tqdm

from .utils import _fetch_and_parse_json, _fetch_catalog, logger


class esm_datastore(intake.catalog.Catalog):
    """
    An intake plugin for parsing an ESM (Earth System Model) Collection/catalog
    and loading assets (netCDF files and/or Zarr stores) into xarray datasets.
    The in-memory representation for the catalog is a Pandas DataFrame.

    Parameters
    ----------
    esmcol_obj : str, pandas.DataFrame
        If string, this must be a path or URL to an ESM collection JSON file.
        If pandas.DataFrame, this must be the catalog content that would otherwise
        be in a CSV file.
    esmcol_data : dict, optional
            ESM collection spec information, by default None
    progressbar : bool, optional
        Will print a progress bar to standard error (stderr)
        when loading assets into :py:class:`~xarray.Dataset`,
        by default True
    sep : str, optional
        Delimiter to use when constructing a key for a query, by default '.'
    log_level: str, optional
        Level of logging to report, by default 'CRITICAL'
        Accepted values include:

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

    def __init__(
        self,
        esmcol_obj,
        esmcol_data=None,
        progressbar=True,
        sep='.',
        log_level='CRITICAL',
        **kwargs,
    ):

        """Intake Catalog representing an ESM Collection.
        """

        numeric_log_level = getattr(logging, log_level.upper(), None)
        if not isinstance(numeric_log_level, int):
            raise ValueError(f'Invalid log level: {log_level}')
        logger.setLevel(numeric_log_level)

        if isinstance(esmcol_obj, str):
            self.esmcol_data, self.esmcol_path = _fetch_and_parse_json(esmcol_obj)
            self.df = _fetch_catalog(self.esmcol_data, esmcol_obj)

        elif isinstance(esmcol_obj, pd.DataFrame):
            if esmcol_data is None:
                raise ValueError(f"Missing required argument: 'esmcol_data'")
            self.df = esmcol_obj
            self.esmcol_data = esmcol_data
            self.esmcol_path = None
        else:
            raise ValueError(f'{self.name} constructor not properly called!')

        self.progressbar = progressbar
        self._kwargs = kwargs
        self._to_dataset_args_token = None
        self._log_level = log_level
        self._datasets = {}
        self.sep = sep
        self.aggregation_info = self._get_aggregation_info()
        self._entries = {}
        self._grouped = self.df.groupby(self.aggregation_info['groupby_attrs'])
        self._keys = list(self._grouped.groups.keys())
        super(esm_datastore, self).__init__(**kwargs)

    def _get_aggregation_info(self):
        groupby_attrs = []
        data_format = None
        format_column_name = None
        variable_column_name = None
        aggregations = []
        aggregation_dict = {}
        agg_columns = []
        path_column_name = self.esmcol_data['assets']['column_name']

        if 'format' in self.esmcol_data['assets']:
            data_format = self.esmcol_data['assets']['format']
        else:
            format_column_name = self.esmcol_data['assets']['format_column_name']

        if 'aggregation_control' in self.esmcol_data:
            aggregation_dict = {}
            variable_column_name = self.esmcol_data['aggregation_control']['variable_column_name']
            groupby_attrs = self.esmcol_data['aggregation_control'].get('groupby_attrs', [])
            aggregations = self.esmcol_data['aggregation_control'].get('aggregations', [])
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

        info = {
            'groupby_attrs': groupby_attrs,
            'variable_column_name': variable_column_name,
            'aggregations': aggregations,
            'agg_columns': agg_columns,
            'aggregation_dict': aggregation_dict,
            'path_column_name': path_column_name,
            'data_format': data_format,
            'format_column_name': format_column_name,
        }
        return info

    def keys(self):
        """
        Get keys for the catalog entries

        Returns
        -------
        list
            keys for the catalog entries
        """
        keys = list(map(lambda x: self.sep.join(x), self._keys))
        return keys

    @property
    def key_template(self):
        """
        Return string template used to create catalog entry keys

        Returns
        -------
        str
          string template used to create catalog entry keys
        """
        return self.sep.join(self.aggregation_info['groupby_attrs'])

    def __len__(self):
        return len(self.keys())

    def _get_entries(self):
        # Due to just-in-time entry creation, we may not have all entries loaded
        # We need to make sure to create entries missing from self._entries
        missing = set(self.keys()) - set(self._entries.keys())
        for key in missing:
            _ = self[key]
        return self._entries

    def __getitem__(self, key):
        """
        This method takes a key argument and return a catalog entry
        corresponding to assets (files) that will be aggregated into a
        single xarray dataset.

        Parameters
        ----------
        key : str
          key to use for catalog entry lookup

        Returns
        -------
        intake.catalog.local.LocalCatalogEntry
             A catalog entry by name (key)

        Raises
        ------
        KeyError
            if key is not found.

        Examples
        --------
        >>> col = intake.open_esm_datastore("mycatalog.json")
        >>> entry = col["AerChemMIP.BCC.BCC-ESM1.piClim-control.AERmon.gn"]
        """
        # The canonical unique key is the key of a compatible group of assets
        try:
            return self._entries[key]
        except KeyError:
            if key in self.keys():
                _key = tuple(key.split(self.sep))
                df = self._grouped.get_group(_key)
                self._entries[key] = _make_entry(key, df, self.aggregation_info)
                return self._entries[key]
            else:
                raise KeyError(key)

    def __contains__(self, key):
        # Python falls back to iterating over the entire catalog
        # if this method is not defined. To avoid this, we implement it differently

        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __repr__(self):
        """Make string representation of object."""
        return f'<{self.esmcol_data["id"]} catalog with {len(self)} dataset(s) from {len(self.df)} asset(s)>'

    def _repr_html_(self):
        """
        Return an html representation for the catalog object.
        Mainly for IPython notebook
        """
        uniques = pd.DataFrame(self.nunique(), columns=['unique'])
        text = uniques._repr_html_()
        output = f'<p><strong>{self.esmcol_data["id"]} catalog with {len(self)} dataset(s) from {len(self.df)} asset(s)</strong>:</p> {text}'
        return output

    @classmethod
    def from_df(
        cls, df, esmcol_data=None, progressbar=True, sep='.', log_level='CRITICAL', **kwargs
    ):
        """
        Create catalog from the given dataframe

        Parameters
        ----------
        df : pandas.DataFrame
            catalog content that would otherwise be in a CSV file.
        esmcol_data : dict, optional
            ESM collection spec information, by default None
        progressbar : bool, optional
            Will print a progress bar to standard error (stderr)
            when loading assets into :py:class:`~xarray.Dataset`,
            by default True
        sep : str, optional
            Delimiter to use when constructing a key for a query, by default '.'
        log_level : str, optional
            Level of logging to report, by default 'CRITICAL'

        Returns
        -------
        intake_esm.core.esm_datastore
            Catalog object
        """
        return cls(
            df,
            esmcol_data=esmcol_data,
            progressbar=progressbar,
            sep=sep,
            log_level=log_level,
            **kwargs,
        )

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

        results = _get_subset(self.df, require_all_on=require_all_on, **query)
        ret = esm_datastore.from_df(
            results,
            esmcol_data=self.esmcol_data,
            progressbar=self.progressbar,
            sep=self.sep,
            log_level=self._log_level,
            **self._kwargs,
        )
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

        collection_data = self.esmcol_data.copy()
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

        uniques = self.unique(self.df.columns.tolist())
        nuniques = {}
        for key, val in uniques.items():
            nuniques[key] = val['count']
        return pd.Series(nuniques)

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

    def to_dataset_dict(
        self,
        zarr_kwargs={},
        cdf_kwargs={'chunks': {}},
        preprocess=None,
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

        import concurrent.futures
        import sys
        from collections import OrderedDict

        source_kwargs = OrderedDict(
            zarr_kwargs=zarr_kwargs,
            cdf_kwargs=cdf_kwargs,
            preprocess=preprocess,
            storage_options=storage_options,
        )
        token = dask.base.tokenize(source_kwargs)
        if progressbar is not None:
            self.progressbar = progressbar

        if preprocess is not None and not callable(preprocess):
            raise ValueError('preprocess argument must be callable')

        # Avoid re-loading data if nothing has changed since the last call
        if self._datasets and (token == self._to_dataset_args_token):
            return self._datasets
        else:
            self._to_dataset_args_token = token
            if self.progressbar:
                print(
                    f"""\n--> The keys in the returned dictionary of datasets are constructed as follows:\n\t'{self.key_template}'"""
                )

            def _load_source(source):
                return source.to_dask()

            sources = [source(**source_kwargs) for _, source in self.items()]

            if self.progressbar:
                total = len(sources)
                # Need to use ascii characters on Windows because there isn't
                # always full unicode support
                # (see https://github.com/tqdm/tqdm/issues/454)
                use_ascii = bool(sys.platform == 'win32')
                progress = tqdm(
                    total=total, ncols=79, ascii=use_ascii, leave=True, desc='Dataset(s)'
                )

            with concurrent.futures.ThreadPoolExecutor(max_workers=len(sources)) as executor:
                future_tasks = [executor.submit(_load_source, source) for source in sources]

                for i, task in enumerate(concurrent.futures.as_completed(future_tasks)):
                    ds = task.result()
                    self._datasets[ds.attrs['intake_esm_dataset_key']] = ds
                    if self.progressbar:
                        progress.update(1)

                if self.progressbar:
                    progress.close()

                return self._datasets


def _unique(df, columns=None):
    if isinstance(columns, str):
        columns = [columns]
    if not columns:
        columns = df.columns.tolist()
    info = {}
    for col in columns:
        values = df[col].dropna().values
        uniques = np.unique(list(_flatten_list(values))).tolist()
        info[col] = {'count': len(uniques), 'values': uniques}
    return info


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


def _flatten_list(data):
    for item in data:
        if isinstance(item, Iterable) and not isinstance(item, str):
            for x in _flatten_list(item):
                yield x
        else:
            yield item


def _make_entry(key, df, aggregation_info):
    args = dict(
        df=df,
        aggregation_dict=aggregation_info['aggregation_dict'],
        path_column=aggregation_info['path_column_name'],
        variable_column=aggregation_info['variable_column_name'],
        data_format=aggregation_info['data_format'],
        format_column=aggregation_info['format_column_name'],
    )
    entry = intake.catalog.local.LocalCatalogEntry(
        name=key, description='', driver='esm_group', args=args, metadata={}
    )
    return entry
