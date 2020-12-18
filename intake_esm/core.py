import concurrent.futures
import json
import pathlib
from collections import OrderedDict, namedtuple
from copy import deepcopy
from typing import Any, Dict, List, Tuple, Union
from warnings import warn

import dask
import intake
import pandas as pd
import xarray as xr
from fastprogress.fastprogress import progress_bar
from intake.catalog import Catalog

from .search import _get_columns_with_iterables, _unique, search
from .utils import _fetch_and_parse_json, _fetch_catalog

_AGGREGATIONS_TYPES = {'join_existing', 'join_new', 'union'}


class esm_datastore(Catalog):
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
    csv_kwargs : dict, optional
        Additional keyword arguments passed through to the
        :py:func:`~pandas.read_csv` function.
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
        esmcol_obj: Union[str, pd.DataFrame],
        esmcol_data: Dict[str, Any] = None,
        progressbar: bool = True,
        sep: str = '.',
        csv_kwargs: Dict[str, Any] = None,
        **kwargs,
    ):

        """Intake Catalog representing an ESM Collection."""
        super(esm_datastore, self).__init__(**kwargs)
        if isinstance(esmcol_obj, (str, pathlib.PurePath)):
            self.esmcol_data, self.esmcol_path = _fetch_and_parse_json(esmcol_obj)
            self._df, self.catalog_file = _fetch_catalog(self.esmcol_data, esmcol_obj, csv_kwargs)

        elif isinstance(esmcol_obj, pd.DataFrame):
            if esmcol_data is None:
                raise ValueError("Missing required argument: 'esmcol_data'")
            self._df = esmcol_obj
            self.esmcol_data = esmcol_data
            self.esmcol_path = None
            self.catalog_file = None
        else:
            raise ValueError(
                f'{self.name} constructor not properly called! `esmcol_obj` is of type: {type(esmcol_obj)}, however valid types of `esmcol_obj` are either `str` or `pathlib.PurePath` or `pandas.DataFrame`. '
            )

        self.progressbar = progressbar
        self._kwargs = kwargs
        self._to_dataset_args_token = None
        self._datasets = None
        self.sep = sep
        self._data_format, self._format_column_name = None, None
        self._path_column_name = self.esmcol_data['assets']['column_name']
        if 'format' in self.esmcol_data['assets']:
            self._data_format = self.esmcol_data['assets']['format']
        else:
            self._format_column_name = self.esmcol_data['assets']['format_column_name']
        self._columns_with_iterables = _get_columns_with_iterables(self.df)
        self.aggregation_info = self._get_aggregation_info()
        self._entries = {}
        self._set_groups_and_keys()
        self._requested_variables = []

        if self.variable_column_name:
            self._multiple_variable_assets = (
                self.variable_column_name in self._columns_with_iterables
            )
        else:
            self._multiple_variable_assets = False

    def _set_groups_and_keys(self):
        if self.aggregation_info.groupby_attrs and set(self.df.columns) != set(
            self.aggregation_info.groupby_attrs
        ):
            self._grouped = self.df.groupby(self.aggregation_info.groupby_attrs)
            internal_keys = self._grouped.groups.keys()
            public_keys = []
            for key in internal_keys:
                if isinstance(key, str):
                    p_key = key
                else:
                    p_key = self.sep.join(str(v) for v in key)
                public_keys.append(p_key)

        else:
            self._grouped = self.df
            internal_keys = list(self._grouped.index)
            public_keys = [
                self.sep.join(str(v) for v in row.values) for _, row in self._grouped.iterrows()
            ]

        self._keys = dict(zip(public_keys, internal_keys))

    def _allnan_or_nonan(self, column: str) -> bool:
        """
        Helper function used to filter groupby_attrs to ensure no columns with all nans

        Parameters
        ----------
        column : str
            Column name

        Returns
        -------
        bool
            Whether the dataframe column has all NaNs or no NaN valles

        Raises
        ------
        ValueError
            When the column has a mix of NaNs non NaN values
        """
        if self.df[column].isnull().all():
            return False
        if self.df[column].isnull().any():
            raise ValueError(
                f'The data in the {column} column should either be all NaN or there should be no NaNs'
            )
        return True

    def _get_aggregation_info(self):

        AggregationInfo = namedtuple(
            'AggregationInfo',
            [
                'groupby_attrs',
                'variable_column_name',
                'aggregations',
                'agg_columns',
                'aggregation_dict',
            ],
        )

        groupby_attrs = []
        variable_column_name = None
        aggregations = []
        aggregation_dict = {}
        agg_columns = []

        if 'aggregation_control' in self.esmcol_data:
            variable_column_name = self.esmcol_data['aggregation_control']['variable_column_name']
            groupby_attrs = self.esmcol_data['aggregation_control'].get('groupby_attrs', [])
            aggregations = self.esmcol_data['aggregation_control'].get('aggregations', [])
            aggregations, aggregation_dict, agg_columns = _construct_agg_info(aggregations)
            groupby_attrs = list(filter(self._allnan_or_nonan, groupby_attrs))

        if not aggregations:
            groupby_attrs = []

        # Cast all agg_columns with iterables to tuple values so as
        # to avoid hashing issues (e.g. TypeError: unhashable type: 'list')

        columns = set(self._columns_with_iterables).intersection(set(agg_columns))
        if columns:
            for column in columns:
                self.df[column] = self.df[column].map(tuple)

        aggregation_info = AggregationInfo(
            groupby_attrs,
            variable_column_name,
            aggregations,
            agg_columns,
            aggregation_dict,
        )
        return aggregation_info

    def keys(self) -> List:
        """
        Get keys for the catalog entries

        Returns
        -------
        list
            keys for the catalog entries
        """
        return self._keys.keys()

    @property
    def key_template(self) -> str:
        """
        Return string template used to create catalog entry keys

        Returns
        -------
        str
          string template used to create catalog entry keys
        """
        if self.aggregation_info.groupby_attrs:
            template = self.sep.join(self.aggregation_info.groupby_attrs)
        else:
            template = self.sep.join(self.df.columns)
        return template

    @property
    def df(self) -> pd.DataFrame:
        """
        Return pandas :py:class:`~pandas.DataFrame`.
        """
        return self._df

    @df.setter
    def df(self, value: pd.DataFrame):
        self._df = value
        self._set_groups_and_keys()

    @property
    def groupby_attrs(self) -> list:
        """
        Dataframe columns used to determine groups of compatible datasets.

        Returns
        -------
        list
            Columns used to determine groups of compatible datasets.
        """
        return self.aggregation_info.groupby_attrs

    @groupby_attrs.setter
    def groupby_attrs(self, value: list) -> None:
        groupby_attrs = list(filter(self._allnan_or_nonan, value))
        self.aggregation_info = self.aggregation_info._replace(groupby_attrs=groupby_attrs)
        self._set_groups_and_keys()
        self._entries = {}

    @property
    def variable_column_name(self) -> str:
        """
        Name of the column that contains the variable name.
        """
        return self.aggregation_info.variable_column_name

    @variable_column_name.setter
    def variable_column_name(self, value: str) -> None:
        self.aggregation_info = self.aggregation_info._replace(variable_column_name=value)

    @property
    def aggregations(self):
        return self.aggregation_info.aggregations

    @property
    def agg_columns(self) -> list:
        """
        List of columns used to merge/concatenate compatible
        multiple :py:class:`~xarray.Dataset` into a single :py:class:`~xarray.Dataset`.
        """
        return self.aggregation_info.agg_columns

    @property
    def aggregation_dict(self) -> dict:
        return self.aggregation_info.aggregation_dict

    def update_aggregation(
        self, attribute_name: str, agg_type: str = None, options: dict = None, delete=False
    ):
        """
        Updates aggregation operations info.

        Parameters
        ----------
        attribute_name : str
            Name of attribute (column) across which to aggregate.

        agg_type : str, optional
            Type of aggregation operation to apply. Valid values include:
            `join_new`, `join_existing`, `union`, by default None

        options : dict, optional
            Aggregration settings that are passed as keywords arguments to
            :py:func:`~xarray.concat` or :py:func:`~xarray.merge`. For `join_existing`, it must contain
            the name of the existing dimension to use (for e.g.: something like {'dim': 'time'}).,
            by default None

        delete : bool, optional
             Whether to delete/remove/disable aggregation operations for a particular attribute,
             by default False
        """

        def validate_type(t):
            assert (
                t in _AGGREGATIONS_TYPES
            ), f'Invalid aggregation agg_type={t}. Valid values are: {list(_AGGREGATIONS_TYPES)}.'

        def validate_attribute_name(name):
            assert (
                name in self.df.columns
            ), f'Attribute_name={attribute_name} is invalid. Attribute name must exist as a column in the dataframe. Valid values: {self.df.columns.tolist()}.'

        def validate_options(options):
            assert isinstance(
                options, dict
            ), f'Options must be a dictionary. Found the type of options={options} to be {type(options)}.'

        aggregations = self.aggregations.copy()
        validate_attribute_name(attribute_name)
        found = False
        match = None
        idx = None
        for index, agg in enumerate(aggregations):
            if agg['attribute_name'] == attribute_name:
                found = True
                match = agg
                idx = index
                break

        if found:
            if delete:
                del aggregations[idx]
            else:
                if agg_type is not None:
                    validate_type(agg_type)
                    match['type'] = agg_type
                if options is not None:
                    validate_options(options)
                    match['options'] = options
                aggregations[idx] = match

        else:
            if delete:
                message = f'No change. Tried removing/deleting/disabling non-existing aggregation operations for attribute={attribute_name}'
                warn(message)
            else:
                match = {}
                validate_type(agg_type)
                match['type'] = agg_type
                match['attribute_name'] = attribute_name
                if options is not None:
                    validate_options(options)
                    match['options'] = options
                elif options is None:
                    match['options'] = {}
                aggregations.append(match)

        aggregations, aggregation_dict, agg_columns = _construct_agg_info(aggregations)
        kwargs = {
            'aggregations': aggregations,
            'aggregation_dict': aggregation_dict,
            'agg_columns': agg_columns,
        }
        if len(aggregations) == 0:
            warn(
                'Setting `groupby_attrs` to []. Aggregations will be disabled because `groupby_attrs` is empty.'
            )
            kwargs['groupby_attrs'] = []
        self.aggregation_info = self.aggregation_info._replace(**kwargs)
        self._entries = {}
        if len(self.groupby_attrs) == 0:
            self._set_groups_and_keys()

    @property
    def path_column_name(self) -> str:
        """
        The name of the column containing the path to the asset.
        """
        return self._path_column_name

    @path_column_name.setter
    def path_column_name(self, value: str) -> None:
        self._path_column_name = value

    @property
    def data_format(self) -> str:
        """
        The data format. Valid values are netcdf and zarr.
        If specified, it means that all data assets in the catalog use the same data format.
        """
        return self._data_format

    @data_format.setter
    def data_format(self, value: str) -> None:
        self._data_format = value

    @property
    def format_column_name(self) -> str:
        """
        Name of the column which contains the data format.
        """
        return self._format_column_name

    @format_column_name.setter
    def format_column_name(self, value: str) -> None:
        self._format_column_name = value

    def __len__(self):
        return len(self.keys())

    def _get_entries(self):
        # Due to just-in-time entry creation, we may not have all entries loaded
        # We need to make sure to create entries missing from self._entries
        missing = set(self.keys()) - set(self._entries.keys())
        for key in missing:
            _ = self[key]
        return self._entries

    def __getitem__(self, key: str):
        """
        This method takes a key argument and return a data source
        corresponding to assets (files) that will be aggregated into a
        single xarray dataset.

        Parameters
        ----------
        key : str
          key to use for catalog entry lookup

        Returns
        -------
        intake_esm.source.ESMGroupDataSource
             A data source by name (key)

        Raises
        ------
        KeyError
            if key is not found.

        Examples
        --------
        >>> col = intake.open_esm_datastore("mycatalog.json")
        >>> data_source = col["AerChemMIP.BCC.BCC-ESM1.piClim-control.AERmon.gn"]
        """
        # The canonical unique key is the key of a compatible group of assets
        try:
            return self._entries[key]
        except KeyError:
            if key in self.keys():
                internal_key = self._keys[key]
                if isinstance(self._grouped, pd.DataFrame):
                    df = self._grouped.loc[internal_key]
                    args = dict(
                        key=key,
                        row=df,
                        path_column=self.path_column_name,
                        data_format=self.data_format,
                        format_column=self.format_column_name,
                        requested_variables=self._requested_variables,
                    )
                    entry = _make_entry(key, 'esm_single_source', args)
                else:
                    df = self._grouped.get_group(internal_key)
                    args = dict(
                        df=df,
                        aggregation_dict=self.aggregation_info.aggregation_dict,
                        path_column=self.path_column_name,
                        variable_column=self.aggregation_info.variable_column_name,
                        data_format=self.data_format,
                        format_column=self.format_column_name,
                        key=key,
                        requested_variables=self._requested_variables,
                    )
                    entry = _make_entry(key, 'esm_group', args)

                self._entries[key] = entry
                return self._entries[key]
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

    def _ipython_display_(self):
        """
        Display the entry as a rich object in an IPython session
        """
        from IPython.display import HTML, display

        contents = self._repr_html_()
        display(HTML(contents))

    def __dir__(self):
        rv = [
            'df',
            'to_dataset_dict',
            'from_df',
            'keys',
            'serialize',
            'search',
            'unique',
            'nunique',
            'update_aggregation',
            'key_template',
            'groupby_attrs',
            'variable_column_name',
            'aggregations',
            'agg_columns',
            'aggregation_dict',
            'path_column_name',
            'data_format',
            'format_column_name',
        ]
        return sorted(list(self.__dict__.keys()) + rv)

    def _ipython_key_completions_(self):
        return self.__dir__()

    @classmethod
    def from_df(
        cls,
        df: pd.DataFrame,
        esmcol_data: Dict[str, Any] = None,
        progressbar: bool = True,
        sep: str = '.',
        **kwargs,
    ) -> 'esm_datastore':
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

        Returns
        -------
        :py:class:`~intake_esm.core.esm_datastore`
            Catalog object
        """
        return cls(
            df,
            esmcol_data=esmcol_data,
            progressbar=progressbar,
            sep=sep,
            **kwargs,
        )

    def search(self, require_all_on: Union[str, List] = None, **query):
        """Search for entries in the catalog.

        Parameters
        ----------
        require_all_on : list, str, optional
            A dataframe column or a list of dataframe columns across
            which all entries must satisfy the query criteria.
            If None, return entries that fulfill any of the criteria specified
            in the query, by default None.
        **query:
            keyword arguments corresponding to user's query to execute against the dataframe.

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

        >>> cat = col.search(
        ...     source_id=["BCC-CSM2-MR", "CNRM-CM6-1", "CNRM-ESM2-1"],
        ...     experiment_id=["historical", "ssp585"],
        ...     variable_id="pr",
        ...     table_id="Amon",
        ...     grid_label="gn",
        ... )
        >>> cat.df.head(3)
            activity_id institution_id    source_id  ... grid_label                                             zstore dcpp_init_year
        260        CMIP            BCC  BCC-CSM2-MR  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r1i...            NaN
        346        CMIP            BCC  BCC-CSM2-MR  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r2i...            NaN
        401        CMIP            BCC  BCC-CSM2-MR  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r3i...            NaN

        The search method also accepts compiled regular expression objects
        from :py:func:`~re.compile` as patterns.

        >>> import re
        >>> # Let's search for variables containing "Frac" in their name
        >>> pat = re.compile(r"Frac")  # Define a regular expression
        >>> cat.search(variable_id=pat)
        >>> cat.df.head().variable_id
        0     residualFrac
        1    landCoverFrac
        2    landCoverFrac
        3     residualFrac
        4    landCoverFrac
        """

        results = search(self.df, require_all_on=require_all_on, **query)
        if self._multiple_variable_assets:
            requested_variables = query.get(self.variable_column_name, [])
        else:
            requested_variables = []
        ret = esm_datastore.from_df(
            results,
            esmcol_data=self.esmcol_data,
            progressbar=self.progressbar,
            sep=self.sep,
            **self._kwargs,
        )
        ret._requested_variables = requested_variables
        return ret

    def serialize(self, name: str, directory: str = None, catalog_type: str = 'dict') -> None:
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
        >>> col_subset = col.search(
        ...     source_id="BCC-ESM1",
        ...     grid_label="gn",
        ...     table_id="Amon",
        ...     experiment_id="historical",
        ... )
        >>> col_subset.serialize(name="cmip6_bcc_esm1", catalog_type="file")
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

    def nunique(self) -> pd.Series:
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

    def unique(self, columns: Union[str, List] = None) -> Dict[str, Any]:
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
        zarr_kwargs: Dict[str, Any] = None,
        cdf_kwargs: Dict[str, Any] = None,
        preprocess: Dict[str, Any] = None,
        storage_options: Dict[str, Any] = None,
        progressbar: bool = None,
        aggregate: bool = None,
    ) -> Dict[str, xr.Dataset]:
        """
        Load catalog entries into a dictionary of xarray datasets.

        Parameters
        ----------
        zarr_kwargs : dict
            Keyword arguments to pass to :py:func:`~xarray.open_zarr` function
        cdf_kwargs : dict
            Keyword arguments to pass to :py:func:`~xarray.open_dataset` function.  If specifying chunks, the chunking
            is applied to each netcdf file.  Therefore, chunks must refer to dimensions that are present in each netcdf
            file, or chunking will fail.
        preprocess : callable, optional
            If provided, call this function on each dataset prior to aggregation.
        storage_options : dict, optional
            Parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.
        progressbar : bool
            If True, will print a progress bar to standard error (stderr)
            when loading assets into :py:class:`~xarray.Dataset`.
        aggregate : bool, optional
            If False, no aggregation will be done.

        Returns
        -------
        dsets : dict
           A dictionary of xarray :py:class:`~xarray.Dataset`.

        Examples
        --------
        >>> import intake
        >>> col = intake.open_esm_datastore("glade-cmip6.json")
        >>> cat = col.search(
        ...     source_id=["BCC-CSM2-MR", "CNRM-CM6-1", "CNRM-ESM2-1"],
        ...     experiment_id=["historical", "ssp585"],
        ...     variable_id="pr",
        ...     table_id="Amon",
        ...     grid_label="gn",
        ... )
        >>> dsets = cat.to_dataset_dict()
        >>> dsets.keys()
        dict_keys(['CMIP.BCC.BCC-CSM2-MR.historical.Amon.gn', 'ScenarioMIP.BCC.BCC-CSM2-MR.ssp585.Amon.gn'])
        >>> dsets["CMIP.BCC.BCC-CSM2-MR.historical.Amon.gn"]
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

        # Return fast
        if not self.keys():
            warn('There are no datasets to load! Returning an empty dictionary.')
            return {}

        source_kwargs = OrderedDict(
            zarr_kwargs=zarr_kwargs,
            cdf_kwargs=cdf_kwargs,
            preprocess=preprocess,
            storage_options=storage_options,
        )

        if progressbar is not None:
            self.progressbar = progressbar

        if preprocess is not None and not callable(preprocess):
            raise ValueError('preprocess argument must be callable')

        if aggregate is not None and not aggregate:
            self = deepcopy(self)
            self.groupby_attrs = []

        if self.progressbar:
            print(
                f"""\n--> The keys in the returned dictionary of datasets are constructed as follows:\n\t'{self.key_template}'"""
            )

        def _load_source(key, source):
            return key, source.to_dask()

        sources = {key: source(**source_kwargs) for key, source in self.items()}
        progress, total = None, None
        if self.progressbar:
            total = len(sources)
            progress = progress_bar(range(total))

        self._datasets = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=dask.system.CPU_COUNT) as executor:
            future_tasks = [
                executor.submit(_load_source, key, source) for key, source in sources.items()
            ]
            for i, task in enumerate(concurrent.futures.as_completed(future_tasks)):
                key, ds = task.result()
                self._datasets[key] = ds
                if self.progressbar:
                    progress.update(i)
            if self.progressbar:
                progress.update(total)
            return self._datasets


def _make_entry(key: str, driver: str, args: dict):
    entry = intake.catalog.local.LocalCatalogEntry(
        name=key, description='', driver=driver, args=args, metadata={}
    )
    return entry.get()


def _construct_agg_info(aggregations: List[Dict]) -> Tuple[List[Dict], Dict, List]:
    """
    Helper function used to determine aggregation columns information and their
    respective settings.

    Examples
    --------

    >>> a = [
    ...     {"type": "union", "attribute_name": "variable_id"},
    ...     {
    ...         "type": "join_new",
    ...         "attribute_name": "member_id",
    ...         "options": {"coords": "minimal", "compat": "override"},
    ...     },
    ...     {
    ...         "type": "join_new",
    ...         "attribute_name": "dcpp_init_year",
    ...         "options": {"coords": "minimal", "compat": "override"},
    ...     },
    ... ]
    >>> aggregations, aggregation_dict, agg_columns = _construct_agg_info(a)
    >>> agg_columns
    ['variable_id', 'member_id', 'dcpp_init_year']
    >>> aggregation_dict
    {'variable_id': {'type': 'union'},
    'member_id': {'type': 'join_new',
    'options': {'coords': 'minimal', 'compat': 'override'}},
    'dcpp_init_year': {'type': 'join_new',
    'options': {'coords': 'minimal', 'compat': 'override'}}}
    """
    agg_columns = []
    aggregation_dict = {}
    if aggregations:
        # Sort aggregations to make sure join_existing is always done before join_new
        aggregations = sorted(aggregations, key=lambda i: i['type'], reverse=True)
        for agg in aggregations:
            key = agg['attribute_name']
            if agg['type'] == 'join_existing' and 'dim' not in agg['options']:
                message = f"""
            Missing `dim` option for `join_existing` operation across `{key}` attribute.
            For `join_existing` to properly work, `options` must contain the name of the existing dimension
            to use (for e.g.: something like {{'dim': 'time'}}).
                """
                warn(message)
            rest = agg.copy()
            del rest['attribute_name']
            aggregation_dict[key] = rest
        agg_columns = list(aggregation_dict.keys())
    return aggregations, aggregation_dict, agg_columns
