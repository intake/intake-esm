import concurrent.futures
import json
import typing
from collections import OrderedDict
from copy import deepcopy
from typing import Any, Dict, List, Tuple, Union
from warnings import warn

import dask
import intake
import pandas as pd
import pydantic
import xarray as xr
from fastprogress.fastprogress import progress_bar
from intake.catalog import Catalog

from ._types import ESMCatalogModel
from .search import search


class esm_datastore(Catalog):
    """
    An intake plugin for parsing an ESM (Earth System Model) Collection/catalog
    and loading assets (netCDF files and/or Zarr stores) into xarray datasets.
    The in-memory representation for the catalog is a Pandas DataFrame.

    Parameters
    ----------
    obj : str, dict
        If string, this must be a path or URL to an ESM collection JSON file.
        If dict, this must be a dict representation of an ESM collection.
        This dict must have two keys: 'esmcat' and 'df'. The 'esmcat' key must be a
        dict representation of the ESM collection and the 'df' key must
        be a Pandas DataFrame containing content that would otherwise be in a CSV file.
    sep : str, optional
        Delimiter to use when constructing a key for a query, by default '.'
    read_csv_kwargs : dict, optional
        Additional keyword arguments passed through to the :py:func:`~pandas.read_csv` function.
    storage_options : dict, optional
            Parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.
    intake_kwargs: dict, optional
            Additional keyword arguments are passed through to the :py:class:`~intake.catalog.Catalog` base class.

    Examples
    --------

    At import time, this plugin is available in intake's registry as `esm_datastore` and
    can be accessed with `intake.open_esm_datastore()`:

    >>> import intake
    >>> url = "https://storage.googleapis.com/cmip6/pangeo-cmip6.json"
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
        obj: typing.Union[pydantic.FilePath, pydantic.AnyUrl, typing.Dict[str, typing.Any]],
        *,
        sep: str = '.',
        read_csv_kwargs: typing.Dict[str, typing.Any] = None,
        storage_options: typing.Dict = None,
        intake_kwargs: typing.Dict = None,
    ):

        """Intake Catalog representing an ESM Collection."""
        intake_kwargs = intake_kwargs or {}
        super(esm_datastore, self).__init__(**intake_kwargs)
        self.storage_options = storage_options or {}
        self.read_csv_kwargs = read_csv_kwargs or {}
        self.sep = sep
        if isinstance(obj, dict):
            self.esmcat = ESMCatalogModel.from_dict(obj)
        else:
            self.esmcat = ESMCatalogModel.load(
                obj, storage_options=self.storage_options, read_csv_kwargs=read_csv_kwargs
            )
        self._entries = {}

    def keys(self) -> List:
        """
        Get keys for the catalog entries

        Returns
        -------
        list
            keys for the catalog entries
        """
        return list(self.esmcat._construct_groups_and_keys(sep=self.sep).keys())

    @property
    def key_template(self) -> str:
        """
        Return string template used to create catalog entry keys

        Returns
        -------
        str
          string template used to create catalog entry keys
        """
        if self.esmcat.aggregation_control.groupby_attrs:
            return self.sep.join(self.esmcat.aggregation_control.groupby_attrs)
        else:
            return self.sep.join(self.esmcat.df.columns)

    @property
    def df(self) -> pd.DataFrame:
        """
        Return pandas :py:class:`~pandas.DataFrame`.
        """
        return self.esmcat.df

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
        return f'<p><strong>{self.esmcol_data["id"]} catalog with {len(self)} dataset(s) from {len(self.df)} asset(s)</strong>:</p> {text}'

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
            """Remove any old references to the catalog."""
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
        return self.esmcat.nunique()

    def unique(self) -> pd.Series:
        """Return unique values for given columns in the
        catalog.
        """

        return self.esmcat.unique()

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
