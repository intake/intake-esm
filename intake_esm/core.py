import concurrent.futures
import typing
import warnings
from copy import deepcopy

import dask
import pandas as pd
import pydantic
import xarray as xr
from fastprogress.fastprogress import progress_bar
from intake.catalog import Catalog

from .cat import ESMCatalogModel
from .derived import DerivedVariableRegistry, default_registry
from .source import ESMDataSource


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
    registry : DerivedVariableRegistry, optional
        Registry of derived variables to use, by default None. If not provided, uses the default registry.
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
        progressbar: bool = True,
        sep: str = '.',
        registry: typing.Optional[DerivedVariableRegistry] = None,
        read_csv_kwargs: typing.Dict[str, typing.Any] = None,
        storage_options: typing.Dict[str, typing.Any] = None,
        intake_kwargs: typing.Dict[str, typing.Any] = None,
    ):

        """Intake Catalog representing an ESM Collection."""
        intake_kwargs = intake_kwargs or {}
        super(esm_datastore, self).__init__(**intake_kwargs)
        self.storage_options = storage_options or {}
        self.read_csv_kwargs = read_csv_kwargs or {}
        self.progressbar = progressbar
        self.sep = sep
        if isinstance(obj, dict):
            self.esmcat = ESMCatalogModel.from_dict(obj)
        else:
            self.esmcat = ESMCatalogModel.load(
                obj, storage_options=self.storage_options, read_csv_kwargs=read_csv_kwargs
            )

        self.derivedcat = registry or default_registry
        self._entries = {}
        self._requested_variables = []
        self.datasets = {}
        self._validate_derivedcat()

    def _validate_derivedcat(self) -> None:
        for key, entry in self.derivedcat.items():
            if self.esmcat.aggregation_control.variable_column_name not in entry.query.keys():
                raise ValueError(
                    f'Variable derivation requires `{self.esmcat.aggregation_control.variable_column_name}` to be specified in query: {entry.query} for derived variable {key}.'
                )

            for col in entry.query:
                if col not in self.esmcat.df.columns:
                    raise ValueError(
                        f'Derived variable {key} depends on unknown column {col} in query: {entry.query}. Valid ESM catalog columns: {self.esmcat.df.columns.tolist()}.'
                    )

    def keys(self) -> typing.List[str]:
        """
        Get keys for the catalog entries

        Returns
        -------
        list
            keys for the catalog entries
        """
        return list(self.esmcat._construct_group_keys(sep=self.sep).keys())

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

    def __len__(self) -> int:
        return len(self.keys())

    def _get_entries(self) -> typing.Dict[str, ESMDataSource]:
        # Due to just-in-time entry creation, we may not have all entries loaded
        # We need to make sure to create entries missing from self._entries
        missing = set(self.keys()) - set(self._entries.keys())
        for key in missing:
            _ = self[key]
        return self._entries

    @pydantic.validate_arguments
    def __getitem__(self, key: str) -> ESMDataSource:
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
        intake_esm.source.ESMDataSource
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
                keys_dict = self.esmcat._construct_group_keys(sep=self.sep)
                grouped = self.esmcat.grouped

                internal_key = keys_dict[key]

                if isinstance(grouped, pd.DataFrame):
                    records = [grouped.loc[internal_key].to_dict()]

                else:
                    records = grouped.get_group(internal_key).to_dict(orient='records')

                # Create a new entry
                entry = ESMDataSource(
                    key=key,
                    records=records,
                    variable_column_name=self.esmcat.aggregation_control.variable_column_name,
                    path_column_name=self.esmcat.assets.column_name,
                    data_format=self.esmcat.assets.format,
                    aggregations=self.esmcat.aggregation_control.aggregations,
                    intake_kwargs={'metadata': {}},
                )
                self._entries[key] = entry
                return self._entries[key]
            raise KeyError(
                f'key={key} not found in catalog. You can access the list of valid keys via the .keys() method.'
            )

    def __contains__(self, key) -> bool:
        # Python falls back to iterating over the entire catalog
        # if this method is not defined. To avoid this, we implement it differently

        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __repr__(self) -> str:
        """Make string representation of object."""
        return f'<{self.esmcat.id or ""} catalog with {len(self)} dataset(s) from {len(self.df)} asset(s)>'

    def _repr_html_(self) -> str:
        """
        Return an html representation for the catalog object.
        Mainly for IPython notebook
        """
        uniques = pd.DataFrame(self.nunique(), columns=['unique'])
        text = uniques._repr_html_()
        return f'<p><strong>{self.esmcat.id or ""} catalog with {len(self)} dataset(s) from {len(self.df)} asset(s)</strong>:</p> {text}'

    def _ipython_display_(self):
        """
        Display the entry as a rich object in an IPython session
        """
        from IPython.display import HTML, display

        contents = self._repr_html_()
        display(HTML(contents))

    def __dir__(self) -> typing.List[str]:
        rv = [
            'df',
            'to_dataset_dict',
            'keys',
            'serialize',
            'datasets',
            'search',
            'unique',
            'nunique',
            'key_template',
        ]
        return sorted(list(self.__dict__.keys()) + rv)

    def _ipython_key_completions_(self):
        return self.__dir__()

    @pydantic.validate_arguments
    def search(
        self, require_all_on: typing.Union[str, typing.List[str]] = None, **query: typing.Any
    ):
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

        # step 1: Search in the base/main catalog
        esmcat_results = self.esmcat.search(require_all_on=require_all_on, query=query)

        # step 2: Search for entries required to derive variables in the derived catalogs
        # This requires a bit of a hack i.e. the user has to specify the variable in the query
        derivedcat_results = []
        variables = query.get(self.esmcat.aggregation_control.variable_column_name, [])
        if variables:
            for key, value in self.derivedcat.items():
                if key in variables:
                    derivedcat_results.append(
                        self.esmcat.search(require_all_on=require_all_on, query=value.query)
                    )

        if derivedcat_results:
            # Merge results from the main and the derived catalogs
            esmcat_results = (
                pd.concat([esmcat_results, *derivedcat_results])
                .drop_duplicates()
                .reset_index(drop=True)
            )

        cat = esm_datastore({'esmcat': self.esmcat.dict(), 'df': esmcat_results})
        if self.esmcat.has_multiple_variable_assets:
            requested_variables = query.get(
                self.esmcat.aggregation_control.variable_column_name, []
            )
        else:
            requested_variables = []
        cat._requested_variables = requested_variables

        # step 3: Subset the derived catalog
        derivat_cat_subset = self.derivedcat.search(variable=variables)
        cat.derivedcat = derivat_cat_subset
        return cat

    @pydantic.validate_arguments
    def serialize(
        self,
        name: pydantic.StrictStr,
        directory: typing.Union[pydantic.DirectoryPath, pydantic.StrictStr] = None,
        catalog_type: str = 'dict',
    ) -> None:
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
        """

        self.esmcat.save(name, directory=directory, catalog_type=catalog_type)

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
        nunique = self.esmcat.nunique()
        nunique[f'derived_{self.esmcat.aggregation_control.variable_column_name}'] = len(
            self.derivedcat.keys()
        )
        return nunique

    def unique(self) -> pd.Series:
        """Return unique values for given columns in the
        catalog.
        """
        unique = self.esmcat.unique()
        unique[f'derived_{self.esmcat.aggregation_control.variable_column_name}'] = list(
            self.derivedcat.keys()
        )
        return unique

    @pydantic.validate_arguments
    def to_dataset_dict(
        self,
        xarray_open_kwargs: typing.Dict[str, typing.Any] = None,
        xarray_combine_by_coords_kwargs: typing.Dict[str, typing.Any] = None,
        preprocess: typing.Callable = None,
        storage_options: typing.Dict[pydantic.StrictStr, typing.Any] = None,
        progressbar: pydantic.StrictBool = None,
        aggregate: pydantic.StrictBool = None,
        skip_on_error: pydantic.StrictBool = False,
        **kwargs,
    ) -> typing.Dict[str, xr.Dataset]:
        """
        Load catalog entries into a dictionary of xarray datasets.

        Parameters
        ----------
        xarray_open_kwargs : dict
            Keyword arguments to pass to :py:func:`~xarray.open_dataset` function
        xarray_combine_by_coords_kwargs: : dict
            Keyword arguments to pass to :py:func:`~xarray.combine_by_coords` function.
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
        skip_on_error : bool, optional
            If True, skip datasets that cannot be loaded and/or variables we are unable to derive.

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
            warnings.warn(
                'There are no datasets to load! Returning an empty dictionary.',
                UserWarning,
                stacklevel=2,
            )
            return {}

        if (
            self.esmcat.aggregation_control.variable_column_name
            in self.esmcat.aggregation_control.groupby_attrs
        ) and len(self.derivedcat) > 0:
            raise NotImplementedError(
                f'The `{self.esmcat.aggregation_control.variable_column_name}` column name is used as a groupby attribute: {self.esmcat.aggregation_control.groupby_attrs}. '
                'This is not yet supported when computing derived variables.'
            )

        xarray_open_kwargs = xarray_open_kwargs or {}
        xarray_combine_by_coords_kwargs = xarray_combine_by_coords_kwargs or {}
        cdf_kwargs, zarr_kwargs = kwargs.get('cdf_kwargs'), kwargs.get('zarr_kwargs')

        if cdf_kwargs or zarr_kwargs:
            warnings.warn(
                'cdf_kwargs and zarr_kwargs are deprecated and will be removed in a future version. '
                'Please use xarray_open_kwargs instead.',
                DeprecationWarning,
                stacklevel=2,
            )
        if cdf_kwargs:
            xarray_open_kwargs.update(cdf_kwargs)
        if zarr_kwargs:
            xarray_open_kwargs.update(zarr_kwargs)

        source_kwargs = dict(
            xarray_open_kwargs=xarray_open_kwargs,
            xarray_combine_by_coords_kwargs=xarray_combine_by_coords_kwargs,
            preprocess=preprocess,
            storage_options=storage_options,
            requested_variables=self._requested_variables,
        )

        if aggregate is not None and not aggregate:
            self = deepcopy(self)
            self.esmcat.aggregation_control.groupby_attrs = []
        if progressbar is not None:
            self.progressbar = progressbar
        if self.progressbar:
            print(
                f"""\n--> The keys in the returned dictionary of datasets are constructed as follows:\n\t'{self.key_template}'"""
            )
        sources = {key: source(**source_kwargs) for key, source in self.items()}
        datasets = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=dask.system.CPU_COUNT) as executor:
            future_tasks = [
                executor.submit(_load_source, key, source) for key, source in sources.items()
            ]
            if self.progressbar:
                gen = progress_bar(
                    concurrent.futures.as_completed(future_tasks), total=len(sources)
                )
            else:
                gen = concurrent.futures.as_completed(future_tasks)
            for task in gen:
                try:
                    key, ds = task.result()
                    datasets[key] = ds
                except Exception as exc:
                    if not skip_on_error:
                        raise exc
        self.datasets = self._create_derived_variables(datasets, skip_on_error)
        return self.datasets

    def _create_derived_variables(self, datasets, skip_on_error):
        if len(self.derivedcat) > 0:
            datasets = self.derivedcat.update_datasets(
                datasets=datasets,
                variable_key_name=self.esmcat.aggregation_control.variable_column_name,
                skip_on_error=skip_on_error,
            )
        return datasets


def _load_source(key, source):
    return key, source.to_dask()
