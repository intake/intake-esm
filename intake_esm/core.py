import ast
import concurrent.futures
import typing
import warnings
from copy import deepcopy

import dask

try:
    from datatree import DataTree

    _DATATREE_AVAILABLE = True
except ImportError:
    _DATATREE_AVAILABLE = False
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
    An intake plugin for parsing an ESM (Earth System Model) Catalog
    and loading assets (netCDF files and/or Zarr stores) into xarray datasets.
    The in-memory representation for the catalog is a Pandas DataFrame.

    Parameters
    ----------
    obj : str, dict
        If string, this must be a path or URL to an ESM catalog JSON file.
        If dict, this must be a dict representation of an ESM catalog.
        This dict must have two keys: 'esmcat' and 'df'. The 'esmcat' key must be a
        dict representation of the ESM catalog and the 'df' key must
        be a Pandas DataFrame containing content that would otherwise be in a CSV file.
    sep : str, optional
        Delimiter to use when constructing a key for a query, by default '.'
    registry : DerivedVariableRegistry, optional
        Registry of derived variables to use, by default None. If not provided, uses the default registry.
    read_csv_kwargs : dict, optional
        Additional keyword arguments passed through to the :py:func:`~pandas.read_csv` function.
    columns_with_iterables : list of str, optional
        A list of columns in the csv file containing iterables. Values in columns specified here will be
        converted with `ast.literal_eval` when :py:func:`~pandas.read_csv` is called (i.e., this is a
        shortcut to passing converters to `read_csv_kwargs`).
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
    >>> url = 'https://storage.googleapis.com/cmip6/pangeo-cmip6.json'
    >>> cat = intake.open_esm_datastore(url)
    >>> cat.df.head()
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
        obj: pydantic.FilePath | pydantic.AnyUrl | dict[str, typing.Any],
        *,
        progressbar: bool = True,
        sep: str = '.',
        registry: DerivedVariableRegistry | None = None,
        read_csv_kwargs: dict[str, typing.Any] = None,
        columns_with_iterables: list[str] = None,
        storage_options: dict[str, typing.Any] = None,
        **intake_kwargs: dict[str, typing.Any],
    ):
        """Intake Catalog representing an ESM Collection."""
        super().__init__(**intake_kwargs)
        self.storage_options = storage_options or {}
        read_csv_kwargs = read_csv_kwargs or {}
        if columns_with_iterables:
            converter = ast.literal_eval
            read_csv_kwargs.setdefault('converters', {})
            for col in columns_with_iterables:
                if read_csv_kwargs['converters'].setdefault(col, converter) != converter:
                    raise ValueError(
                        f"Cannot provide converter for '{col}' via `read_csv_kwargs` when '{col}' is also specified in `columns_with_iterables`"
                    )
        self.read_csv_kwargs = read_csv_kwargs
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
        if self.esmcat.aggregation_control is None and len(self.derivedcat):
            raise ValueError(
                'Variable derivation requires `aggregation_control` to be specified in the catalog.'
            )
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

    def keys(self) -> list[str]:
        """
        Get keys for the catalog entries

        Returns
        -------
        list
            keys for the catalog entries
        """
        return list(self.esmcat._construct_group_keys(sep=self.sep).keys())

    def keys_info(self) -> pd.DataFrame:
        """
        Get keys for the catalog entries and their metadata

        Returns
        -------
        pandas.DataFrame
            keys for the catalog entries and their metadata

        Examples
        --------

        >>> import intake
        >>> cat = intake.open_esm_datastore('./tests/sample-catalogs/cesm1-lens-netcdf.json')
        >>> cat.keys_info()
                        component experiment stream
        key
        ocn.20C.pop.h         ocn        20C  pop.h
        ocn.CTRL.pop.h        ocn       CTRL  pop.h
        ocn.RCP85.pop.h       ocn      RCP85  pop.h



        """
        results = self.esmcat._construct_group_keys(sep=self.sep)
        if self.esmcat.aggregation_control and self.esmcat.aggregation_control.groupby_attrs:
            groupby_attrs = self.esmcat.aggregation_control.groupby_attrs
        else:
            groupby_attrs = list(self.df.columns)
        data = {key: dict(zip(groupby_attrs, results[key])) for key in results}
        data = pd.DataFrame.from_dict(data, orient='index')
        data.index.name = 'key'
        return data

    @property
    def key_template(self) -> str:
        """
        Return string template used to create catalog entry keys

        Returns
        -------
        str
          string template used to create catalog entry keys
        """
        if self.esmcat.aggregation_control and self.esmcat.aggregation_control.groupby_attrs:
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

    def _get_entries(self) -> dict[str, ESMDataSource]:
        # Due to just-in-time entry creation, we may not have all entries loaded
        # We need to make sure to create entries missing from self._entries
        missing = set(self.keys()) - set(self._entries.keys())
        for key in missing:
            _ = self[key]
        return self._entries

    @pydantic.validate_call
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
        >>> cat = intake.open_esm_datastore('mycatalog.json')
        >>> data_source = cat['AerChemMIP.BCC.BCC-ESM1.piClim-control.AERmon.gn']
        """
        # The canonical unique key is the key of a compatible group of assets
        try:
            return self._entries[key]
        except KeyError as e:
            if key in self.keys():
                keys_dict = self.esmcat._construct_group_keys(sep=self.sep)
                grouped = self.esmcat.grouped

                internal_key = keys_dict[key]

                if isinstance(grouped, pd.DataFrame):
                    records = [grouped.loc[internal_key].to_dict()]

                else:
                    records = grouped.get_group(internal_key).to_dict(orient='records')

                if self.esmcat.aggregation_control:
                    variable_column_name = self.esmcat.aggregation_control.variable_column_name
                    aggregations = self.esmcat.aggregation_control.aggregations
                else:
                    variable_column_name = None
                    aggregations = []
                # Create a new entry
                entry = ESMDataSource(
                    key=key,
                    records=records,
                    variable_column_name=variable_column_name,
                    path_column_name=self.esmcat.assets.column_name,
                    data_format=self.esmcat.assets.format,
                    format_column_name=self.esmcat.assets.format_column_name,
                    aggregations=aggregations,
                    intake_kwargs={'metadata': {}},
                )
                self._entries[key] = entry
                return self._entries[key]
            raise KeyError(
                f'key={key} not found in catalog. You can access the list of valid keys via the .keys() method.'
            ) from e

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

    def __dir__(self) -> list[str]:
        rv = [
            'df',
            'to_dataset_dict',
            'to_datatree',
            'to_dask',
            'keys',
            'keys_info',
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

    @pydantic.validate_call
    def search(
        self,
        require_all_on: str | list[str] | None = None,
        **query: typing.Any,
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
        >>> cat = intake.open_esm_datastore('pangeo-cmip6.json')
        >>> cat.df.head(3)
        activity_id institution_id source_id  ... grid_label                                             zstore dcpp_init_year
        0  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
        1  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN
        2  AerChemMIP            BCC  BCC-ESM1  ...         gn  gs://cmip6/AerChemMIP/BCC/BCC-ESM1/ssp370/r1i1...            NaN

        >>> sub_cat = cat.search(
        ...     source_id=['BCC-CSM2-MR', 'CNRM-CM6-1', 'CNRM-ESM2-1'],
        ...     experiment_id=['historical', 'ssp585'],
        ...     variable_id='pr',
        ...     table_id='Amon',
        ...     grid_label='gn',
        ... )
        >>> sub_cat.df.head(3)
            activity_id institution_id    source_id  ... grid_label                                             zstore dcpp_init_year
        260        CMIP            BCC  BCC-CSM2-MR  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r1i...            NaN
        346        CMIP            BCC  BCC-CSM2-MR  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r2i...            NaN
        401        CMIP            BCC  BCC-CSM2-MR  ...         gn  gs://cmip6/CMIP/BCC/BCC-CSM2-MR/historical/r3i...            NaN

        The search method also accepts compiled regular expression objects
        from :py:func:`~re.compile` as patterns.

        >>> import re
        >>> # Let's search for variables containing "Frac" in their name
        >>> pat = re.compile(r'Frac')  # Define a regular expression
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
        if self.esmcat.aggregation_control:
            variables = query.pop(self.esmcat.aggregation_control.variable_column_name, None)
        else:
            variables = None
        dependents = []
        derived_cat_subset = {}
        if variables:
            if isinstance(variables, str):
                variables = [variables]
            for key, value in self.derivedcat.items():
                if key in variables:
                    res = self.esmcat.search(
                        require_all_on=require_all_on, query={**value.query, **query}
                    )
                    if not res.empty:
                        derivedcat_results.append(res)
                        dependents.extend(
                            value.dependent_variables(
                                self.esmcat.aggregation_control.variable_column_name
                            )
                        )
                        derived_cat_subset[key] = value

        if derivedcat_results:
            # Merge results from the main and the derived catalogs
            esmcat_results = (
                pd.concat([esmcat_results, *derivedcat_results])
                .drop_duplicates()
                .reset_index(drop=True)
            )

        cat = self.__class__({'esmcat': self.esmcat.dict(), 'df': esmcat_results})
        cat.esmcat.catalog_file = None  # Don't save the catalog file
        if self.esmcat.has_multiple_variable_assets:
            requested_variables = list(set(variables or []).union(dependents))
        else:
            requested_variables = []
        cat._requested_variables = requested_variables

        # step 3: Subset the derived catalog,
        # but only if variables were looked up, otherwise transfer the whole catalog.
        if variables is not None:
            cat.derivedcat = DerivedVariableRegistry()
            cat.derivedcat._registry.update(derived_cat_subset)
        else:
            cat.derivedcat = self.derivedcat
        return cat

    @pydantic.validate_call
    def serialize(
        self,
        name: pydantic.StrictStr,
        directory: pydantic.DirectoryPath | pydantic.StrictStr | None = None,
        catalog_type: str = 'dict',
        to_csv_kwargs: dict[typing.Any, typing.Any] | None = None,
        json_dump_kwargs: dict[typing.Any, typing.Any] | None = None,
        storage_options: dict[str, typing.Any] | None = None,
    ) -> None:
        """Serialize catalog to corresponding json and csv files.

        Parameters
        ----------
        name : str
            name to use when creating ESM catalog json file and csv catalog.
        directory : str, PathLike, default None
            The path to the local directory. If None, use the current directory
        catalog_type: str, default 'dict'
            Whether to save the catalog table as a dictionary in the JSON file or as a separate CSV file.
        to_csv_kwargs : dict, optional
            Additional keyword arguments passed through to the :py:meth:`~pandas.DataFrame.to_csv` method.
        json_dump_kwargs : dict, optional
            Additional keyword arguments passed through to the :py:func:`~json.dump` function.
        storage_options: dict
            fsspec parameters passed to the backend file-system such as Google Cloud Storage,
            Amazon Web Service S3.

        Notes
        -----
        Large catalogs can result in large JSON files. To keep the JSON file size manageable, call with
        `catalog_type='file'` to save catalog as a separate CSV file.

        Examples
        --------
        >>> import intake
        >>> cat = intake.open_esm_datastore('pangeo-cmip6.json')
        >>> cat_subset = cat.search(
        ...     source_id='BCC-ESM1',
        ...     grid_label='gn',
        ...     table_id='Amon',
        ...     experiment_id='historical',
        ... )
        >>> cat_subset.serialize(name='cmip6_bcc_esm1', catalog_type='file')
        """

        self.esmcat.save(
            name,
            directory=directory,
            catalog_type=catalog_type,
            to_csv_kwargs=to_csv_kwargs,
            json_dump_kwargs=json_dump_kwargs,
            storage_options=storage_options,
        )

    def nunique(self) -> pd.Series:
        """Count distinct observations across dataframe columns
        in the catalog.

        Examples
        --------
        >>> import intake
        >>> cat = intake.open_esm_datastore('pangeo-cmip6.json')
        >>> cat.nunique()
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
        if self.esmcat.aggregation_control:
            nunique[f'derived_{self.esmcat.aggregation_control.variable_column_name}'] = len(
                self.derivedcat.keys()
            )
        return nunique

    def unique(self) -> pd.Series:
        """Return unique values for given columns in the
        catalog.
        """
        unique = self.esmcat.unique()
        if self.esmcat.aggregation_control:
            unique[f'derived_{self.esmcat.aggregation_control.variable_column_name}'] = list(
                self.derivedcat.keys()
            )
        return unique

    @pydantic.validate_call
    def to_dataset_dict(
        self,
        xarray_open_kwargs: dict[str, typing.Any] | None = None,
        xarray_combine_by_coords_kwargs: dict[str, typing.Any] | None = None,
        preprocess: typing.Callable | None = None,
        storage_options: dict[pydantic.StrictStr, typing.Any] | None = None,
        progressbar: pydantic.StrictBool | None = None,
        aggregate: pydantic.StrictBool | None = None,
        skip_on_error: pydantic.StrictBool = False,
        **kwargs,
    ) -> dict[str, xr.Dataset]:
        """
        Load catalog entries into a dictionary of xarray datasets.

        Column values, dataset keys and requested variables are added as global
        attributes on the returned datasets. The names of these attributes can be
        customized with :py:class:`intake_esm.utils.set_options`.

        Parameters
        ----------
        xarray_open_kwargs : dict
            Keyword arguments to pass to :py:func:`~xarray.open_dataset` function
        xarray_combine_by_coords_kwargs: : dict
            Keyword arguments to pass to :py:func:`~xarray.combine_by_coords` function.
        preprocess : callable, optional
            If provided, call this function on each dataset prior to aggregation.
        storage_options : dict, optional
            fsspec Parameters passed to the backend file-system such as Google Cloud Storage,
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
        >>> cat = intake.open_esm_datastore('glade-cmip6.json')
        >>> sub_cat = cat.search(
        ...     source_id=['BCC-CSM2-MR', 'CNRM-CM6-1', 'CNRM-ESM2-1'],
        ...     experiment_id=['historical', 'ssp585'],
        ...     variable_id='pr',
        ...     table_id='Amon',
        ...     grid_label='gn',
        ... )
        >>> dsets = sub_cat.to_dataset_dict()
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

        # Return fast
        if not self.keys():
            warnings.warn(
                'There are no datasets to load! Returning an empty dictionary.',
                UserWarning,
                stacklevel=2,
            )
            return {}

        if (
            self.esmcat.aggregation_control
            and (
                self.esmcat.aggregation_control.variable_column_name
                in self.esmcat.aggregation_control.groupby_attrs
            )
            and len(self.derivedcat) > 0
        ):
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
            requested_variables=self._requested_variables,
            storage_options=storage_options,
        )

        if aggregate is not None and not aggregate and self.esmcat.aggregation_control:
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

    @pydantic.validate_call
    def to_datatree(
        self,
        xarray_open_kwargs: dict[str, typing.Any] | None = None,
        xarray_combine_by_coords_kwargs: dict[str, typing.Any] | None = None,
        preprocess: typing.Callable | None = None,
        storage_options: dict[pydantic.StrictStr, typing.Any] | None = None,
        progressbar: pydantic.StrictBool | None = None,
        aggregate: pydantic.StrictBool | None = None,
        skip_on_error: pydantic.StrictBool = False,
        levels: list[str] = None,
        **kwargs,
    ):
        """
        Load catalog entries into a tree of xarray datasets.

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
        levels : list[str], optional
            List of fields to use as the datatree nodes. WARNING: This will overwrite the fields
            used to create the unique aggregation keys.

        Returns
        -------
        dsets : :py:class:`~datatree.DataTree`
           A tree of xarray :py:class:`~xarray.Dataset`.

        Examples
        --------
        >>> import intake
        >>> cat = intake.open_esm_datastore('glade-cmip6.json')
        >>> sub_cat = cat.search(
        ...     source_id=['BCC-CSM2-MR', 'CNRM-CM6-1', 'CNRM-ESM2-1'],
        ...     experiment_id=['historical', 'ssp585'],
        ...     variable_id='pr',
        ...     table_id='Amon',
        ...     grid_label='gn',
        ... )
        >>> dsets = sub_cat.to_datatree()
        >>> dsets['CMIP/BCC.BCC-CSM2-MR/historical/Amon/gn'].ds
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

        if not _DATATREE_AVAILABLE:
            raise ImportError(
                '.to_datatree() requires the xarray-datatree package to be installed. '
                'To proceed please install xarray-datatree using: '
                ' `python -m pip install xarray-datatree` or `conda install -c conda-forge xarray-datatree`.'
            )

        # Change the groupby controls if neccessary, used to assemble the tree
        if levels is not None:
            self = deepcopy(self)
            self.esmcat.aggregation_control.groupby_attrs = levels

        # Set the separator to a / for datatree temporarily
        self.sep, old_sep = '/', self.sep

        # Use to dataset dict to access dictionary of datasets
        self.datasets = self.to_dataset_dict(
            xarray_open_kwargs=xarray_open_kwargs,
            xarray_combine_by_coords_kwargs=xarray_combine_by_coords_kwargs,
            preprocess=preprocess,
            storage_options=storage_options,
            progressbar=progressbar,
            aggregate=aggregate,
            skip_on_error=skip_on_error,
            **kwargs,
        )

        # Set the separator to the original value
        self.sep = old_sep

        # Convert the dictionary of datasets to a datatree
        self.datasets = DataTree.from_dict(self.datasets)
        return self.datasets

    def to_dask(self, **kwargs) -> xr.Dataset:
        """
        Convert result to an xarray dataset.

        This is only possible if the search returned exactly one result.

        Parameters
        ----------
        kwargs: dict
          Parameters forwarded to :py:func:`~intake_esm.esm_datastore.to_dataset_dict`.

        Returns
        -------
        :py:class:`~xarray.Dataset`
        """
        if len(self) != 1:  # quick check to fail more quickly if there are many results
            raise ValueError(
                f'Expected exactly one dataset. Received {len(self)} datasets. Please refine your search or use `.to_dataset_dict()`.'
            )
        res = self.to_dataset_dict(**{**kwargs, 'progressbar': False})
        if len(res) != 1:  # extra check in case kwargs did modify something
            raise ValueError(
                f'Expected exactly one dataset. Received {len(self)} datasets. Please refine your search or use `.to_dataset_dict()`.'
            )
        _, ds = res.popitem()
        return ds

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
