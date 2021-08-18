import itertools
import typing
from collections import OrderedDict

import intake
import pandas as pd
import pydantic


@pydantic.dataclasses.dataclass
class DerivedVariable:
    """
    Support computation of variables that depend on multiple variables,
    i.e., "derived vars"
    """

    dependent_variables: typing.List[str]
    func: typing.Callable

    def __call__(self, ds, **kwargs):
        """call the function to compute derived var"""
        self._ensure_variables(ds)
        return self.func(ds, **kwargs)

    def _ensure_variables(self, ds):
        """ensure that required variables are present"""
        missing_var = set(self.dependent_variables) - set(ds.variables)
        if missing_var:
            raise ValueError(f'Variables missing: {missing_var}')


@pydantic.dataclasses.dataclass
class DerivedCatalog:
    # knows how to read and load derived variables
    # could just evolve on its own - contract
    # this sends a key and a dataset to .to_dataset_dict()

    intake_catalog_json: str

    def __post_init_post_parse__(self):

        self.intake_catalog = intake.open_esm_datastore(self.intake_catalog_json)
        self.columns = self.intake_catalog.df.columns
        self.derived_variables = pd.DataFrame(columns=self.columns + ['dependent_variables'])
        self.derived_variable_registry = dict()

    def add_variable(
        self,
        variable,
        dependent_variables,
        function,
        core_columns=['component', 'experiment', 'frequency', 'spatial_domain'],
    ):
        intake_catalog = self.intake_catalog.search(variable=dependent_variables)
        unique_ids = intake_catalog.unique(core_columns)

        columns = []
        keys = []
        for key, val in unique_ids.items():
            columns.append(key)
            keys.append(val['values'])

        values = list(itertools.product(*keys))

        dict_list = []
        for val in values:
            variable_dict = dict(zip(columns, val))
            variable_dict['variable'] = variable
            variable_dict['dependent_variables'] = dependent_variables

            dict_list.append(variable_dict)

        # Append the derived_variables dictionary with the list of dictionaries
        self.derived_variables = self.derived_variables.append(dict_list)
        self.derived_variable_registry[variable] = DerivedVariable(dependent_variables, function)

    def to_dataset_dict(
        self,
        variable,
        zarr_kwargs: typing.Dict[str, typing.Any] = None,
        cdf_kwargs: typing.Dict[str, typing.Any] = None,
        preprocess: typing.Dict[str, typing.Any] = None,
        storage_options: typing.Dict[str, typing.Any] = None,
    ):
        """Read in the data and return a dictionary of datasets"""
        source_kwargs = OrderedDict(
            zarr_kwargs=zarr_kwargs,
            cdf_kwargs=cdf_kwargs,
            preprocess=preprocess,
            storage_options=storage_options,
        )
        derived_var = self.derived_variable_registry[variable]
        cat = self.intake_catalog.search(variable=derived_var.dependent_variables)
        keys = cat.keys()
        dsets = {}

        for key in keys:
            df = cat[key]
            dsets[key] = derived_var(df(**source_kwargs).to_dask())

        return dsets
