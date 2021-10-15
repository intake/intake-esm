import typing

import pydantic
import tlz
import xarray as xr

from .utils import INTAKE_ESM_ATTRS_PREFIX


class DerivedVariableError(Exception):
    pass


class DerivedVariable(pydantic.BaseModel):
    func: typing.Callable
    variable: pydantic.StrictStr
    dependent_variables: typing.List[pydantic.StrictStr]

    def __call__(self, *args, **kwargs) -> xr.Dataset:
        try:
            ds = self.func(*args, **kwargs)
            ds[self.variable].attrs[
                f'{INTAKE_ESM_ATTRS_PREFIX}/derivation'
            ] = f'dependent_variables: {self.dependent_variables}'
            return ds
        except Exception as exc:
            raise DerivedVariableError(
                f'Unable to derived variable: {self.variable} with dependent: {self.dependent_variables} using args:{args} and kwargs:{kwargs}'
            ) from exc


@pydantic.dataclasses.dataclass
class DerivedVariableRegistry:
    def __post_init_post_parse__(self):
        self._registry = {}

    @tlz.curry
    def register(
        self, func: typing.Callable, *, variable: str, dependent_variables: typing.List[str]
    ) -> typing.Callable:
        """Register a derived variable
        Parameters
        ----------
        func : typing.Callable
            The function to apply to the dependent variables.
        variable : str
            The name of the variable to derive.
        dependent_variables : typing.List[str]
            The list of dependent variables required to derive `variable`.

        Returns
        -------
        typing.Callable
            The function that was registered.
        """
        self._registry[variable] = DerivedVariable(
            func=func, variable=variable, dependent_variables=dependent_variables
        )
        return func

    def __contains__(self, item: str) -> bool:
        return item in self._registry

    def __getitem__(self, item: str) -> DerivedVariable:
        return self._registry[item]

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self._registry.keys())

    def __repr__(self) -> str:
        return f'DerivedVariableRegistry({self._registry})'

    def __len__(self) -> int:
        return len(self._registry)

    def items(self) -> typing.List[typing.Tuple[str, DerivedVariable]]:
        return list(self._registry.items())

    def keys(self) -> typing.List[str]:
        return list(self._registry.keys())

    def search(self, variable: typing.Union[str, typing.List[str]]) -> 'DerivedVariableRegistry':
        """Search for a derived variable by name or list of names
        Parameters
        ----------
        variable : typing.Union[str, typing.List[str]]
            The name of the variable to search for.

        Returns
        -------
        DerivedVariableRegistry
            A DerivedVariableRegistry with the found variables.
        """
        if isinstance(variable, str):
            variable = [variable]
        results = tlz.dicttoolz.keyfilter(lambda x: x in variable, self._registry)
        reg = DerivedVariableRegistry()
        reg._registry = results
        return reg

    def update_datasets(
        self, datasets: typing.Dict[str, xr.Dataset]
    ) -> typing.Dict[str, xr.Dataset]:
        """Given a dictionary of datasets, return a dictionary of datasets with the derived variables

        Parameters
        ----------
        datasets : typing.Dict[str, xr.Dataset]
            A dictionary of datasets to apply the derived variables to.

        Returns
        -------
        typing.Dict[str, xr.Dataset]
            A dictionary of datasets with the derived variables applied.
        """

        for dset_key, dataset in datasets.items():
            for _, derived_variable in self.items():
                if set(dataset.variables).intersection(derived_variable.dependent_variables):
                    # Assumes all dependent variables are in the same dataset
                    # TODO: Make this more robust to support datasets with variables from different datasets
                    datasets[dset_key] = derived_variable(dataset)
        return datasets


default_registry = DerivedVariableRegistry()
