import importlib
import inspect
import typing

import pydantic
import tlz
import xarray as xr


class DerivedVariableError(Exception):
    pass


class DerivedVariable(pydantic.BaseModel):
    func: typing.Callable
    variable: pydantic.StrictStr
    query: typing.Dict[pydantic.StrictStr, typing.Union[typing.Any, typing.List[typing.Any]]]

    @pydantic.validator('query')
    def validate_query(cls, values):
        _query = values.copy()
        for key, value in _query.items():
            if isinstance(value, (str, int, float, bool)):
                _query[key] = [value]
        return _query

    def dependent_variables(self, variable_key_name: str) -> typing.List[pydantic.StrictStr]:
        """Return a list of dependent variables for a given variable"""
        return self.query[variable_key_name]

    def __call__(self, *args, variable_key_name: str = None, **kwargs) -> xr.Dataset:
        """Call the function and return the result"""
        try:
            return self.func(*args, **kwargs)
        except Exception as exc:
            dependent_variables = (
                self.dependent_variables(variable_key_name) if variable_key_name else []
            )
            raise DerivedVariableError(
                f'Unable to derived variable: {self.variable} with dependent: {dependent_variables} using args:{args} and kwargs:{kwargs}'
            ) from exc


@pydantic.dataclasses.dataclass
class DerivedVariableRegistry:
    """Registry of derived variables"""

    def __post_init_post_parse__(self):
        self._registry = {}

    @classmethod
    def load(cls, name: str, package: str = None) -> 'DerivedVariableRegistry':
        """Load a DerivedVariableRegistry from a Python module/file

        Parameters
        ----------
        name : str
            The name of the module to load the DerivedVariableRegistry from.
        package : str, optional
            The package to load the module from. This argument is
            required when performing a relative import. It specifies the package
            to use as the anchor point from which to resolve the relative import
            to an absolute import.

        Returns
        -------
        DerivedVariableRegistry
            A DerivedVariableRegistry loaded from the Python module.

        Notes
        -----
        If you have a folder: /home/foo/pythonfiles, and you want to load a registry
        defined in registry.py, located in that directory, ensure to add your folder to the
        $PYTHONPATH before calling this function.

        >>> import sys
        >>> sys.path.insert(0, "/home/foo/pythonfiles")
        >>> from intake_esm.derived import DerivedVariableRegistry
        >>> registsry = DerivedVariableRegistry.load("registry")
        """
        modname = importlib.import_module(name, package=package)
        candidates = inspect.getmembers(modname, lambda x: isinstance(x, DerivedVariableRegistry))
        if candidates:
            return candidates[0][1]
        else:
            raise ValueError(f'No DerivedVariableRegistry found in {name} module')

    @tlz.curry
    def register(
        self,
        func: typing.Callable,
        *,
        variable: str,
        query: typing.Dict[pydantic.StrictStr, typing.Union[typing.Any, typing.List[typing.Any]]],
    ) -> typing.Callable:
        """Register a derived variable
        Parameters
        ----------
        func : typing.Callable
            The function to apply to the dependent variables.
        variable : str
            The name of the variable to derive.
        query : typing.Dict[str, typing.Union[typing.Any, typing.List[typing.Any]]]
            The query to use to retrieve dependent variables required to derive `variable`.

        Returns
        -------
        typing.Callable
            The function that was registered.
        """
        self._registry[variable] = DerivedVariable(func=func, variable=variable, query=query)
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

    def values(self) -> typing.List[DerivedVariable]:
        return list(self._registry.values())

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
        self,
        *,
        datasets: typing.Dict[str, xr.Dataset],
        variable_key_name: str,
        skip_on_error: bool = False,
    ) -> typing.Dict[str, xr.Dataset]:
        """Given a dictionary of datasets, return a dictionary of datasets with the derived variables

        Parameters
        ----------
        datasets : typing.Dict[str, xr.Dataset]
            A dictionary of datasets to apply the derived variables to.
        variable_key_name : str
            The name of the variable key used in the derived variable query
        skip_on_error : bool, optional
            If True, skip variables that fail variable derivation.

        Returns
        -------
        typing.Dict[str, xr.Dataset]
            A dictionary of datasets with the derived variables applied.
        """

        for dset_key, dataset in datasets.items():
            for _, derived_variable in self.items():
                if set(dataset.variables).intersection(
                    derived_variable.dependent_variables(variable_key_name)
                ):
                    try:
                        # Assumes all dependent variables are in the same dataset
                        # TODO: Make this more robust to support datasets with variables from different datasets
                        datasets[dset_key] = derived_variable(
                            dataset, variable_key_name=variable_key_name
                        )
                    except Exception as exc:
                        if not skip_on_error:
                            raise exc
        return datasets


default_registry = DerivedVariableRegistry()
