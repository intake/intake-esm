import typing

import pydantic
import tlz


@pydantic.dataclasses.dataclass
class DerivedVariable:

    func: typing.Callable
    variable: str
    dependent_variables: typing.List[str]


@pydantic.dataclasses.dataclass
class DerivedVariableRegistry:
    def __post_init_post_parse__(self):
        self._registry = {}

    @tlz.curry
    def register(
        self, func: typing.Callable, *, variable: str, dependent_variables: typing.List[str]
    ) -> typing.Callable:
        self._registry[variable] = DerivedVariable(
            func=func, variable=variable, dependent_variables=dependent_variables
        )
        return func

    def keys(self) -> typing.List[str]:
        return self._registry.keys()

    def search(
        self, variable: typing.Union[str, typing.List[str]]
    ) -> typing.Optional[DerivedVariable]:
        if isinstance(variable, str):
            variable = [variable]

        results = tlz.dicttoolz.keyfilter(lambda x: x in variable, self._registry)
        reg = DerivedVariableRegistry()
        reg._registry = results
        return reg


registry = DerivedVariableRegistry()
