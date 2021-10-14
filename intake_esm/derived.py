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

    def __contains__(self, item: str) -> bool:
        return item in self._registry

    def __getitem__(self, item: str) -> DerivedVariable:
        return self._registry[item]

    def __iter__(self) -> typing.Iterator[DerivedVariable]:
        return iter(self._registry.values())

    def __repr__(self) -> str:
        return f'DerivedVariableRegistry({self._registry})'

    def __len__(self) -> int:
        return len(self._registry)

    def items(self) -> typing.List[typing.Tuple[str, DerivedVariable]]:
        return list(self._registry.items())

    def keys(self) -> typing.List[str]:
        return list(self._registry.keys())

    def search(
        self, variable: typing.Union[str, typing.List[str]]
    ) -> typing.Optional[DerivedVariable]:
        if isinstance(variable, str):
            variable = [variable]

        results = tlz.dicttoolz.keyfilter(lambda x: x in variable, self._registry)
        reg = DerivedVariableRegistry()
        reg._registry = results
        return reg


default_registry = DerivedVariableRegistry()
