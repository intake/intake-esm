import intake_esm

registry = intake_esm.DerivedVariableRegistry()


@registry.register(variable='FOO', query={'variable': ['FLUT']})
def func(ds):
    return ds
