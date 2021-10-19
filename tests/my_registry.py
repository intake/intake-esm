import intake_esm

registry = intake_esm.DerivedVariableRegistry()


@registry.register(variable='FOO', dependent_variables=['FLUT'])
def func(ds):
    return ds
