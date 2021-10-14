from intake_esm.derived import DerivedVariable, DerivedVariableRegistry


def test_registry_init():
    """
    Test the DerivedVariableRegistry class.
    """
    dvr = DerivedVariableRegistry()

    assert dvr._registry == {}
    assert len(dvr.keys()) == 0


def test_registry_register():
    dvr = DerivedVariableRegistry()

    @dvr.register(variable='FOO', dependent_variables=['BAR'])
    def func(ds):
        return ds.x + ds.y

    assert dvr.keys() == ['FOO']
    assert dvr['FOO'].func == func
    assert 'FOO' in dvr
    assert isinstance(dvr['FOO'], DerivedVariable)
    assert len(dvr) == 1
