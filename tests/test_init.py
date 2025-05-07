from unittest import mock

import pytest


def test__all__(cleanup_init):
    import intake_esm

    assert intake_esm.__all__ == [
        'esm_datastore',
        'DerivedVariableRegistry',
        'default_registry',
        'set_options',
        'show_versions',
        'tutorial',
        '__version__',
        '_ESMVALCORE_AVAILABLE',
    ]


def test__to_optional_import_flag(cleanup_init):
    from intake_esm import _to_opt_import_flag

    assert _to_opt_import_flag('esmvalcore') == '_ESMVALCORE_AVAILABLE'
    # This looks stupid but we need to be careful re. underscores
    assert _to_opt_import_flag('intake_esm') == '_INTAKE_ESM_AVAILABLE'


def test__from_optional_import_flag(cleanup_init):
    from intake_esm import _from_opt_import_flag

    assert _from_opt_import_flag('_ESMVALCORE_AVAILABLE') == 'esmvalcore'
    # This looks stupid but we need to be careful re. underscores
    assert _from_opt_import_flag('_INTAKE_ESM_AVAILABLE') == 'intake_esm'


@pytest.mark.parametrize(
    'str',
    [
        '_ESMVALCORE_AVAILABLE',
        '_INTAKE_ESM_AVAILABLE',
    ],
)
def test__opt_import_flags(cleanup_init, str):
    from intake_esm import _from_opt_import_flag, _to_opt_import_flag

    assert _to_opt_import_flag(_from_opt_import_flag(str)) == str


@pytest.mark.parametrize(
    'str',
    [
        '_INVALID_FLAG',
        'invalid_flag',
        'INVALID_FLAG_AVAILABLE',
    ],
)
def test__opt_import_flags_invalid(cleanup_init, str):
    from intake_esm import _from_opt_import_flag

    with pytest.raises(ValueError):
        _from_opt_import_flag(str)


@pytest.mark.parametrize(
    'str',
    [
        'esmvalcore',
        'intake_esm',
    ],
)
def test_rev_opt_import_flags(cleanup_init, str):
    from intake_esm import _from_opt_import_flag, _to_opt_import_flag

    assert _from_opt_import_flag(_to_opt_import_flag(str)) == str


def test_getattr_random_attr_fail(cleanup_init):
    import intake_esm

    with pytest.raises(AttributeError, match="Module 'intake_esm' has no attribute 'random_attr'"):
        _ = intake_esm.random_attr


@mock.patch('importlib.util.find_spec', return_value=False)
def test_getattr_optional_import(mock_fnd_spec, cleanup_init):
    import intake_esm

    assert intake_esm._optional_imports == {'esmvalcore': None}

    assert intake_esm._ESMVALCORE_AVAILABLE is False
    assert intake_esm._optional_imports == {'esmvalcore': False}


@mock.patch('importlib.util.find_spec', return_value=True)
def test_getattr_caching(mock_find_spec, cleanup_init):
    import intake_esm

    # Simulate the first call to find_spec
    mock_find_spec.return_value = True
    assert intake_esm._ESMVALCORE_AVAILABLE is True
    assert intake_esm._ESMVALCORE_AVAILABLE is True

    mock_find_spec.assert_called_once_with('esmvalcore')
