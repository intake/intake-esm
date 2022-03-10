import intake
import pytest
import xarray as xr

from intake_esm import tutorial


@pytest.mark.network
class TestLoadCatalog:
    @pytest.fixture(autouse=True)
    def setUp(self):
        self.testfile = 'tiny'

    @pytest.fixture
    def simulate_importerror(self, monkeypatch):
        monkeypatch.setitem(sys.modules, 'pooch', None)

    def test_download_from_github(self, tmp_path) -> None:
        cache_dir = tmp_path / tutorial._default_cache_dir_name
        ds = tutorial.open_catalog(self.testfile, cache_dir=cache_dir).load()
        tiny = DataArray(range(5), name='tiny').to_dataset().to_cat()
        xr.testing.assert_identical(ds, tiny)

    def test_download_from_github_load_without_cache(self, tmp_path, monkeypatch) -> None:
        cache_dir = tmp_path / tutorial._default_cache_dir_name

        ds_nocache = tutorial.open_dataset(self.testfile, cache=False, cache_dir=cache_dir).load()
        ds_cache = tutorial.open_dataset(self.testfile, cache_dir=cache_dir).load()
        xr.testing.assert_identical(ds_cache, ds_nocache)

    def test_pooch_import_error(self, simulate_importerror):
        with pytest.raises(ImportError):
            tutorial.open_catalog(self.testfile)

