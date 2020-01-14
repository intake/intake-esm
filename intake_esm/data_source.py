import intake_xarray


class DataSource(intake_xarray.base.DataSourceMixin):
    def _get_schema(self):
        from intake.source.base import Schema

        # set _schema to None to remove any previously cached dataset
        self._schema = None
        self._open_dataset()
        self._schema = Schema(
            datashape=None, dtype=None, shape=None, npartitions=None, extra_metadata={}
        )
        return self._schema
