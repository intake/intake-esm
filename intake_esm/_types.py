import enum
import typing

import fsspec
import pydantic


class AggregationType(str, enum.Enum):
    join_new = 'join_new'
    join_existing = 'join_existing'
    union = 'union'

    class Config:
        validate_all = True
        validate_assignment = True


class DataFormat(str, enum.Enum):
    netcdf = 'netcdf'
    zarr = 'zarr'

    class Config:
        validate_all = True
        validate_assignment = True


class Attribute(pydantic.BaseModel):
    column_name: pydantic.StrictStr
    vocabulary: pydantic.StrictStr = ''

    class Config:
        validate_all = True
        validate_assignment = True


class Assets(pydantic.BaseModel):
    column_name: pydantic.StrictStr
    format: DataFormat
    format_column_name: typing.Optional[pydantic.StrictStr]

    class Config:
        validate_all = True
        validate_assignment = True

    @pydantic.root_validator
    def _validate_data_format(cls, values):
        data_format, format_column_name = values.get('format'), values.get('format_column_name')
        if data_format is not None and format_column_name is not None:
            raise ValueError('Cannot set both format and format_column_name')
        return values


class Aggregation(pydantic.BaseModel):
    type: AggregationType
    attribute_name: pydantic.StrictStr
    options: typing.Optional[typing.Dict] = {}

    class Config:
        validate_all = True
        validate_assignment = True


class AggregationControl(pydantic.BaseModel):
    variable_column_name: pydantic.StrictStr
    groupby_attrs: typing.List[pydantic.StrictStr]
    aggregations: typing.List[Aggregation] = []

    class Config:
        validate_all = True
        validate_assignment = True


class ESMCatalogModel(pydantic.BaseModel):
    """
    Pydantic model for the ESM data catalog defined in https://git.io/JBWoW
    """

    esmcat_version: pydantic.StrictStr
    id: str
    attributes: typing.List[Attribute]
    assets: Assets
    aggregation_control: AggregationControl
    catalog_dict: typing.Optional[typing.List[typing.Dict]] = None
    catalog_file: pydantic.StrictStr = None
    description: pydantic.StrictStr = None
    title: pydantic.StrictStr = None

    class Config:
        validate_all = True
        validate_assignment = True

    @pydantic.root_validator
    def validate_catalog(cls, values):
        catalog_dict, catalog_file = values.get('catalog_dict'), values.get('catalog_file')
        if catalog_dict is not None and catalog_file is not None:
            raise ValueError('catalog_dict and catalog_file cannot be set at the same time')

        return values

    @classmethod
    def load_catalog_file(
        cls,
        catalog_file: typing.Union[str, pydantic.FilePath, pydantic.AnyUrl],
        storage_options=None,
    ) -> 'ESMCatalogModel':
        """
        Loads the catalog from a file
        """
        storage_options = storage_options if storage_options is not None else {}

        with fsspec.open(catalog_file, **storage_options) as fobj:
            return cls.parse_raw(fobj.read())
