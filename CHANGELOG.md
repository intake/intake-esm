# Changelog

## Intake-esm v2020.12.18

([full changelog](https://github.com/intake/intake-esm/compare/4f63319294fc7d8394a7c89680ca3525ca1b0d54...dd3e7fdbd752a9e26030ccc7c03e571adb3d3be1))

### Bug Fixes

- 🐛 Disable `_requested_variables` for single variable assets [#306](https://github.com/intake/intake-esm/pull/306) ([@andersy005](https://github.com/andersy005))

### Internal Changes

- Update changelog in preparation for new release [#307](https://github.com/intake/intake-esm/pull/307) ([@andersy005](https://github.com/andersy005))
- Use `github-activity` to update list of contributors [#302](https://github.com/intake/intake-esm/pull/302) ([@andersy005](https://github.com/andersy005))
- Add nbqa & Update prettier commit hooks [#300](https://github.com/intake/intake-esm/pull/300) ([@andersy005](https://github.com/andersy005))
- Update pre-commit and GH actions [#299](https://github.com/intake/intake-esm/pull/299) ([@andersy005](https://github.com/andersy005))

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2020-11-05&to=2020-12-19&type=c))

[@andersy005](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Aandersy005+updated%3A2020-11-05..2020-12-19&type=Issues) | [@dcherian](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Adcherian+updated%3A2020-11-05..2020-12-19&type=Issues) | [@jbusecke](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajbusecke+updated%3A2020-11-05..2020-12-19&type=Issues) | [@naomi-henderson](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Anaomi-henderson+updated%3A2020-11-05..2020-12-19&type=Issues) | [@Recalculate](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3ARecalculate+updated%3A2020-11-05..2020-12-19&type=Issues)

## Intake-esm v2020.11.4

### Features

- ✨ Support multiple variable assets/files. ({pr}`287`) [@andersy005](https://github.com/andersy005)
- ✨ Add utility function for printing version information. ({pr}`284`) [@andersy005](https://github.com/andersy005)

### Breaking Changes

- 💥 Remove unnecessary logging bits. ({pr}`297`) [@andersy005](https://github.com/andersy005)

### Bug Fixes

- ✔️ Fix test failures. ({pr}`280`) [@andersy005](https://github.com/andersy005)
- Fix TypeError bug in `.search()` method when using wildcard and regular expressions. ({pr}`285`) [@andersy005](https://github.com/andersy005)
- Use file like object when dealing with netcdf in the cloud. ({pr}`292`) [@andersy005](https://github.com/andersy005)

### Documentation

- 📚 Fix ReadtheDocs documentation builds. ({pr}`286`) [@andersy005](https://github.com/andersy005)
- 📚 Migrate docs from restructured text to markdown via `myst-parsers`. ({pr}`296`) [@andersy005](https://github.com/andersy005)
- 🔨 Refactor documentation contents & add new notebooks. ({pr}`298`) [@andersy005](https://github.com/andersy005)

### Internal Changes

- Fix import errors due to [intake/intake#526](https://github.com/intake/intake/pull/526). ({pr}`282`) [@andersy005](https://github.com/andersy005)
- Migrate CI from CircleCI to GitHub Actions. ({pr}`283`) [@andersy005](https://github.com/andersy005)
- Use mamba to speed up CI testing. ({pr}`293`) [@andersy005](https://github.com/andersy005)
- Enable dependabot updates. ({pr}`294`) [@andersy005](https://github.com/andersy005)
- Test against Python 3.9. ({pr}`295`) [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2020-08-15&to=2020-11-04&type=c))

[@andersy005](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Aandersy005+updated%3A2020-08-15..2020-11-04&type=Issues) | [@dcherian](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Adcherian+updated%3A2020-08-15..2020-11-04&type=Issues) | [@jbusecke](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajbusecke+updated%3A2020-08-15..2020-11-04&type=Issues) | [@jukent](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajukent+updated%3A2020-08-15..2020-11-04&type=Issues) | [@sherimickelson](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Asherimickelson+updated%3A2020-08-15..2020-11-04&type=Issues)

## Intake-esm v2020.8.15

### Features

- Support regular expression objects in {py:meth}`~intake_esm.core.esm_datastore.search`
  ({pr}`236`) [@andersy005](https://github.com/andersy005)
- Support wildcard expresssions in {py:meth}`~intake_esm.core.esm_datastore.search`
  ({pr}`259`) [@andersy005](https://github.com/andersy005)
- Expose attributes used when aggregating/combining datasets ({pr}`268`) [@andersy005](https://github.com/andersy005)
- Support turning aggregations off ({pr}`269`) [@andersy005](https://github.com/andersy005)
- Improve error messages ({pr}`270`) [@andersy005](https://github.com/andersy005)
- Expose aggregations options passed to xarray during datasets aggregation
  ({pr}`272`) [@andersy005](https://github.com/andersy005)
- Reset `_entries` dict after updating aggregations ({pr}`274`) [@andersy005](https://github.com/andersy005)

### Documentation

- Update {py:meth}`~intake_esm.core.esm_datastore.to_dataset_dict` docstring
  to inform users on how `cdf_kwargs` argument is used in regards to chunking
  ({pr}`278`) [@bonnland](https://github.com/bonnland)

### Internal Changes

- Update pre-commit hooks & GitHub actions ({pr}`260`) [@andersy005](https://github.com/andersy005)
- Update badges ({pr}`258`) [@andersy005](https://github.com/andersy005)
- Update upstream environment ({pr}`263`) [@andersy005](https://github.com/andersy005)
- Refactor search functionality into a standalone module ({pr}`267`) [@andersy005](https://github.com/andersy005)
- Fix dask/concurrent.futures parallelism ({pr}`271`) [@andersy005](https://github.com/andersy005)
- Increase test coverage to ~100% ({pr}`273`) [@andersy005](https://github.com/andersy005)
- Bump minimum required versions ({pr}`275`) [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2020-06-11&to=2020-08-15&type=c))

[@andersy005](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Aandersy005+updated%3A2020-06-11..2020-08-15&type=Issues) | [@bonnland](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Abonnland+updated%3A2020-06-11..2020-08-15&type=Issues) | [@dcherian](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Adcherian+updated%3A2020-06-11..2020-08-15&type=Issues) | [@jeffdlb](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajeffdlb+updated%3A2020-06-11..2020-08-15&type=Issues) | [@jukent](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajukent+updated%3A2020-06-11..2020-08-15&type=Issues) | [@kmpaul](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Akmpaul+updated%3A2020-06-11..2020-08-15&type=Issues) | [@markusritschel](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Amarkusritschel+updated%3A2020-06-11..2020-08-15&type=Issues) | [@martindurant](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Amartindurant+updated%3A2020-06-11..2020-08-15&type=Issues) | [@matt-long](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Amatt-long+updated%3A2020-06-11..2020-08-15&type=Issues)

## Intake-esm v2020.6.11

### Features

- Add `df` property setter ({pr}`247`) [@andersy005](https://github.com/andersy005)

### Documentation

- Use Pandas sphinx theme ({pr}`244`) [@andersy005](https://github.com/andersy005)
- Update documentation tutorial ({pr}`252`) [@andersy005](https://github.com/andersy005) & [@charlesbluca](https://github.com/charlesbluca)

### Internal Changes

- Fix anti-patterns and other bug risks ({pr}`251`) [@andersy005](https://github.com/andersy005)
- Sync with intake's Entry unification ({pr}`249`) [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2020-05-21&to=2020-06-11&type=c))

[@andersy005](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Aandersy005+updated%3A2020-05-21..2020-06-11&type=Issues) | [@jhamman](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajhamman+updated%3A2020-05-21..2020-06-11&type=Issues) | [@martindurant](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Amartindurant+updated%3A2020-05-21..2020-06-11&type=Issues)

## Intake-esm v2020.5.21

### Features

- Provide informative message/warnings from empty queries. ({pr}`235`) [@andersy005](https://github.com/andersy005)
- Replace tqdm progressbar with fastprogress. ({pr}`238`) [@andersy005](https://github.com/andersy005)
- Add `catalog_file` attribute to `esm_datastore` class. ({pr}`240`) [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2020-05-01&to=2020-05-21&type=c))

[@andersy005](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Aandersy005+updated%3A2020-05-01..2020-05-21&type=Issues) | [@bonnland](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Abonnland+updated%3A2020-05-01..2020-05-21&type=Issues) | [@dcherian](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Adcherian+updated%3A2020-05-01..2020-05-21&type=Issues) | [@jbusecke](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajbusecke+updated%3A2020-05-01..2020-05-21&type=Issues) | [@jeffdlb](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajeffdlb+updated%3A2020-05-01..2020-05-21&type=Issues) | [@kmpaul](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Akmpaul+updated%3A2020-05-01..2020-05-21&type=Issues) | [@markusritschel](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Amarkusritschel+updated%3A2020-05-01..2020-05-21&type=Issues)

## Intake-esm v2020.5.01

### Features

- Add html representation for the catalog object. ({pr}`229`) [@andersy005](https://github.com/andersy005)

- Move logic for assets aggregation into {py:meth}`~intake_esm.source.ESMGroupDataSource`
  and add few basic dict-like methods (`keys()`, `len()`, `getitem()`, `contains()`)
  to the catalog object. ({pr}`194`) [@andersy005](https://github.com/andersy005) & [@jhamman](https://github.com/jhamman) & [@kmpaul](https://github.com/kmpaul)

- Support columns with iterables in {py:meth}`~intake_esm.core.esm_datastore.unique` and
  {py:meth}`~intake_esm.core.esm_datastore.nunique`. ({pr}`223`) [@andersy005](https://github.com/andersy005)

### Bug Fixes

- Revert back to using `concurrent.futures` to address failures due
  to dask's distributed scheduler. ({issue}`225`) & ({issue}`226`)

### Internal Changes

- Increase test coverage. ({pr}`222`) [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2020-03-16&to=2020-05-01&type=c))

[@andersy005](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Aandersy005+updated%3A2020-03-16..2020-05-01&type=Issues) | [@bonnland](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Abonnland+updated%3A2020-03-16..2020-05-01&type=Issues) | [@dcherian](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Adcherian+updated%3A2020-03-16..2020-05-01&type=Issues) | [@jbusecke](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajbusecke+updated%3A2020-03-16..2020-05-01&type=Issues) | [@jhamman](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajhamman+updated%3A2020-03-16..2020-05-01&type=Issues) | [@kmpaul](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Akmpaul+updated%3A2020-03-16..2020-05-01&type=Issues) | [@sherimickelson](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Asherimickelson+updated%3A2020-03-16..2020-05-01&type=Issues)

## Intake-esm v2020.3.16

### Features

- Support single file catalogs. ({pr}`195`) [@bonnland](https://github.com/bonnland)

- Add `progressbar` argument to {py:meth}`~intake_esm.core.esm_datastore.to_dataset_dict`.
  This allows the user to override the default `progressbar` value used
  during the class instantiation. ({pr}`204`) [@andersy005](https://github.com/andersy005)

- Enhanced search: enforce query criteria via `require_all_on` argument via
  {py:meth}`~intake_esm.core.esm_datastore.search` method.
  ({issue}`202`) & ({pr}`207`) & ({pr}`209`) [@andersy005](https://github.com/andersy005) & [@jbusecke](https://github.com/jbusecke)

- Support relative paths for catalog files. ({pr}`208`) [@andersy005](https://github.com/andersy005)

### Bug Fixes

- Use raw path if protocol is `None`. ({pr}`210`) [@andersy005](https://github.com/andersy005)

### Internal Changes

- Github Action to publish package to PyPI on release.
  ({pr}`190`) [@andersy005](https://github.com/andersy005)

- Remove unnecessary inheritance. ({pr}`193`) [@andersy005](https://github.com/andersy005)

- Update linting GitHub action to run on all pull requests.
  ({pr}`196`) [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2019-12-13&to=2020-03-16&type=c))

[@andersy005](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Aandersy005+updated%3A2019-12-13..2020-03-16&type=Issues) | [@bonnland](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Abonnland+updated%3A2019-12-13..2020-03-16&type=Issues) | [@dcherian](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Adcherian+updated%3A2019-12-13..2020-03-16&type=Issues) | [@jbusecke](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajbusecke+updated%3A2019-12-13..2020-03-16&type=Issues) | [@jhamman](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajhamman+updated%3A2019-12-13..2020-03-16&type=Issues) | [@kmpaul](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Akmpaul+updated%3A2019-12-13..2020-03-16&type=Issues)

## Intake-esm v2019.12.13

### Features

- Add optional `preprocess` argument to {py:meth}`~intake_esm.core.esm_datastore.to_dataset_dict`
  ({pr}`155`) [@matt-long](https://github.com/matt-long)
- Allow users to disable dataset aggregations by passing `aggregate=False`
  to {py:meth}`~intake_esm.core.esm_datastore.to_dataset_dict` ({pr}`164`) [@matt-long](https://github.com/matt-long)
- Avoid manipulating dataset coordinates by using `data_vars=varname`
  when concatenating datasets via xarray {py:func}:`~xarray.concat()`
  ({pr}`174`) [@andersy005](https://github.com/andersy005)
- Support loading netCDF assets from openDAP endpoints
  ({pr}`176`) [@andersy005](https://github.com/andersy005)
- Add {py:meth}`~intake_esm.core.esm_datastore.serialize` method to serialize collection/catalog
  ({pr}`179`) [@andersy005](https://github.com/andersy005)
- Allow passing extra storage options to the backend file system via
  {py:meth}`~intake_esm.core.esm_datastore.to_dataset_dict` ({pr}`180`) [@bonnland](https://github.com/bonnland)
- Provide informational messages to the user via Logging module
  ({pr}`186`) [@andersy005](https://github.com/andersy005)

### Bug Fixes

- Remove the caching option ({pr}`158`) [@matt-long](https://github.com/matt-long)
- Preserve encoding when aggregating datasets ({pr}`161`) [@matt-long](https://github.com/matt-long)
- Sort aggregations to make sure {py:func}:`~intake_esm.merge_util.join_existing`
  is always done before {py:func}:`~intake_esm.merge_util.join_new`
  ({pr}`171`) [@andersy005](https://github.com/andersy005)

### Documentation

- Add example for preprocessing function ({pr}`168`) [@jbusecke](https://github.com/jbusecke)
- Add FAQ style document to documentation ({pr}`182`) & ({issue}`177`)
  [@andersy005](https://github.com/andersy005) & [@jhamman](https://github.com/jhamman)

### Internal Changes

- Simplify group loading by using `concurrent.futures` ({pr}`185`) [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2019-10-15&to=2019-12-13&type=c))

[@andersy005](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Aandersy005+updated%3A2019-10-15..2019-12-13&type=Issues) | [@bonnland](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Abonnland+updated%3A2019-10-15..2019-12-13&type=Issues) | [@dcherian](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Adcherian+updated%3A2019-10-15..2019-12-13&type=Issues) | [@jbusecke](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajbusecke+updated%3A2019-10-15..2019-12-13&type=Issues) | [@jhamman](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Ajhamman+updated%3A2019-10-15..2019-12-13&type=Issues) | [@matt-long](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Amatt-long+updated%3A2019-10-15..2019-12-13&type=Issues) | [@naomi-henderson](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Anaomi-henderson+updated%3A2019-10-15..2019-12-13&type=Issues) | [@Recalculate](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3ARecalculate+updated%3A2019-10-15..2019-12-13&type=Issues) | [@sebasblancogonz](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Asebasblancogonz+updated%3A2019-10-15..2019-12-13&type=Issues)

## Intake-esm v2019.10.15

### Features

- Rewrite `intake-esm`'s core based on `(esm-collection-spec)`\_ Earth System Model Collection specification
  ({pr}`135`) [@andersy005](https://github.com/andersy005), [@matt-long](https://github.com/matt-long), [@rabernat](https://github.com/rabernat)

### Breaking changes

- Replaced {py:class}:`~intake_esm.core.esm_metadatastore` with {py:class}:`~intake_esm.core.esm_datastore`, see the API reference for more details.
- `intake-esm` won't build collection catalogs anymore. `intake-esm` now expects an ESM collection JSON file as input. This JSON should conform to the [Earth System Model Collection](https://github.com/NCAR/esm-collection-spec) specification.

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2019-08-23&to=2019-10-15&type=c))

[@aaronspring](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Aaaronspring+updated%3A2019-08-23..2019-10-15&type=Issues) | [@andersy005](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Aandersy005+updated%3A2019-08-23..2019-10-15&type=Issues) | [@bonnland](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Abonnland+updated%3A2019-08-23..2019-10-15&type=Issues) | [@dcherian](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Adcherian+updated%3A2019-08-23..2019-10-15&type=Issues) | [@n-henderson](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3An-henderson+updated%3A2019-08-23..2019-10-15&type=Issues) | [@naomi-henderson](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Anaomi-henderson+updated%3A2019-08-23..2019-10-15&type=Issues) | [@rabernat](https://github.com/search?q=repo%3Aintake%2Fintake-esm+involves%3Arabernat+updated%3A2019-08-23..2019-10-15&type=Issues)

## Intake-esm v2019.8.23

### Features

- Add `mistral` data holdings to `intake-esm-datastore` ({pr}`133`) [@aaronspring](https://github.com/aaronspring)

- Add support for `NA-CORDEX` data holdings. ({pr}`115`) [@jukent](https://github.com/jukent)

- Replace `.csv` with `netCDF` as serialization format when saving the built collection to disk. With `netCDF`, we can record very useful information into the global attributes of the netCDF dataset. ({pr}`119`) [@andersy005](https://github.com/andersy005)

- Add string representation of ` ESMMetadataStoreCatalog`` object ({pr} `122`) [@andersy005](https://github.com/andersy005)

- Automatically build missing collections by calling `esm_metadatastore(collection_name="GLADE-CMIP5")`. When the specified collection is part of the curated collections in `intake-esm-datastore`. ({pr}`124`) [@andersy005](https://github.com/andersy005)

  ```python

  In [1]: import intake

  In [2]: col = intake.open_esm_metadatastore(collection_name="GLADE-CMIP5")

  In [3]: # if "GLADE-CMIP5" collection isn't built already, the above is equivalent to:

  In [4]: col = intake.open_esm_metadatastore(collection_input_definition="GLADE-CMIP5")
  ```

- Revert back to using official DRS attributes when building CMIP5 and CMIP6 collections.
  ({pr}`126`) [@andersy005](https://github.com/andersy005)

- Add `.df` property for interfacing with the built collection via dataframe
  To maintain backwards compatiblity. ({pr}`127`) [@andersy005](https://github.com/andersy005)

- Add `unique()` and `nunique()` methods for summarizing count and unique values in a collection.
  ({pr}`128`) [@andersy005](https://github.com/andersy005)

  ```python

  In [1]: import intake

  In [2]: col = intake.open_esm_metadatastore(collection_name="GLADE-CMIP5")

  In [3]: col
  Out[3]: GLADE-CMIP5 collection catalogue with 615853 entries: > 3 resource(s)

            > 1 resource_type(s)

            > 1 direct_access(s)

            > 1 activity(s)

            > 218 ensemble_member(s)

            > 51 experiment(s)

            > 312093 file_basename(s)

            > 615853 file_fullpath(s)

            > 6 frequency(s)

            > 25 institute(s)

            > 15 mip_table(s)

            > 53 model(s)

            > 7 modeling_realm(s)

            > 3 product(s)

            > 9121 temporal_subset(s)

            > 454 variable(s)

            > 489 version(s)

  In[4]: col.nunique()

  resource 3
  resource_type 1
  direct_access 1
  activity 1
  ensemble_member 218
  experiment 51
  file_basename 312093
  file_fullpath 615853
  frequency 6
  institute 25
  mip_table 15
  model 53
  modeling_realm 7
  product 3
  temporal_subset 9121
  variable 454
  version 489
  dtype: int64

  In[4]: col.unique(columns=['frequency', 'modeling_realm'])

  {'frequency': {'count': 6, 'values': ['mon', 'day', '6hr', 'yr', '3hr', 'fx']},
  'modeling_realm': {'count': 7, 'values': ['atmos', 'land', 'ocean', 'seaIce', 'ocnBgchem',
  'landIce', 'aerosol']}}

  ```

### Bug Fixes

- For CMIP6, extract `grid_label` from directory path instead of file name. ({pr}`127`) [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2019-10-15&to=2019-12-13&type=c))

## Intake-esm v2019.8.5

### Features

- Support building collections using inputs from intake-esm-datastore repository.
  ({pr}`79`) [@andersy005](https://github.com/andersy005)

- Ensure that requested files are available locally before loading data into xarray datasets.
  ({pr}`82`) [@andersy005](https://github.com/andersy005) and [@matt-long](https://github.com/matt-long)

- Split collection definitions out of config. ({pr}`83`) [@matt-long](https://github.com/matt-long)

- Add `intake-esm-builder`, a CLI tool for building collection from the command line. ({pr}`89`) [@andersy005](https://github.com/andersy005)

- Add support for CESM-LENS data holdings residing in AWS S3. ({pr}`98`) [@andersy005](https://github.com/andersy005)

- Sort collection upon creation according to order-by-columns, pass urlpath through stack for use in parsing collection filenames ({pr}`100`) [@pbranson](https://github.com/pbranson)

### Bug Fixes

- Fix bug in `_list_files_hsi()` to return list instead of filter object.
  ({pr}`81`) [@matt-long](https://github.com/matt-long) and [@andersy005](https://github.com/andersy005)

- `cesm._get_file_attrs` fixed to break loop when longest `stream` is matched. ({pr}`80`) [@matt-long](https://github.com/matt-long)

- Restore `non_dim_coords` to data variables all the time. ({pr}`90`) [@andersy005](https://github.com/andersy005)

- Fix bug in `intake_esm/cesm.py` that caused `intake-esm` to exclude hourly (1hr, 6hr, etc..) CESM-LE data.
  ({pr}`110`) [@andersy005](https://github.com/andersy005)

- Fix bugs in `intake_esm/cmip.py` that caused improper regular expression matching for `table_id` and `grid_label`.
  ({pr}`113`) & ({issue}`111`) [@naomi-henderson](https://github.com/naomi-henderson) and [@andersy005](https://github.com/andersy005)

### Internal Changes

- Refactor existing functionality to make intake-esm robust and extensible. ({pr}`77`) [@andersy005](https://github.com/andersy005)

- Add `aggregate._override_coords` function to override dim coordinates except time
  in case there's floating point precision difference. ({pr}`108`) [@andersy005](https://github.com/andersy005)

- Fix CESM-LE ice component peculiarities that caused intake-esm to load data improperly.
  The fix separates variables for `ice` component into two separate components:

  - `ice_sh`: for southern hemisphere
  - `ice_nh`: for northern hemisphere

  ({pr}`114`) [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2019-05-11&to=2019-08-05&type=c))

## Intake-esm v2019.5.11

### Features

- Add implementation for The Gridded Meteorological Ensemble Tool (GMET) data holdings ({pr}`61`) [@andersy005](https://github.com/andersy005)
- Allow users to specify exclude\*dirs for CMIP collections ({pr}`63`) & ({issue}`62`) [@andersy005](https://github.com/andersy005)
- Keep CMIP6 `tracking_id` in `merge_keys` ({pr}`67`) [@andersy005](https://github.com/andersy005)
- Add implementation for ERA5 datasets ({pr}`68`) [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2019-04-26&to=2019-05-11&type=c))

## Intake-esm v2019.4.26

### Features

- Add implementations for `CMIPCollection` and `CMIPSource` ({pr}`38`) [@andersy005](https://github.com/andersy005)
- Add support for CMIP6 data ({pr}`46`) [@andersy005](https://github.com/andersy005)
- Add implementation for The Max Planck Institute Grand Ensemble (MPI-GE) data holdings ({pr}`52`) & ({issue}`51`) [@aaronspring](https://github.com/aaronspring) and [@andersy005](https://github.com/andersy005)
- Return dictionary of datasets all the time for consistency ({pr}`56`) [@andersy005](https://github.com/andersy005)

### Bug Fixes

- Include multiple netcdf files in same subdirectory ({pr}`55`) & ({issue}`54`) [@naomi-henderson](https://github.com/naomi-henderson) and [@andersy005](https://github.com/andersy005)

### Contributors to this release

([GitHub contributors page for this release](https://github.com/intake/intake-esm/graphs/contributors?from=2019-02-28&to=2019-04-26&type=c))

## Intake-esm v2019.2.28

### Features

- Allow CMIP integration ({pr}`35`) [@andersy005](https://github.com/andersy005)

### Bug Fixes

- Fix bug on build catalog and move `exclude_dirs` to `locations` ({pr}`33`) [@matt-long](https://github.com/matt-long)

### Internal Changes

- Change Logger, update dev-environment dependencies, and formatting fix in input.yml ({pr}`31`) [@matt-long](https://github.com/matt-long)
- Update CircleCI workflow ({pr}`32`) [@andersy005](https://github.com/andersy005)
- Rename package from `intake-cesm` to `intake-esm` ({pr}`34`) [@andersy005](https://github.com/andersy005)
