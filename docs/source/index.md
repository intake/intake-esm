---
sd_hide_title: true
---

# Overview

::::{grid}
:reverse:
:gutter: 3 4 4 4
:margin: 1 2 1 2

:::{grid-item}
:columns: 12 4 4 4

```{image} ../_static/images/NSF_4-Color_bitmap_Logo.png
:width: 200px
:class: sd-m-auto
```

:::

:::{grid-item}
:columns: 12 8 8 8
:child-align: justify
:class: sd-fs-5

```{rubric} Intake-ESM

```

A data cataloging utility built on top of [intake](https://github.com/intake/intake), [pandas](https://pandas.pydata.org/), and [xarray](https://xarray.pydata.org/en/stable/), and it's pretty awesome!

```{button-ref} how-to/install-intake-esm
:ref-type: doc
:color: primary
:class: sd-rounded-pill

Get Started
```

:::

::::

---

## Motivation

Computer simulations of the Earth’s climate and weather generate huge amounts of data.
These data are often persisted on HPC systems or in the cloud across multiple data
assets of a variety of formats ([netCDF](https://www.unidata.ucar.edu/software/netcdf/), [zarr](https://zarr.readthedocs.io/en/stable/), etc...). Finding, investigating,
loading these data assets into compute-ready data containers costs time and effort.
The data user needs to know what data sets are available, the attributes describing
each data set, before loading a specific data set and analyzing it.

Finding, investigating, loading these assets into data array containers
such as xarray can be a daunting task due to the large number of files
a user may be interested in. Intake-esm aims to address these issues by
providing necessary functionality for searching, discovering, data access/loading.

---

## Get in touch

- If you encounter any errors or problems with **intake-esm**, please open an issue at the GitHub [main repository](http://github.com/intake/intake-esm/issues).
- If you have a question like “How do I find x?”, ask on [GitHub discussions](https://github.com/intake/intake-esm/discussions). Please include a self-contained reproducible example if possible.

---

```{toctree}
---
maxdepth: 1
caption: Tutorials
hidden:
---
tutorials/loading-cmip6-data.md
```

```{toctree}
---
maxdepth: 2
caption: How to guides and examples
hidden:
---

how-to/install-intake-esm.md
how-to/build-a-catalog-from-timeseries-files.md
how-to/define-and-use-derived-variable-registry.md
how-to/use-catalogs-with-assets-containing-multiple-variables.md
how-to/filter-catalog-by-substring-and-regex-criteria.md
how-to/enforce-search-query-criteria-via-require-all-on.md
how-to/understand-keys-and-how-to-change-them.md
how-to/modify-catalog.md
```

```{toctree}
---
maxdepth: 2
caption: Reference
hidden:
---

reference/esm-catalog-spec.md
reference/api.md
reference/faq.md
reference/cmip_ap.md



```

```{toctree}
---
maxdepth: 2
caption: Development
hidden:
---

contributing.md
reference/changelog.md

```

```{toctree}
---
maxdepth: 2
caption: Project links
hidden:
---


GitHub Repo <https://github.com/intake/intake-esm>
GitHub discussions <https://github.com/intake/intake-esm/discussions>

```
