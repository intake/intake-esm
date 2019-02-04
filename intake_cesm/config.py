#!/usr/bin/env python
""" The configuration utility script. This script is used
to pass CESM database/files to intake_cesm DataSources.
"""

from __future__ import absolute_import, print_function

import os

collection_dir = os.path.join(os.path.dirname(__file__), "..", "collections")

collections = {
    "cesm_dple": os.path.join(collection_dir, "cesm_dple.csv"),
    "cesm2_runs": os.path.join(collection_dir, "cesm2_runs.csv"),
    "cesm1_le": os.path.join(collection_dir, "cesm1_le.csv"),
}
