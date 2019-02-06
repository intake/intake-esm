import os

import pandas as pd

import intake_cesm

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection():
    collection_input_file = os.path.join(here, 'collection_input_test.yml')
    collection_type_def_file = os.path.join('intake_cesm/cesm_definitions.yml')
    col = intake_cesm.CESMCollections(
        collection_input_file=collection_input_file,
        collection_type_def_file=collection_type_def_file,
    )
    assert isinstance(col.df, pd.DataFrame)
