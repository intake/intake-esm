import os

import intake
import pandas as pd
import pytest

here = os.path.abspath(os.path.dirname(__file__))


def test_build_collection():
    collection_input_file = os.path.join(here, 'cmip_collection_input_test.yml')
    col = intake.open_esm_metadatastore(collection_input_file=collection_input_file)
    assert isinstance(col.df, pd.DataFrame)
