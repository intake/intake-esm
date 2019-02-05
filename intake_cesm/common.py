import os

import numpy as np
import pandas as pd

from .config import SETTINGS


def open_collection(collection):
    """ Open a CESM collection and return a Pandas dataframe """
    try:
        db_dir = SETTINGS["database_directory"]
        collection_path = os.path.join(db_dir, f"{collection}.csv")
        df = pd.read_csv(collection_path, index_col=0)
        return df

    except (FileNotFoundError) as err:
        print("****** The specified collection does not exit. ******")
        raise err


def get_subset(collection, query):
    """ Get a subset of collection entries that match a query """
    df = open_collection(collection)

    condition = np.ones(len(df), dtype=bool)

    for key, val in query.items():

        if isinstance(val, list):
            condition_i = np.zeros(len(df), dtype=bool)
            for val_i in val:
                condition_i = condition_i | (df[key] == val_i)
            condition = condition & condition_i

        elif val is not None:
            condition = condition & (df[key] == val)

    query_results = df.loc[condition].sort_values(
        by=["sequence_order", "files"], ascending=True
    )

    return query_results


def get_collection_def(collection):
    """Return list of columns defining a collection.
    """
    return open_collection(collection).columns.tolist()
