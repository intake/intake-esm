import intake


class AbstractESMEntry(intake.catalog.entry.CatalogEntry):
    def __init__(self, df):
        self.df = df

    def __len__(self):
        return len(self.df)


class SingleEntry(AbstractESMEntry):
    ...


class AggregateEntry(AbstractESMEntry):
    ...
