import logging

import pandas as pd

from sdv.relational import HMA1
from sdv.tabular import CTGAN

LOGGER = logging.getLogger(__name__)


class NewModel(HMA1):
    DEFAULT_MODEL = CTGAN
    DEFAULT_MODEL_KWARGS = {}

    def __init__(self, metadata, root_path=None, model=DEFAULT_MODEL,
                 model_kwargs=DEFAULT_MODEL_KWARGS):
        HMA1.__init__(self, metadata, root_path, model, model_kwargs)

    def _get_extension(self, child_name, child_table, foreign_key):
        table_meta = self._models[child_name].get_metadata()

        extension_rows = list()
        foreign_key_values = child_table[foreign_key].unique()
        child_table = child_table.set_index(foreign_key)
        child_primary = self.metadata.get_primary_key(child_name)

        index = []
        scale_columns = None
        for foreign_key_value in foreign_key_values:
            child_rows = child_table.loc[[foreign_key_value]]
            if child_primary in child_rows.columns:
                del child_rows[child_primary]

            try:
                model = self._model(table_metadata=table_meta)
                model.fit(child_rows.reset_index(drop=True))
                row = model.get_parameters()
                row = pd.Series(row)
                row.index = f'__{child_name}__{foreign_key}__' + row.index

                if scale_columns is None:
                    scale_columns = [
                        column
                        for column in row.index
                        if column.endswith('scale')
                    ]

                if len(child_rows) == 1:
                    row.loc[scale_columns] = None

                extension_rows.append(row)
                index.append(foreign_key_value)
            except Exception as e:
                print(e)
                import traceback
                traceback.print_exc()
                # Skip children rows subsets that fail
                pass
                raise e

        return pd.DataFrame(extension_rows, index=index)


    def _extend_table(self, table, tables, table_name):
        LOGGER.info('Computing extensions for table %s', table_name)
        for child_name in self.metadata.get_children(table_name):
            if child_name not in self._models:
                child_table = self._model_table(child_name, tables)
            else:
                child_table = tables[child_name]

            foreign_keys = self.metadata.get_foreign_keys(table_name, child_name)
            for index, foreign_key in enumerate(foreign_keys):
                extension = self._get_extension(child_name, child_table, foreign_key)
                table = table.merge(extension, how='left', right_index=True, left_index=True)
                num_rows_key = f'__{child_name}__{foreign_key}__num_rows'
                table[num_rows_key].fillna(0, inplace=True)
                self._max_child_rows[num_rows_key] = table[num_rows_key].max()

        return table

    def _model_table(self, table_name, tables):
        LOGGER.info('Self not child Modeling %s', table_name)

        table = self._load_table(tables, table_name)
        self._table_sizes[table_name] = len(table)

        primary_key = self.metadata.get_primary_key(table_name)
        if primary_key:
            table = table.set_index(primary_key)
            table = self._extend_table(table, tables, table_name)

        table_meta, keys = self._prepare_for_modeling(table, table_name, primary_key)

        LOGGER.info('Fitting %s for table %s; shape: %s', self._model.__name__,
                    table_name, table.shape)
        model = self._model(**self._model_kwargs, table_metadata=table_meta)
        model.fit(table)
        self._models[table_name] = model

        if primary_key:
            table.reset_index(inplace=True)

        for name, values in keys.items():
            table[name] = values

        tables[table_name] = table

        return table

    def _fit(self, tables=None):
        self.metadata.validate(tables)
        if tables:
            tables = tables.copy()
        else:
            tables = {}

        for table_name in self.metadata.get_tables():
            if not self.metadata.get_parents(table_name):
                self._model_table(table_name, tables)

        LOGGER.info('Modeling Complete')
