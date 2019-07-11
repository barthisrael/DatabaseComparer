import os
import Spartacus.Database

from .import custom_exceptions


def compare_datatables(p_database_1=None, p_database_2=None, p_block_size=None, p_key=None, p_sql=None, p_inserted_callback=None, p_updated_callback=None, p_deleted_callback=None, p_equal_callback=None):
    """Used to compare data between datatables. Such objects are fetched by blocks using given database connections and SQL query.

        Args:
            p_database_1 (Spartacus.Database.PostgreSQL): the first database. Defaults to None.
            p_database_2 (Spartacus.Database.PostgreSQL): the second database. Defaults to None.
            p_block_size (int): Number of data records that the comparer will deal with at the same time. Defaults to None.
            p_key (list): list of columns that form the table records key. Defaults to None.
            p_sql (str): the sql query to be executed in both databases. Defaults to None.
            p_inserted_callback (function): callback executed when an inserted record is found in database 2. Defaults to None.
                Notes:
                    Has the following parameters:
                        p_columns (list): list of columns that are present in p_row parameter.
                        p_row (list): the row that was inserted in the database 2.
                        p_key (list): the key used for comparison.
            p_updated_callback (function): callback executed when an updated record is found in database 2. Defaults to None.
                Notes:
                    Has the following parameters:
                        p_columns (list): list of columns that are present in p_row_1 and p_row_2 parameters.
                        p_row_1 (list): the row as it was in database 1.
                        p_row_2 (list): the row as it is in the database 2.
                        p_key (list): the key used for comparison.
                        p_all_diffs (list): list of fields that are different between databases. Each item has the following structure:
                            {
                                'column' (str): the column that differs,
                                'old_value' (object): value in database 1,
                                'new_value' (object): value in database 2.
                            }
            p_deleted_callback (function): the callback executed when a deleted record is not found in database 2. Defaults to None.
                Notes:
                    Has the following parameters:
                        p_columns (list): list of columns that are present in p_row parameter.
                        p_row (list): the row that was removed from database 2.
                        p_key (list): the key used for comparison.
            p_equal_callback (function): the callback executed when a record exists in tha same way in both databases. Defaults to None.
                Notes:
                    p_columns (list): list of columns that are present in p_row parameter.
                    p_row (list): the row that that matched.
                    p_key (list): the key used for comparison.

        Raises:
            custom_exceptions.InvalidParameterTypeException.
            custom_exceptions.InvalidParameterValueException.
    """

    if not isinstance(p_database_1, Spartacus.Database.PostgreSQL):
        raise custom_exceptions.InvalidParameterTypeException('"p_database_1" parameter must be a "Spartacus.Database.PostgreSQL" instance.', p_database_1)

    if not isinstance(p_database_2, Spartacus.Database.PostgreSQL):
        raise custom_exceptions.InvalidParameterTypeException('"p_database_2" parameter must be a "Spartacus.Database.PostgreSQL" instance.', p_database_2)

    if not isinstance(p_block_size, int):
        raise custom_exceptions.InvalidParameterTypeException('"p_block_size" parameter must be an "int" instance.', p_block_size)

    if p_block_size < 1:
        raise custom_exceptions.InvalidParameterValueException('"p_block_size" parameter must be a positive "int" instance.', p_block_size)

    if not isinstance(p_key, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_key" parameter must be a "list" instance.', p_key)

    if not isinstance(p_sql, str):
        raise custom_exceptions.InvalidParameterTypeException('"p_sql" parameter must be a "str" instance.', p_sql)

    if p_inserted_callback is not None and not callable(p_inserted_callback):
        raise custom_exceptions.InvalidParameterTypeException('"p_inserted_callback" parameter must be a callable "function".', p_inserted_callback)

    if p_updated_callback is not None and not callable(p_updated_callback):
        raise custom_exceptions.InvalidParameterTypeException('"p_updated_callback" parameter must be a callable "function".', p_updated_callback)

    if p_deleted_callback is not None and not callable(p_deleted_callback):
        raise custom_exceptions.InvalidParameterTypeException('"p_deleted_callback" parameter must be a callable "function".', p_deleted_callback)

    if p_equal_callback is not None and not callable(p_equal_callback):
        raise custom_exceptions.InvalidParameterTypeException('"p_equal_callback" parameter must be a callable "function".', p_equal_callback)

    p_database_1.Open(p_autocommit=False)
    p_database_2.Open(p_autocommit=False)

    #Query first block in each database
    v_table_1 = p_database_1.QueryBlock(p_sql=p_sql, p_blocksize=p_block_size)
    v_table_2 = p_database_2.QueryBlock(p_sql=p_sql, p_blocksize=p_block_size)

    if v_table_1.Columns != v_table_2.Columns:
        raise Exception('Cannot compare table with different columns.')

    #Set comparison key
    v_key = p_key

    v_has_more_data_1 = True
    v_has_more_data_2 = True
    v_index_1 = 0
    v_index_2 = 0

    #Main loop, compare tables data
    while v_has_more_data_1 or v_has_more_data_2:
        while v_index_1 < len(v_table_1.Rows) and v_index_2 < len(v_table_2.Rows):
            v_row_1 = v_table_1.Rows[v_index_1]
            v_row_2 = v_table_2.Rows[v_index_2]

            v_record_1_pk = '_'.join(
                [
                    str(v_row_1[v_column])
                    for v_column in v_key
                ]
            )

            v_record_2_pk = '_'.join(
                [
                    str(v_row_2[v_column])
                    for v_column in v_key
                ]
            )

            #Record in both datatables
            if v_record_1_pk == v_record_2_pk:
                v_all_match = True
                v_output_row = []
                v_all_diffs = []

                for v_column in v_table_1.Columns:
                    if not v_table_1.Equal(v_row_1[v_column], v_row_2[v_column]):
                        v_all_diffs.append({
                            'column': v_column,
                            'old_value': v_row_1[v_column],
                            'new_value': v_row_2[v_column]
                        })
                        v_all_match = False

                if v_all_match:
                    if p_equal_callback is not None:
                        p_equal_callback(
                            v_table_2.Columns,
                            v_row_2,
                            v_key
                        )
                else:
                    if p_updated_callback is not None:
                        p_updated_callback(
                            v_table_1.Columns,
                            v_row_1,
                            v_row_2,
                            v_key,
                            v_all_diffs,
                        )

                v_index_1 += 1
                v_index_2 += 1
            #Record was deleted from second database
            elif v_record_1_pk < v_record_2_pk:
                if p_deleted_callback is not None:
                    p_deleted_callback(
                        v_table_1.Columns,
                        v_row_1,
                        v_key
                    )

                v_index_1 += 1
            #Record was inserted into second database
            else:
                if p_inserted_callback is not None:
                    p_inserted_callback(
                        v_table_2.Columns,
                        v_row_2,
                        v_key
                    )

                v_index_2 += 1

        v_has_more_data_1 = not p_database_1.v_start
        v_has_more_data_2 = not p_database_2.v_start

        if not v_has_more_data_1:
            #Data fetch finished on first database, so let's insert remaining rows of table 2, if any
            while v_index_2 < len(v_table_2.Rows):
                v_row_2 = v_table_2.Rows[v_index_2]

                if p_inserted_callback is not None:
                    p_inserted_callback(
                        v_table_2.Columns,
                        v_row_2,
                        v_key
                    )

                v_index_2 += 1

        if not v_has_more_data_2:
            #Data fetch finished on second database, so let's insert remaining rows of table 1, if any
            while v_index_1 < len(v_table_1.Rows):
                v_row_1 = v_table_1.Rows[v_index_1]

                if p_deleted_callback is not None:
                    p_deleted_callback(
                        v_table_1.Columns,
                        v_row_1,
                        v_key
                    )

                v_index_1 += 1

        if v_index_1 == len(v_table_1.Rows) and v_has_more_data_1:
            v_table_1 = p_database_1.QueryBlock(p_sql=p_sql, p_blocksize=p_block_size)
            v_index_1 = 0

        if v_index_2 == len(v_table_2.Rows) and v_has_more_data_2:
            v_table_2 = p_database_2.QueryBlock(p_sql=p_sql, p_blocksize=p_block_size)
            v_index_2 = 0

    p_database_1.Close(p_commit=False)
    p_database_2.Close(p_commit=False)
