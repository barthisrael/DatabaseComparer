import os
import inspect
import multiprocessing
import Spartacus.Database

from .import custom_exceptions
from .import utils


def compare_tables_data(p_database_1=None, p_database_2=None, p_block_size=None, p_schema=None, p_table=None, p_key=None, p_queue=None, p_is_sending_data_array=None, p_worker_index=None):
    """Used to compare tables data between databases.

        Args:
            p_database_1 (Spartacus.Database.PostgreSQL): the first database. Defaults to None.
            p_database_2 (Spartacus.Database.PostgreSQL): the second database. Defaults to None.
            p_block_size (int): Number of data records that the comparer will deal with at the same time. Defaults to None.
            p_schema (str): the schema name. Defaults to None.
            p_table (str): the table name. Defaults to None.
            p_key (str): list of comma separated columns that form the table records key. Defaults to None.
            p_queue (multiprocessing.managers.BaseProxy): queue used to communicate to parent process. Created from a multiprocessing.Manager instance. Defaults to None.
            p_is_sending_data_array (multiprocessing.managers.ArrayProxy): array used to control process that are still sending data. Defaults to None.
            p_worker_index (int): the worker sub process index. Defaults to None.

        Raises:
            custom_exceptions.InvalidParameterTypeException.
            custom_exceptions.InvalidParameterValueException.
    """

    try:
        if not isinstance(p_database_1, Spartacus.Database.PostgreSQL):
            raise custom_exceptions.InvalidParameterTypeException('"p_database_1" parameter must be a "Spartacus.Database.PostgreSQL" instance.', p_database_1)

        if not isinstance(p_database_2, Spartacus.Database.PostgreSQL):
            raise custom_exceptions.InvalidParameterTypeException('"p_database_2" parameter must be a "Spartacus.Database.PostgreSQL" instance.', p_database_2)

        if not isinstance(p_block_size, int):
            raise custom_exceptions.InvalidParameterTypeException('"p_block_size" parameter must be an "int" instance.', p_block_size)

        if p_block_size < 1:
            raise custom_exceptions.InvalidParameterValueException('"p_block_size" parameter must be a positive "int" instance.', p_block_size)

        if not isinstance(p_schema, str):
            raise custom_exceptions.InvalidParameterTypeException('"p_schema" parameter must be a "str" instance.', p_schema)

        if not isinstance(p_table, str):
            raise custom_exceptions.InvalidParameterTypeException('"p_table" parameter must be a "str" instance.', p_table)

        if not isinstance(p_key, str):
            raise custom_exceptions.InvalidParameterTypeException('"p_key" parameter must be a "str" instance.', p_key)

        if not isinstance(p_queue, multiprocessing.managers.BaseProxy):
            raise custom_exceptions.InvalidParameterTypeException('"p_queue" parameter must be a "multiprocessing.managers.BaseProxy" instance.', p_queue)

        if not isinstance(p_is_sending_data_array, multiprocessing.managers.ArrayProxy):
            raise custom_exceptions.InvalidParameterTypeException('"p_is_sending_data_array" parameter must be an "multiprocessing.managers.ArrayProxy" instance.', p_is_sending_data_array)

        if not isinstance(p_worker_index, int):
            raise custom_exceptions.InvalidParameterTypeException('"p_worker_index" parameter must be an "int" instance.', p_worker_index)

        if p_worker_index < 0:
            raise custom_exceptions.InvalidParameterTypeException('"p_worker_index" parameter must be an "int" instance greater than or equal to 0.', p_worker_index)

        p_database_1.Open(p_autocommit=False)
        p_database_2.Open(p_autocommit=False)

        #Get columns types
        v_table = p_database_2.Query(
            p_sql='''
                SELECT column_name,
                       FORMAT(
                           '%s %s',
                           data_type,
                           (CASE WHEN character_maximum_length IS NOT NULL
                                 THEN FORMAT(
                                          '(%s)',
                                          character_maximum_length
                                      )
                                 WHEN numeric_precision IS NOT NULL AND NULLIF(numeric_scale, 0) IS NOT NULL
                                 THEN FORMAT(
                                          '(%s, %s)',
                                          numeric_precision,
                                          numeric_scale
                                      )
                                 ELSE ''
                            END)
                       ) AS data_type
                FROM information_schema.columns
                WHERE table_schema = '{p_schema}'
                  AND table_name = '{p_table}'
            '''.format(
                p_schema=p_schema,
                p_table=p_table
            )
        )

        v_column_type_dict = {}

        for v_row in v_table.Rows:
            v_column_type_dict[v_row['column_name']] = v_row['data_type']

        #Prepare table query SQL
        v_sql = '''
            SELECT *
            FROM {p_schema}.{p_table}
            ORDER BY {p_order}
        '''.format(
            p_schema=p_schema,
            p_table=p_table,
            p_order=p_key
        )

        #Query first block of table in each database
        v_table_1 = None
        v_table_2 = p_database_2.QueryBlock(p_sql=v_sql, p_blocksize=p_block_size)

        try:
            v_table_1 = p_database_1.QueryBlock(p_sql=v_sql, p_blocksize=p_block_size)
        except Spartacus.Database.Exception:
            #Table does not exist in database 1, let's create a fake one just for comparison
            v_table_1 = Spartacus.Database.DataTable()

            for v_column in v_table_2.Columns:
                v_table_1.AddColumn(p_columnname=v_column)

        if v_table_1.Columns != v_table_2.Columns:
            raise Exception('Cannot compare table with different columns: {p_schema}.{p_table}.'.format(p_schema=p_schema, p_table=p_table))

        #Set comparison key
        v_key = p_key.split(',')

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

                #Record in both databases
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

                    if not v_all_match:
                        p_queue.put({
                            'type': 'tables_data',
                            'row': {
                                'schema_name': p_schema,
                                'table_name': p_table,
                                'status': 'UPDATED',
                                'sql': inspect.cleandoc(
                                    doc='''\
                                        UPDATE {p_schema}.{p_table}
                                        SET {p_set}
                                        WHERE {p_condition};
                                    '''.format(
                                        p_schema=p_schema,
                                        p_table=p_table,
                                        p_set=','.join([
                                            '{p_column} = {p_value}::{p_type}'.format(
                                                p_column=v_diff['column'],
                                                p_value='$data_comparer${p_value}$data_comparer$'.format(p_value=str(v_diff['new_value'])) if v_diff['new_value'] is not None else 'NULL',
                                                p_type=v_column_type_dict[v_diff['column']]
                                            )
                                            for v_diff in v_all_diffs
                                        ]),
                                        p_condition=' AND '.join([
                                            '{p_column} = {p_value}::{p_type}'.format(
                                                p_column=v_column,
                                                p_value='$data_comparer${p_value}$data_comparer$'.format(p_value=str(v_row_2[v_column])) if v_row_2[v_column] is not None else 'NULL',
                                                p_type=v_column_type_dict[v_column]
                                            )
                                            for v_column in v_key
                                        ])
                                    )
                                )
                            }
                        })

                    v_index_1 += 1
                    v_index_2 += 1
                #Record was deleted from second database
                elif v_record_1_pk < v_record_2_pk:
                    p_queue.put({
                        'type': 'tables_data',
                        'row': {
                            'schema_name': p_schema,
                            'table_name': p_table,
                            'status': 'DELETED',
                            'sql': inspect.cleandoc(
                                doc='''\
                                    DELETE
                                    FROM {p_schema}.{p_table}
                                    WHERE {p_condition};
                                '''.format(
                                    p_schema=p_schema,
                                    p_table=p_table,
                                    p_condition=' AND '.join([
                                        '{p_column} = {p_value}::{p_type}'.format(
                                            p_column=v_column,
                                            p_value='$data_comparer${p_value}$data_comparer$'.format(p_value=str(v_row_1[v_column])) if v_row_1[v_column] is not None else 'NULL',
                                            p_type=v_column_type_dict[v_column]
                                        )
                                        for v_column in v_key
                                    ])
                                )
                            )
                        }
                    })

                    v_index_1 += 1
                #Record was inserted into second database
                else:
                    p_queue.put({
                        'type': 'tables_data',
                        'row': {
                            'schema_name': p_schema,
                            'table_name': p_table,
                            'status': 'INSERTED',
                            'sql': inspect.cleandoc(
                                doc='''\
                                    INSERT INTO {p_schema}.{p_table} (
                                        {p_columns}
                                    ) VALUES (
                                        {p_values}
                                    )
                                '''.format(
                                    p_schema=p_schema,
                                    p_table=p_table,
                                    p_columns=','.join(v_table_2.Columns),
                                    p_values=','.join([
                                        '{p_value}::{p_type}'.format(
                                            p_value='$data_comparer${p_value}$data_comparer$'.format(p_value=str(v_row_2[v_column])) if v_row_2[v_column] is not None else 'NULL',
                                            p_type=v_column_type_dict[v_column]
                                        )
                                        for v_column in v_table_2.Columns
                                    ])
                                )
                            )
                        }
                    })

                    v_index_2 += 1

            v_has_more_data_1 = not p_database_1.v_start
            v_has_more_data_2 = not p_database_2.v_start

            if not v_has_more_data_1:
                #Data fetch finished on first database, so let's insert remaining rows of table 2, if any
                while v_index_2 < len(v_table_2.Rows):
                    v_row_2 = v_table_2.Rows[v_index_2]

                    p_queue.put({
                        'type': 'tables_data',
                        'row': {
                            'schema_name': p_schema,
                            'table_name': p_table,
                            'status': 'INSERTED',
                            'sql': inspect.cleandoc(
                                doc='''\
                                    INSERT INTO {p_schema}.{p_table} (
                                        {p_columns}
                                    ) VALUES (
                                        {p_values}
                                    )
                                '''.format(
                                    p_schema=p_schema,
                                    p_table=p_table,
                                    p_columns=','.join(v_table_2.Columns),
                                    p_values=','.join([
                                        '{p_value}::{p_type}'.format(
                                            p_value='$data_comparer${p_value}$data_comparer$'.format(p_value=str(v_row_2[v_column])) if v_row_2[v_column] is not None else 'NULL',
                                            p_type=v_column_type_dict[v_column]
                                        )
                                        for v_column in v_table_2.Columns
                                    ])
                                )
                            )
                        }
                    })

                    v_index_2 += 1

            if not v_has_more_data_2:
                #Data fetch finished on second database, so let's insert remaining rows of table 1, if any
                while v_index_1 < len(v_table_1.Rows):
                    v_row_1 = v_table_1.Rows[v_index_1]

                    p_queue.put({
                        'type': 'tables_data',
                        'row': {
                            'schema_name': p_schema,
                            'table_name': p_table,
                            'status': 'DELETED',
                            'sql': inspect.cleandoc(
                                doc='''\
                                    DELETE
                                    FROM {p_schema}.{p_table}
                                    WHERE {p_condition};
                                '''.format(
                                    p_schema=p_schema,
                                    p_table=p_table,
                                    p_condition=' AND '.join([
                                        '{p_column} = {p_value}::{p_type}'.format(
                                            p_column=v_column,
                                            p_value='$data_comparer${p_value}$data_comparer$'.format(p_value=str(v_row_1[v_column])) if v_row_1[v_column] is not None else 'NULL',
                                            p_type=v_column_type_dict[v_column]
                                        )
                                        for v_column in v_key
                                    ])
                                )
                            )
                        }
                    })

                    v_index_1 += 1

            if v_index_1 == len(v_table_1.Rows) and v_has_more_data_1:
                v_table_1 = p_database_1.QueryBlock(p_sql=v_sql, p_blocksize=p_block_size)
                v_index_1 = 0

            if v_index_2 == len(v_table_2.Rows) and v_has_more_data_2:
                v_table_2 = p_database_2.QueryBlock(p_sql=v_sql, p_blocksize=p_block_size)
                v_index_2 = 0

        p_database_1.Close(p_commit=False)
        p_database_2.Close(p_commit=False)
    finally:
        p_queue.put(None)
        p_is_sending_data_array[p_worker_index] = False


def get_compare_tables_data_tasks(p_database_1=None, p_database_2=None, p_block_size=None):
    """Get list of tasks that will compare tables data between databases.

        Args:
            p_database_1 (Spartacus.Database.PostgreSQL): the first database. Defaults to None.
            p_database_2 (Spartacus.Database.PostgreSQL): the second database. Defaults to None.
            p_block_size (int): Number of data records that the comparer will deal with at the same time. Defaults to None.

        Returns:
            list: list of tasks to be executed in a process pool. Each item is a dict instance with following strucutre:
                {
                    'function' (function): the function to be executed.
                    'kwds': keyworded args to be passed to the function.
                }

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

    v_sql = '''
        WITH select_tables AS (
            WITH parents AS (
                SELECT DISTINCT n.table_schema,
                                c.table_name
                FROM pg_inherits i
                INNER JOIN (
                    SELECT oid,
                           relnamespace,
                           QUOTE_IDENT(relname) AS table_name
                    FROM pg_class
                    WHERE relkind in (
                        'r',
                        'p'
                    )
                ) c
                        ON c.oid = i.inhparent
                INNER JOIN (
                    SELECT n.oid,
                           n.table_schema
                    FROM (
                        SELECT oid,
                               QUOTE_IDENT(nspname) AS table_schema
                        FROM pg_namespace
                    ) n
                    WHERE n.table_schema NOT IN (
                        'information_schema',
                        'pg_catalog',
                        'pg_toast'
                    )
                      AND n.table_schema NOT LIKE 'pg%%temp%%'
                ) n
                        ON n.oid = c.relnamespace
                INNER JOIN pg_class cc
                        ON cc.oid = i.inhrelid
                INNER JOIN pg_namespace nc
                        ON nc.oid = cc.relnamespace
            ),
            children AS (
                SELECT DISTINCT n.table_schema,
                                c.table_name
                FROM pg_inherits i
                INNER JOIN pg_class cp
                        ON i.inhparent = cp.oid
                INNER JOIN pg_namespace np
                        ON cp.relnamespace = np.oid
                INNER JOIN (
                    SELECT oid,
                           relnamespace,
                           QUOTE_IDENT(relname) AS table_name
                    FROM pg_class
                ) c
                        ON i.inhrelid = c.oid
                INNER JOIN (
                    SELECT n.oid,
                           n.table_schema
                    FROM (
                        SELECT oid,
                               QUOTE_IDENT(nspname) AS table_schema
                        FROM pg_namespace
                    ) n
                    WHERE n.table_schema NOT IN (
                        'information_schema',
                        'pg_catalog',
                        'pg_toast'
                    )
                      AND n.table_schema NOT LIKE 'pg%%temp%%'
                ) n
                        ON c.relnamespace = n.oid
            )
            SELECT n.table_schema,
                   c.table_name
            FROM (
                SELECT relnamespace,
                       QUOTE_IDENT(relname) AS table_name
                FROM pg_class
                WHERE relkind in (
                    'r',
                    'p'
                )
            ) c
            INNER JOIN (
                SELECT n.oid,
                       n.table_schema
                FROM (
                    SELECT oid,
                           QUOTE_IDENT(nspname) AS table_schema
                    FROM pg_namespace
                ) n
                WHERE n.table_schema NOT IN (
                    'information_schema',
                    'pg_catalog',
                    'pg_toast'
                )
                  AND n.table_schema NOT LIKE 'pg%%temp%%'
            ) n
                    ON c.relnamespace = n.oid
            LEFT JOIN parents p
                   ON c.table_name = p.table_name
                  AND n.table_schema = p.table_schema
            LEFT JOIN children ch
                   ON c.table_name = ch.table_name
                  AND n.table_schema = ch.table_schema
            WHERE ch.table_name IS NULL
            ORDER BY n.table_schema,
                     c.table_name
        ),
        select_pks AS (
            SELECT tc.table_schema,
                   tc.table_name,
                   STRING_AGG(kc.column_name, ',' ORDER BY kc.ordinal_position) AS column_names
            FROM (
                SELECT table_schema,
                       table_name,
                       constraint_name
                FROM information_schema.table_constraints
                WHERE constraint_type = 'PRIMARY KEY'
                  AND table_schema NOT IN (
                      'information_schema',
                      'pg_catalog',
                      'pg_toast'
                  )
                    AND table_schema NOT LIKE 'pg%%temp%%'
            ) tc
            INNER JOIN (
                SELECT table_schema,
                       table_name,
                       constraint_name,
                       QUOTE_IDENT(column_name) AS column_name,
                       ordinal_position
                FROM information_schema.key_column_usage
                WHERE table_schema NOT IN (
                    'information_schema',
                    'pg_catalog',
                    'pg_toast'
                )
                  AND table_schema NOT LIKE 'pg%%temp%%'
            ) kc
                    ON tc.table_name = kc.table_name
                   AND tc.table_schema = kc.table_schema
                   AND tc.constraint_name = kc.constraint_name
            GROUP BY tc.table_schema,
                     tc.table_name
        ),
        select_columns AS (
            SELECT table_schema,
                   table_name,
                   STRING_AGG(QUOTE_IDENT(column_name), ',' ORDER BY ordinal_position) AS column_names,
                   STRING_AGG(
                       FORMAT(
                           '%s(%s %s)',
                           QUOTE_IDENT(column_name),
                           data_type,
                           (CASE WHEN character_maximum_length IS NOT NULL
                                 THEN FORMAT(
                                          '(%s)',
                                          character_maximum_length
                                      )
                                 WHEN numeric_precision IS NOT NULL AND NULLIF(numeric_scale, 0) IS NOT NULL
                                 THEN FORMAT(
                                          '(%s, %s)',
                                          numeric_precision,
                                          numeric_scale
                                      )
                                 ELSE ''
                            END)
                       ),
                       ',' ORDER BY ordinal_position
                   ) AS columns_names_types
            FROM information_schema.columns
            WHERE table_schema NOT IN (
                'information_schema',
                'pg_catalog',
                'pg_toast'
            )
              AND table_schema NOT LIKE 'pg%%temp%%'
            GROUP BY table_schema,
                     table_name
        )
        SELECT st.table_schema,
               st.table_name,
               COALESCE(sp.column_names, sc.column_names) AS table_key,
               sc.columns_names_types
        FROM select_tables st
        LEFT JOIN select_pks sp
                ON st.table_schema = sp.table_schema
               AND st.table_name = sp.table_name
        INNER JOIN select_columns sc
                ON st.table_schema = sc.table_schema
               AND st.table_name = sc.table_name
        ORDER BY st.table_schema,
                 st.table_name
    '''

    def local_inserted_callback(p_row_list=None, p_columns=None, p_row=None, p_key=None):
        """Callback executed when a table is present just in second database.

            Args:
                p_row_list (list): list of rows representing tables wich data will be compared later. Defaults to None.
                p_columns (list): list of columns that are present in p_row parameter. Defaults to None.
                p_row (list): the row that was inserted in the database 2. Defaults to None.
                p_key (list): the key used for comparison. Defaults to None.

            Raises:
                custom_exceptions.InvalidParameterTypeException.
        """

        if not isinstance(p_row_list, list):
            raise custom_exceptions.InvalidParameterTypeException('"p_row_list" parameter must be a "list" instance.', p_row_list)

        if not isinstance(p_columns, list):
            raise custom_exceptions.InvalidParameterTypeException('"p_columns" parameter must be a "list" instance.', p_columns)

        if not isinstance(p_row, list):
            raise custom_exceptions.InvalidParameterTypeException('"p_row" parameter must be a "list" instance.', p_row)

        if not isinstance(p_key, list):
            raise custom_exceptions.InvalidParameterTypeException('"p_key" parameter must be a "list" instance.', p_key)

        p_row_list.append(p_row)

    def local_equal_callback(p_row_list=None, p_columns=None, p_row=None, p_key=None):
        """Callback executed when a table is present in both databases with the same columns.

            Args:
                p_row_list (list): list of rows representing tables wich data will be compared later. Defaults to None.
                p_columns (list): list of columns that are present in p_row parameter. Defaults to None.
                p_row (list): the row that present in both databases. Defaults to None.
                p_key (list): the key used for comparison. Defaults to None.

            Raises:
                custom_exceptions.InvalidParameterTypeException.
        """

        if not isinstance(p_row_list, list):
            raise custom_exceptions.InvalidParameterTypeException('"p_row_list" parameter must be a "list" instance.', p_row_list)

        if not isinstance(p_columns, list):
            raise custom_exceptions.InvalidParameterTypeException('"p_columns" parameter must be a "list" instance.', p_columns)

        if not isinstance(p_row, list):
            raise custom_exceptions.InvalidParameterTypeException('"p_row" parameter must be a "list" instance.', p_row)

        if not isinstance(p_key, list):
            raise custom_exceptions.InvalidParameterTypeException('"p_key" parameter must be a "list" instance.', p_key)

        p_row_list.append(p_row)

    v_row_list = []

    utils.compare_datatables(
        p_database_1=p_database_1,
        p_database_2=p_database_2,
        p_block_size=p_block_size,
        p_key=['table_schema', 'table_name'],
        p_sql=v_sql,
        p_inserted_callback=lambda p_columns, p_row, p_key: local_inserted_callback(p_row_list=v_row_list, p_columns=p_columns, p_row=p_row, p_key=p_key),
        p_equal_callback=lambda p_columns, p_row, p_key: local_equal_callback(p_row_list=v_row_list, p_columns=p_columns, p_row=p_row, p_key=p_key)
    )

    return [
        {
            'function': compare_tables_data,
            'kwds': {
                'p_schema': v_row['table_schema'],
                'p_table': v_row['table_name'],
                'p_key': v_row['table_key']
            }
        }
        for v_row in v_row_list
    ]
