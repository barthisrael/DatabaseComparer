import os
import inspect
import multiprocessing
import Spartacus.Database

from .import custom_exceptions
from .import utils


def inserted_callback(p_queue=None, p_columns=None, p_row=None, p_key=None):
    """Callback executed when a table column was created in second database. Sends a row by queue to master process.

        Args:
            p_queue (multiprocessing.managers.BaseProxy): queue used to communicate to parent process. Created from a multiprocessing.Manager instance. Defaults to None.
            p_columns (list): list of columns that are present in p_row parameter.
            p_row (list): the row that was inserted in the database 2.
            p_key (list): the key used for comparison.

        Raises:
            custom_exceptions.InvalidParameterTypeException.
    """

    if not isinstance(p_queue, multiprocessing.managers.BaseProxy):
        raise custom_exceptions.InvalidParameterTypeException('"p_queue" parameter must be a "multiprocessing.managers.BaseProxy" instance.', p_queue)

    if not isinstance(p_columns, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_columns" parameter must be a "list" instance.', p_columns)

    if not isinstance(p_row, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_row" parameter must be a "list" instance.', p_row)

    if not isinstance(p_key, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_key" parameter must be a "list" instance.', p_key)

    p_queue.put({
        'type': 'tables_columns',
        'row': [
            p_row['table_schema'],
            p_row['table_name'],
            p_row['column_name'],
            ','.join(p_key),
            'INSERTED',
            '',
            inspect.cleandoc(doc=p_row['add_column_ddl'])
        ]
    })


def updated_callback(p_queue=None, p_columns=None, p_row_1=None, p_row_2=None, p_key=None, p_all_diffs=None):
    """Callback executed when a table column was updated in second database. Sends a row by queue to master process.

        Args:
            p_queue (multiprocessing.managers.BaseProxy): queue used to communicate to parent process. Created from a multiprocessing.Manager instance. Defaults to None.
            p_columns (list): list of columns that are present in p_row_1 and p_row_2 parameters.
            p_row_1 (list): the row as it is in database 1.
            p_row_2 (list): the row as it is in database 2.
            p_key (list): the key used for comparison.
            p_all_diffs (list): list of diffs. Each item has the following structure:
                {
                    'column' (str): the column that differs,
                    'old_value' (object): value in database 1,
                    'new_value' (object): value in database 2.
                }

        Raises:
            custom_exceptions.InvalidParameterTypeException.
    """

    if not isinstance(p_queue, multiprocessing.managers.BaseProxy):
        raise custom_exceptions.InvalidParameterTypeException('"p_queue" parameter must be a "multiprocessing.managers.BaseProxy" instance.', p_queue)

    if not isinstance(p_columns, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_columns" parameter must be a "list" instance.', p_columns)

    if not isinstance(p_row_1, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_row_1" parameter must be a "list" instance.', p_row_1)

    if not isinstance(p_row_2, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_row_2" parameter must be a "list" instance.', p_row_2)

    if not isinstance(p_key, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_key" parameter must be a "list" instance.', p_key)

    if not isinstance(p_all_diffs, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_all_diffs" parameter must be a "list" instance.', p_all_diffs)

    for v_diff in p_all_diffs:
        if v_diff['column'] == 'data_type':
            p_queue.put({
                'type': 'tables_columns',
                'row': [
                    p_row_2['table_schema'],
                    p_row_2['table_name'],
                    p_row_2['column_name'],
                    ','.join(p_key),
                    'UPDATED',
                    v_diff['column'],
                    inspect.cleandoc(
                        doc='''\
                            ALTER TABLE {p_schema}.{p_table}
                            ALTER COLUMN {p_column} TYPE {p_type};
                        '''.format(
                            p_schema=p_row_2['table_schema'],
                            p_table=p_row_2['table_name'],
                            p_column=p_row_2['column_name'],
                            p_type=p_row_2['data_type']
                        )
                    )
                ]
            })
        elif v_diff['column'] == 'not_null':
            p_queue.put({
                'type': 'tables_columns',
                'row': [
                    p_row_2['table_schema'],
                    p_row_2['table_name'],
                    p_row_2['column_name'],
                    ','.join(p_key),
                    'UPDATED',
                    v_diff['column'],
                    inspect.cleandoc(
                        doc='''\
                            ALTER TABLE {p_schema}.{p_table}
                            ALTER COLUMN {p_column} {p_operation} NOT NULL;
                        '''.format(
                            p_schema=p_row_2['table_schema'],
                            p_table=p_row_2['table_name'],
                            p_column=p_row_2['column_name'],
                            p_nullable='SET' if p_row_2['not_null'] else 'DROP'
                        )
                    )
                ]
            })
        elif v_diff['column'] == 'column_default':
            p_queue.put({
                'type': 'tables_columns',
                'row': [
                    p_row_2['table_schema'],
                    p_row_2['table_name'],
                    p_row_2['column_name'],
                    ','.join(p_key),
                    'UPDATED',
                    v_diff['column'],
                    inspect.cleandoc(
                        doc='''\
                            ALTER TABLE {p_schema}.{p_table}
                            ALTER COLUMN {p_column} {p_default};
                        '''.format(
                            p_schema=p_row_2['table_schema'],
                            p_table=p_row_2['table_name'],
                            p_column=p_row_2['column_name'],
                            p_default='SET DEFAULT {p_default}'.format(p_default=p_row_2['column_default']) if p_row_2['column_default'] is not None else 'DROP DEFAULT'
                        )
                    )
                ]
            })


def deleted_callback(p_queue=None, p_columns=None, p_row=None, p_key=None):
    """Callback executed when a table column was dropped from second database. Sends a row by queue to master process.

        Args:
            p_queue (multiprocessing.managers.BaseProxy): queue used to communicate to parent process. Created from a multiprocessing.Manager instance. Defaults to None.
            p_columns (list): list of columns that are present in p_row parameter.
            p_row (list): the row that was inserted in the database 2.
            p_key (list): the key used for comparison.

        Raises:
            custom_exceptions.InvalidParameterTypeException.
    """

    if not isinstance(p_queue, multiprocessing.managers.BaseProxy):
        raise custom_exceptions.InvalidParameterTypeException('"p_queue" parameter must be a "multiprocessing.managers.BaseProxy" instance.', p_queue)

    if not isinstance(p_columns, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_columns" parameter must be a "list" instance.', p_columns)

    if not isinstance(p_row, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_row" parameter must be a "list" instance.', p_row)

    if not isinstance(p_key, list):
        raise custom_exceptions.InvalidParameterTypeException('"p_key" parameter must be a "list" instance.', p_key)

    p_queue.put({
        'type': 'tables_columns',
        'row': [
            p_row['table_schema'],
            p_row['table_name'],
            p_row['column_name'],
            ','.join(p_key),
            'DELETED',
            '',
            inspect.cleandoc(doc=p_row['drop_column_ddl'])
        ]
    })


def compare_tables_columns(p_database_1=None, p_database_2=None, p_block_size=None, p_queue=None, p_is_sending_data_array=None, p_worker_index=None):
    """Used to compare tables columns between databases.

        Args:
            p_database_1 (Spartacus.Database.PostgreSQL): the first database. Defaults to None.
            p_database_2 (Spartacus.Database.PostgreSQL): the second database. Defaults to None.
            p_block_size (int): Number of data records that the comparer will deal with at the same time. Defaults to None.
            p_queue (multiprocessing.managers.BaseProxy): queue used to communicate to parent process. Created from a multiprocessing.Manager instance. Defaults to None.
            p_is_sending_data_array (multiprocessing.managers.ArrayProxy): array used to control process that are still sending data. Defaults to None.
            p_worker_index (int): the worker sub process index. Defaults to None.

        Raises:
            custom_exceptions.InvalidParameterTypeException.
            custom_exceptions.InvalidParameterValueException
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

        if not isinstance(p_queue, multiprocessing.managers.BaseProxy):
            raise custom_exceptions.InvalidParameterTypeException('"p_queue" parameter must be a "multiprocessing.managers.BaseProxy" instance.', p_queue)

        if not isinstance(p_is_sending_data_array, multiprocessing.managers.ArrayProxy):
            raise custom_exceptions.InvalidParameterTypeException('"p_is_sending_data_array" parameter must be an "multiprocessing.managers.ArrayProxy" instance.', p_is_sending_data_array)

        if not isinstance(p_worker_index, int):
            raise custom_exceptions.InvalidParameterTypeException('"p_worker_index" parameter must be an "int" instance.', p_worker_index)

        if p_worker_index < 0:
            raise custom_exceptions.InvalidParameterTypeException('"p_worker_index" parameter must be an "int" instance greater than or equal to 0.', p_worker_index)

        #Prepare table query
        v_sql = '''\
            SELECT QUOTE_IDENT(s.nspname) AS table_schema,
                   QUOTE_IDENT(c.relname) as table_name,
                   QUOTE_IDENT(a.attname) as column_name,
                   FORMAT_TYPE(t.oid, a.atttypmod) AS data_type,
                   a.attnotnull AS not_null,
                   def.adsrc AS column_default,
                   FORMAT(
                       'ALTER TABLE %s.%s ADD COLUMN %I %s%s%s;',
                       QUOTE_IDENT(s.nspname),
                       QUOTE_IDENT(c.relname),
                       a.attname::text,
                       FORMAT_TYPE(t.oid, a.atttypmod),
                	   CASE WHEN LENGTH(col.collcollate) > 0
                      	    THEN ' COLLATE ' || QUOTE_IDENT(col.collcollate::text)
                            ELSE ''
                       END,
                       CASE WHEN a.attnotnull
                            THEN ' NOT NULL'::text
                            ELSE ''::text
                       END
                   ) AS add_column_ddl,
                   FORMAT(
                       'ALTER TABLE %s.%s DROP COLUMN %I;',
                       QUOTE_IDENT(s.nspname),
                       QUOTE_IDENT(c.relname),
                       a.attname::text
                   ) AS drop_column_ddl
            FROM pg_class c
            JOIN (
                SELECT oid,
                       nspname
                FROM pg_namespace
                WHERE nspname NOT IN (
                    'information_schema',
                    'pg_catalog',
                    'pg_toast'
                )
                  AND nspname NOT LIKE 'pg%%temp%%'
            ) s ON s.oid = c.relnamespace
            JOIN pg_attribute a ON c.oid = a.attrelid
            LEFT JOIN pg_attrdef def ON c.oid = def.adrelid AND a.attnum = def.adnum
            LEFT JOIN pg_constraint con ON con.conrelid = c.oid AND (a.attnum = ANY (con.conkey)) AND con.contype = 'p'
            LEFT JOIN pg_type t ON t.oid = a.atttypid
            LEFT JOIN pg_collation col ON col.oid = a.attcollation
            JOIN pg_namespace tn ON tn.oid = t.typnamespace
            WHERE c.relkind IN ('r','p') AND a.attnum > 0 AND NOT a.attisdropped
              AND has_table_privilege(c.oid, 'select') AND has_schema_privilege(s.oid, 'usage')
            ORDER BY QUOTE_IDENT(s.nspname), QUOTE_IDENT(c.relname), QUOTE_IDENT(a.attname)
        '''

        utils.compare_datatables(
            p_database_1=p_database_1,
            p_database_2=p_database_2,
            p_block_size=p_block_size,
            p_key=['table_schema', 'table_name', 'column_name'],
            p_sql=v_sql,
            p_inserted_callback=lambda p_columns, p_row, p_key: inserted_callback(p_queue=p_queue, p_columns=p_columns, p_row=p_row, p_key=p_key),
            p_updated_callback=lambda p_columns, p_row_1, p_row_2, p_key, p_all_diffs: updated_callback(p_queue=p_queue, p_columns=p_columns, p_row_1=p_row_1, p_row_2=p_row_2, p_key=p_key, p_all_diffs=p_all_diffs),
            p_deleted_callback=lambda p_columns, p_row, p_key: deleted_callback(p_queue=p_queue, p_columns=p_columns, p_row=p_row, p_key=p_key)
        )
    finally:
        p_queue.put(None)
        p_is_sending_data_array[p_worker_index] = False


def get_compare_tables_columns_tasks():
    """Get list of tasks that will compare tables columns between databases.

        Args:

        Returns:
            list: list of tasks to be executed in a process pool. Each item is a dict instance with following strucutre:
                {
                    'function' (function): the function to be executed.
                    'kwds': keyworded args to be passed to the function.
                }
    """

    return [{
        'function': compare_tables_columns,
        'kwds': {}
    }]
