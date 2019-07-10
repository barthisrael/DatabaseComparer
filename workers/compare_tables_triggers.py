import os
import inspect
import multiprocessing
import Spartacus.Database

from .import custom_exceptions
from .import utils


def inserted_callback(p_queue=None, p_columns=None, p_row=None, p_key=None):
    """Callback executed when a table trigger was created in second database. Sends a row by queue to master process.

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
        'type': 'tables_triggers',
        'row': [
            p_row['schema_name'],
            p_row['table_name'],
            p_row['trigger_name'],
            ','.join(p_key),
            'INSERTED',
            '',
            inspect.cleandoc(doc=p_row['create_trigger_ddl'])
        ]
    })


def updated_callback(p_queue=None, p_columns=None, p_row_1=None, p_row_2=None, p_key=None, p_all_diffs=None):
    """Callback executed when a table trigger was updated in second database. Sends a row by queue to master process.

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
        if v_diff['column'] == 'trigger_enabled':
            p_queue.put({
                'type': 'tables_triggers',
                'row': [
                    p_row_2['schema_name'],
                    p_row_2['table_name'],
                    p_row_2['trigger_name'],
                    ','.join(p_key),
                    'UPDATED',
                    v_diff['column'],
                    inspect.cleandoc(
                        doc='''\
                            ALTER TABLE {p_schema}.{p_table}
                            {p_operation} TRIGGER {p_trigger};
                        '''.format(
                            p_schema=p_row_2['schema_name'],
                            p_table=p_row_2['table_name'],
                            p_operation='ENABLE' if p_row_2['trigger_enabled'] == 'O' else 'DISABLE',
                            p_trigger=p_row_2['trigger_name']
                        )
                    )
                ]
            })
        elif v_diff['column'] == 'trigger_definition':
            p_queue.put({
                'type': 'tables_triggers',
                'row': [
                    p_row_2['schema_name'],
                    p_row_2['table_name'],
                    p_row_2['trigger_name'],
                    ','.join(p_key),
                    'UPDATED',
                    v_diff['column'],
                    inspect.cleandoc(
                        doc='''\
                            {p_drop}
                            {p_create}
                        '''.format(
                            p_drop=p_row_2['create_trigger_ddl'],
                            p_create=p_row_2['drop_trigger_ddl']
                        )
                    )
                ]
            })


def deleted_callback(p_queue=None, p_columns=None, p_row=None, p_key=None):
    """Callback executed when a table trigger was dropped from second database. Sends a row by queue to master process.

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
        'type': 'tables_triggers',
        'row': [
            p_row['schema_name'],
            p_row['table_name'],
            p_row['trigger_name'],
            ','.join(p_key),
            'DELETED',
            '',
            inspect.cleandoc(doc=p_row['drop_trigger_ddl'])
        ]
    })


def compare_tables_triggers(p_database_1=None, p_database_2=None, p_block_size=None, p_queue=None, p_is_sending_data_array=None, p_worker_index=None):
    """Used to compare tables triggers between databases.

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
            SELECT QUOTE_IDENT(n.nspname) AS schema_name,
                   QUOTE_IDENT(c.relname) AS table_name,
                   QUOTE_IDENT(t.tgname) AS trigger_name,
                   t.tgenabled AS trigger_enabled,
                   PG_GET_TRIGGERDEF(t.oid) AS trigger_definition,
                   FORMAT('%s;', PG_GET_TRIGGERDEF(t.oid)) AS create_trigger_ddl,
                   FORMAT(
                       'DROP TRIGGER %s ON %s.%s;',
                       QUOTE_IDENT(t.tgname),
                       QUOTE_IDENT(n.nspname),
                       QUOTE_IDENT(c.relname)
                   ) AS drop_trigger_ddl
            FROM pg_trigger t
            INNER JOIN pg_class c
                    ON c.oid = t.tgrelid
            INNER JOIN pg_namespace n
                    ON n.oid = c.relnamespace
            INNER JOIN pg_proc p
                    ON p.oid = t.tgfoid
            INNER JOIN pg_namespace np
                    ON np.oid = p.pronamespace
            WHERE NOT t.tgisinternal
              AND n.nspname NOT IN (
                  'information_schema',
                  'pg_catalog',
                  'pg_toast'
              )
              AND n.nspname NOT LIKE 'pg%%temp%%'
            ORDER BY 1,
                     2,
                     3
        '''

        utils.compare_datatables(
            p_database_1=p_database_1,
            p_database_2=p_database_2,
            p_block_size=p_block_size,
            p_key=['schema_name', 'table_name', 'trigger_name'],
            p_sql=v_sql,
            p_inserted_callback=lambda p_columns, p_row, p_key: inserted_callback(p_queue=p_queue, p_columns=p_columns, p_row=p_row, p_key=p_key),
            p_updated_callback=lambda p_columns, p_row_1, p_row_2, p_key, p_all_diffs: updated_callback(p_queue=p_queue, p_columns=p_columns, p_row_1=p_row_1, p_row_2=p_row_2, p_key=p_key, p_all_diffs=p_all_diffs),
            p_deleted_callback=lambda p_columns, p_row, p_key: deleted_callback(p_queue=p_queue, p_columns=p_columns, p_row=p_row, p_key=p_key)
        )
    finally:
        p_queue.put(None)
        p_is_sending_data_array[p_worker_index] = False


def get_compare_tables_triggers_tasks():
    """Get list of tasks that will compare tables triggers between databases.

        Args:

        Returns:
            list: list of tasks to be executed in a process pool. Each item is a dict instance with following strucutre:
                {
                    'function' (function): the function to be executed.
                    'kwds': keyworded args to be passed to the function.
                }
    """

    return [{
        'function': compare_tables_triggers,
        'kwds': {}
    }]
