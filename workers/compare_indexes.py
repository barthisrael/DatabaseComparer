import os
import inspect
import multiprocessing
import Spartacus.Database

from .import custom_exceptions
from .import utils


def inserted_callback(p_queue=None, p_columns=None, p_row=None, p_key=None):
    """Callback executed when a index was created in second database. Sends a row by queue to master process.

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
        'type': 'indexes',
        'row': [
            p_row['index_namespace'],
            p_row['index_name'],
            ','.join(p_key),
            'INSERTED',
            '',
            inspect.cleandoc(doc=p_row['create_index_ddl'])
        ]
    })


def updated_callback(p_queue=None, p_columns=None, p_row_1=None, p_row_2=None, p_key=None, p_all_diffs=None):
    """Callback executed when a index was updated in second database. Sends a row by queue to master process.

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

    p_queue.put({
        'type': 'indexes',
        'row': [
            p_row_2['index_namespace'],
            p_row_2['index_name'],
            ','.join(p_key),
            'UPDATED',
            ','.join(v_diff['column'] for v_diff in p_all_diffs),
            inspect.cleandoc(
                doc='''\
                    {p_drop}
                    {p_add}
                '''.format(
                    p_drop=p_row_2['drop_index_ddl'],
                    p_add=p_row_2['create_index_ddl']
                )
            )
        ]
    })


def deleted_callback(p_queue=None, p_columns=None, p_row=None, p_key=None):
    """Callback executed when a index was dropped from second database. Sends a row by queue to master process.

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
        'type': 'indexes',
        'row': [
            p_row['index_namespace'],
            p_row['index_name'],
            ','.join(p_key),
            'DELETED',
            '',
            inspect.cleandoc(doc=p_row['drop_index_ddl'])
        ]
    })


def compare_indexes(p_database_1=None, p_database_2=None, p_block_size=None, p_queue=None, p_is_sending_data_array=None, p_worker_index=None):
    """Used to compare indexes between databases.

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
            WITH ii AS (
                SELECT DISTINCT n1.nspname AS class_namespace,
                                c.relname AS class_name,
                                n2.nspname AS index_namespace,
                                i.relname AS index_name,
                                CASE d.refclassid WHEN 'pg_constraint'::regclass
                                                  THEN 'ALTER TABLE ' || TEXT(c.oid::regclass)
                                                       || ' ADD CONSTRAINT ' || quote_ident(cc.conname)
                                                       || ' ' || pg_get_constraintdef(cc.oid)
                                                  ELSE pg_get_indexdef(i.oid)
                                END AS indexdef
                FROM pg_index x
                JOIN pg_class c ON c.oid = x.indrelid
                JOIN pg_class i ON i.oid = x.indexrelid
                JOIN pg_depend d ON d.objid = x.indexrelid
                LEFT JOIN pg_constraint cc ON cc.oid = d.refobjid
                JOIN pg_namespace n1 ON c.relnamespace = n1.oid
                JOIN pg_namespace n2 ON i.relnamespace = n2.oid
                WHERE c.relkind in ('r','m') AND i.relkind = 'i'::"char"
                  AND n1.nspname NOT IN (
                      'information_schema',
                      'pg_catalog',
                      'pg_toast'
                  )
                  AND n1.nspname NOT LIKE 'pg%%temp%%'
                  AND n2.nspname NOT IN (
                      'information_schema',
                      'pg_catalog',
                      'pg_toast'
                  )
                  AND n2.nspname NOT LIKE 'pg%%temp%%'
            )
            SELECT index_namespace,
                   index_name,
                   indexdef || E';\n' AS create_index_ddl,
                   FORMAT('DROP INDEX %s.%s;', index_namespace, index_name) AS drop_index_ddl
            FROM ii
            WHERE indexdef LIKE 'CREATE%INDEX%'
        '''

        utils.compare_datatables(
            p_database_1=p_database_1,
            p_database_2=p_database_2,
            p_block_size=p_block_size,
            p_key=['index_namespace', 'index_name'],
            p_sql=v_sql,
            p_inserted_callback=lambda p_columns, p_row, p_key: inserted_callback(p_queue=p_queue, p_columns=p_columns, p_row=p_row, p_key=p_key),
            p_updated_callback=lambda p_columns, p_row_1, p_row_2, p_key, p_all_diffs: updated_callback(p_queue=p_queue, p_columns=p_columns, p_row_1=p_row_1, p_row_2=p_row_2, p_key=p_key, p_all_diffs=p_all_diffs),
            p_deleted_callback=lambda p_columns, p_row, p_key: deleted_callback(p_queue=p_queue, p_columns=p_columns, p_row=p_row, p_key=p_key)
        )
    finally:
        p_queue.put(None)
        p_is_sending_data_array[p_worker_index] = False


def get_compare_indexes_tasks():
    """Get list of tasks that will compare indexes between databases.

        Args:

        Returns:
            list: list of tasks to be executed in a process pool. Each item is a dict instance with following strucutre:
                {
                    'function' (function): the function to be executed.
                    'kwds': keyworded args to be passed to the function.
                }
    """

    return [{
        'function': compare_indexes,
        'kwds': {}
    }]
