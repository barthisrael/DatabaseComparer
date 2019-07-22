import traceback
import argparse
import inspect
import Spartacus.Database
import multiprocessing
import queue

import workers.custom_exceptions
import workers.compare_functions
import workers.compare_indexes
import workers.compare_mviews
import workers.compare_procedures
import workers.compare_schemas
import workers.compare_sequences
import workers.compare_tables_checks
import workers.compare_tables_columns
import workers.compare_tables_data
import workers.compare_tables_excludes
import workers.compare_tables_fks
import workers.compare_tables_pks
import workers.compare_tables_rules
import workers.compare_tables_triggers
import workers.compare_tables_uniques
import workers.compare_tables
import workers.compare_trigger_functions
import workers.compare_views


def get_output_sql(p_type=None, p_row=None):
    """Get sql to insert in report table.

        Args:
            p_type (str): the type of the row that will be inserted into output database. Defaults to None.
                Notes: must be one of:
                    - functions
                    - indexes
                    - mviews
                    - procedures
                    - schemas
                    - sequences
                    - tables_checks
                    - tables_columns
                    - tables_data
                    - tables_excludes
                    - tables_fks
                    - tables_pks
                    - tables_rules
                    - tables_triggers
                    - tables_uniques
                    - tables
                    - trigger_functions
                    - views
            p_row (dict): key/value pairs of values to be inserted into output database. Defaults to None.

        Returns:
            str: SQL to be executed.

        Raises:
            workers.custom_exceptions.InvalidParameterTypeException.
            workers.custom_exceptions.InvalidParameterValueException.
    """

    if not isinstance(p_type, str):
        raise workers.custom_exceptions.InvalidParameterTypeException('"p_type" parameter must be a "str" instance.', p_type)

    if p_type not in ['functions', 'indexes', 'mviews', 'procedures', 'schemas', 'sequences', 'tables_checks', 'tables_columns', 'tables_data', 'tables_exludes', 'tables_fks', 'tables_pks', 'tables_rules', 'tables_triggers', 'tables_uniques', 'tables', 'trigger_functions', 'views']:
        raise workers.custom_exceptions.InvalidParameterValueException(
            '"p_type" parameter must be one between: functions, indexes, mviews, procedures, schemas, sequences, tables_checks, tables_columns, tables_data, tables_exludes, tables_fks, tables_pks, tables_rules, tables_triggers, tables_uniques, tables, trigger_functions, views.',
            p_type
        )

    if not isinstance(p_row, dict):
        raise workers.custom_exceptions.InvalidParameterTypeException('"p_row" parameter must be a "dict" instance.', p_row)

    v_sql = None

    if p_type == 'functions':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_function_id := '{p_function_id}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_function_id=p_row['function_id'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'indexes':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_index_name := '{p_index_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_index_name=p_row['index_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'mviews':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_mview_name := '{p_mview_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_mview_name=p_row['mview_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'procedures':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_function_id := '{p_function_id}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_function_id=p_row['function_id'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'schemas':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'sequences':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_sequence_name := '{p_sequence_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_sequence_name=p_row['sequence_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'tables_checks':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_table_name := '{p_table_name}',
                p_constraint_name := '{p_constraint_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_table_name=p_row['table_name'],
            p_constraint_name=p_row['constraint_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'tables_columns':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_table_name := '{p_table_name}',
                p_column_name := '{p_column_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_table_name=p_row['table_name'],
            p_column_name=p_row['column_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'tables_data':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_table_name := '{p_table_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_table_name=p_row['table_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'tables_exludes':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_table_name := '{p_table_name}',
                p_constraint_name := '{p_constraint_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_table_name=p_row['table_name'],
            p_constraint_name=p_row['constraint_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'tables_fks':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_table_name := '{p_table_name}',
                p_constraint_name := '{p_constraint_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_table_name=p_row['table_name'],
            p_constraint_name=p_row['constraint_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'tables_pks':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_table_name := '{p_table_name}',
                p_constraint_name := '{p_constraint_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_table_name=p_row['table_name'],
            p_constraint_name=p_row['constraint_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'tables_rules':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_table_name := '{p_table_name}',
                p_constraint_name := '{p_constraint_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_table_name=p_row['table_name'],
            p_constraint_name=p_row['constraint_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'tables_triggers':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_table_name := '{p_table_name}',
                p_trigger_name := '{p_trigger_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_table_name=p_row['table_name'],
            p_trigger_name=p_row['trigger_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'tables_uniques':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_table_name := '{p_table_name}',
                p_constraint_name := '{p_constraint_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_table_name=p_row['table_name'],
            p_constraint_name=p_row['constraint_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'tables':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_table_name := '{p_table_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_table_name=p_row['table_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'trigger_functions':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_function_id := '{p_function_id}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_function_id=p_row['function_id'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )
    elif p_type == 'views':
        v_sql = '''
            SELECT database_comparer_report.output_report_fnc_add (
                p_category := '{p_category}',
                p_schema_name := '{p_schema_name}',
                p_view_name := '{p_view_name}',
                p_status := '{p_status}',
                p_sql := $output_report_sql${p_sql}$output_report_sql$
            )
        '''.format(
            p_category=p_type,
            p_schema_name=p_row['schema_name'],
            p_view_name=p_row['view_name'],
            p_status=p_row['status'],
            p_sql=p_row['sql']
        )

    return v_sql


def consumer_worker(p_output_database=None, p_block_size=None, p_queue=None, p_is_sending_data_array=None):
    """Worker in charge of getting changes pointed by producer workers and insert such information into the output database.

        Args:
            p_output_database (Spartacus.Database.PostgreSQL): the output database. Defaults to None.
            p_block_size (int): number of data records that the consumer will insert at a time. Defaults to None.
            p_queue (multiprocessing.managers.BaseProxy): queue used to communicate to parent process. Created from a multiprocessing.Manager instance. Defaults to None.
            p_is_sending_data_array (multiprocessing.managers.ArrayProxy): array used to control process that are still sending data. Defaults to None.

        Raises:
            custom_exceptions.InvalidParameterTypeException.
            custom_exceptions.InvalidParameterValueException.
    """

    if not isinstance(p_output_database, Spartacus.Database.PostgreSQL):
        raise workers.custom_exceptions.InvalidParameterTypeException('"p_output_database" parameter must be a "Spartacus.Database.PostgreSQL" instance.', p_output_database)

    if not isinstance(p_block_size, int):
        raise custom_exceptions.InvalidParameterTypeException('"p_block_size" parameter must be an "int" instance.', p_block_size)

    if p_block_size < 1:
        raise custom_exceptions.InvalidParameterValueException('"p_block_size" parameter must be a positive "int" instance.', p_block_size)

    if not isinstance(p_queue, multiprocessing.managers.BaseProxy):
        raise custom_exceptions.InvalidParameterTypeException('"p_queue" parameter must be a "multiprocessing.managers.BaseProxy" instance.', p_queue)

    if not isinstance(p_is_sending_data_array, multiprocessing.managers.ArrayProxy):
        raise custom_exceptions.InvalidParameterTypeException('"p_is_sending_data_array" parameter must be an "multiprocessing.managers.ArrayProxy" instance.', p_is_sending_data_array)

    v_sql_list = []

    p_output_database.Open(p_autocommit=True)

    #While any task is still active or has data to be read, try to get data
    #Used or because processes can be done but queue still have data or processes still executing and queue with no data
    while any(p_is_sending_data_array) or p_queue.qsize() > 0:
        v_data = None

        try:
            v_data = p_queue.get_nowait()
        except queue.Empty:
            pass

        if v_data is not None:
            v_sql_list.append(
                get_output_sql(
                    p_type=v_data['type'],
                    p_row=v_data['row']
                )
            )

            if len(v_sql_list) == p_block_size:
                p_output_database.Execute(p_sql=';'.join(v_sql_list))
                v_sql_list = []

    if len(v_sql_list) > 0:
        p_output_database.Execute(p_sql=';'.join(v_sql_list))

    p_output_database.Close(p_commit=True)


if __name__ == '__main__':
    try:
        v_parser = argparse.ArgumentParser(
            epilog=inspect.cleandoc(
                doc='''\
                    Script used to compare databases and find the differences between them, if any.
                    Found differences will be written to a given report database connection, in the table "database_comparer_report.output_report".
                    The differences considers commands to get in target from source database.
                '''
            )
        )

        v_parser.add_argument(
            '-b',
            '--block-size',
            dest='block_size',
            help='Number of data records that the comparer will deal with at the same time in each opened process. Used to reduce memory usage. Also points number of records inserted at a time in the consumer workers.',
            type=int,
            required=True
        )

        v_parser.add_argument(
            '-e',
            '--exclude-tables',
            dest='exclude_tables',
            help='List of tables to be excluded from tables data comparisons. Will still be considered for tables structure comparison. Example: --exclude-tables {public.table_1, my_schema.table_2}.',
            type=str,
            default=[],
            nargs='+',
            required=False
        )

        v_parser.add_argument(
            '-s',
            '--source-database-connection',
            dest='source_database_connection',
            help='Connection string to the source database of comparison. Has the following structure: HOST:PORT:DATABASE:USER:PASSWORD. You can leave password empty if it is already in you .pgpass file.',
            type=str,
            required=True
        )

        v_parser.add_argument(
            '-t',
            '--target-database-connection',
            dest='target_database_connection',
            help='Connection string to the target database of comparison. Has the following structure: HOST:PORT:DATABASE:USER:PASSWORD. You can leave password empty if it is already in you .pgpass file.',
            type=str,
            required=True
        )

        v_parser.add_argument(
            '-o',
            '--output-database-connection',
            dest='output_database_connection',
            help='Connection string to the report database. Has the following structure: HOST:PORT:DATABASE:USER:PASSWORD. You can leave password empty if it is already in you .pgpass file.',
            type=str,
            required=True
        )

        v_options = v_parser.parse_args()

        #Get databases credentials
        v_source_params = v_options.source_database_connection.split(':')
        v_target_params = v_options.target_database_connection.split(':')
        v_output_params = v_options.output_database_connection.split(':')

        v_output_database = Spartacus.Database.PostgreSQL(
            p_host=v_output_params[0],
            p_port=v_output_params[1],
            p_service=v_output_params[2],
            p_user=v_output_params[3],
            p_password=v_output_params[4],
            p_application_name='compare_databases'
        )

        #Open output database and create output structure
        v_output_database.Open(p_autocommit=True)

        v_output_database.Execute(
            p_sql='''
                CREATE SCHEMA IF NOT EXISTS database_comparer_report;
            '''
        )

        v_output_database.Execute(
            p_sql='''
                CREATE TABLE IF NOT EXISTS database_comparer_report.output_report (
                    id SERIAL NOT NULL PRIMARY KEY,
                    category TEXT NOT NULL,
                    schema_name TEXT,
                    table_name TEXT,
                    column_name TEXT,
                    constraint_name TEXT,
                    trigger_name TEXT,
                    index_name TEXT,
                    sequence_name TEXT,
                    view_name TEXT,
                    mview_name TEXT,
                    function_id TEXT,
                    status TEXT,
                    sql TEXT
                );
            '''
        )

        v_output_database.Execute(
            p_sql='''
                TRUNCATE database_comparer_report.output_report
            '''
        )

        v_output_database.Execute(
            p_sql='''
                CREATE OR REPLACE FUNCTION database_comparer_report.output_report_fnc_add (
                    p_category TEXT DEFAULT NULL::TEXT,
                    p_schema_name TEXT DEFAULT NULL::TEXT,
                    p_table_name TEXT DEFAULT NULL::TEXT,
                    p_column_name TEXT DEFAULT NULL::TEXT,
                    p_constraint_name TEXT DEFAULT NULL::TEXT,
                    p_trigger_name TEXT DEFAULT NULL::TEXT,
                    p_index_name TEXT DEFAULT NULL::TEXT,
                    p_sequence_name TEXT DEFAULT NULL::TEXT,
                    p_view_name TEXT DEFAULT NULL::TEXT,
                    p_mview_name TEXT DEFAULT NULL::TEXT,
                    p_function_id TEXT DEFAULT NULL::TEXT,
                    p_status TEXT DEFAULT NULL::TEXT,
                    p_sql TEXT DEFAULT NULL::TEXT
                )
                RETURNS INTEGER
                LANGUAGE plpgsql
                AS
                $function$
                DECLARE
                    v_id INTEGER;
                BEGIN
                    INSERT INTO database_comparer_report.output_report (
                        category,
                        schema_name,
                        table_name,
                        column_name,
                        constraint_name,
                        trigger_name,
                        index_name,
                        sequence_name,
                        view_name,
                        mview_name,
                        function_id,
                        status,
                        sql
                    ) VALUES (
                        p_category,
                        p_schema_name,
                        p_table_name,
                        p_column_name,
                        p_constraint_name,
                        p_trigger_name,
                        p_index_name,
                        p_sequence_name,
                        p_view_name,
                        p_mview_name,
                        p_function_id,
                        p_status,
                        p_sql
                    )
                    RETURNING id INTO v_id;

                    RETURN v_id;
                END;
                $function$
            '''
        )

        v_output_database.Close(p_commit=True)

        #Open a process pool for producers and create tasks to be run in parallel
        v_producers_process_pool = multiprocessing.Pool(multiprocessing.cpu_count())
        v_producers_result_list = []
        v_producers_task_list = []

        v_producers_task_list += workers.compare_functions.get_compare_functions_tasks()
        v_producers_task_list += workers.compare_indexes.get_compare_indexes_tasks()
        v_producers_task_list += workers.compare_mviews.get_compare_mviews_tasks()
        v_producers_task_list += workers.compare_procedures.get_compare_procedures_tasks()
        v_producers_task_list += workers.compare_schemas.get_compare_schemas_tasks()
        v_producers_task_list += workers.compare_sequences.get_compare_sequences_tasks()
        v_producers_task_list += workers.compare_tables_checks.get_compare_tables_checks_tasks()
        v_producers_task_list += workers.compare_tables_columns.get_compare_tables_columns_tasks()

        v_producers_task_list += workers.compare_tables_data.get_compare_tables_data_tasks(
            p_database_1=Spartacus.Database.PostgreSQL(
                p_host=v_source_params[0],
                p_port=v_source_params[1],
                p_service=v_source_params[2],
                p_user=v_source_params[3],
                p_password=v_source_params[4],
                p_application_name='compare_databases'
            ),
            p_database_2=Spartacus.Database.PostgreSQL(
                p_host=v_target_params[0],
                p_port=v_target_params[1],
                p_service=v_target_params[2],
                p_user=v_target_params[3],
                p_password=v_target_params[4],
                p_application_name='compare_databases'
            ),
            p_block_size=v_options.block_size,
            p_exclude_tables=v_options.exclude_tables
        )

        v_producers_task_list += workers.compare_tables_excludes.get_compare_tables_excludes_tasks()
        v_producers_task_list += workers.compare_tables_fks.get_compare_tables_fks_tasks()
        v_producers_task_list += workers.compare_tables_pks.get_compare_tables_pks_tasks()
        v_producers_task_list += workers.compare_tables_rules.get_compare_tables_rules_tasks()
        v_producers_task_list += workers.compare_tables_triggers.get_compare_tables_triggers_tasks()
        v_producers_task_list += workers.compare_tables_uniques.get_compare_tables_uniques_tasks()
        v_producers_task_list += workers.compare_tables.get_compare_tables_tasks()
        v_producers_task_list += workers.compare_trigger_functions.get_compare_trigger_functions_tasks()
        v_producers_task_list += workers.compare_views.get_compare_views_tasks()

        v_manager = multiprocessing.Manager()
        v_queue = v_manager.Queue()
        v_is_sending_data_array = v_manager.Array('b', [True] * len(v_producers_task_list))

        #Open a process pool for consumers and create tasks to be run in parallel
        v_consumers_process_pool = multiprocessing.Pool(multiprocessing.cpu_count())
        v_consumers_result_list = []

        for i in range(multiprocessing.cpu_count()):
            v_consumers_result_list.append(
                v_consumers_process_pool.apply_async(
                    func=consumer_worker,
                    kwds={
                        'p_output_database': Spartacus.Database.PostgreSQL(
                            p_host=v_output_params[0],
                            p_port=v_output_params[1],
                            p_service=v_output_params[2],
                            p_user=v_output_params[3],
                            p_password=v_output_params[4],
                            p_application_name='compare_databases'
                        ),
                        'p_block_size': v_options.block_size,
                        'p_queue': v_queue,
                        'p_is_sending_data_array': v_is_sending_data_array
                    }
                )
            )

        for i in range(len(v_producers_task_list)):
            v_task = v_producers_task_list[i]

            v_task['kwds']['p_database_1'] = Spartacus.Database.PostgreSQL(
                p_host=v_source_params[0],
                p_port=v_source_params[1],
                p_service=v_source_params[2],
                p_user=v_source_params[3],
                p_password=v_source_params[4],
                p_application_name='compare_databases'
            )

            v_task['kwds']['p_database_2'] = Spartacus.Database.PostgreSQL(
                p_host=v_target_params[0],
                p_port=v_target_params[1],
                p_service=v_target_params[2],
                p_user=v_target_params[3],
                p_password=v_target_params[4],
                p_application_name='compare_databases'
            )

            v_task['kwds']['p_block_size'] = v_options.block_size
            v_task['kwds']['p_queue'] = v_queue
            v_task['kwds']['p_is_sending_data_array'] = v_is_sending_data_array
            v_task['kwds']['p_worker_index'] = i

            v_producers_result_list.append(
                v_producers_process_pool.apply_async(
                    func=v_task['function'],
                    kwds=v_task['kwds']
                )
            )

        v_producers_process_pool.close()
        v_producers_process_pool.join()

        v_consumers_process_pool.close()
        v_consumers_process_pool.join()

        #If any exception in any producer task
        if not all([v_result.successful() for v_result in v_producers_result_list]):
            print('Some exception has occurred in the comparer subprocesses. Please, check the exceptions below:')

            for v_result in v_producers_result_list:
                if not v_result.successful():
                    try:
                        v_result.get()
                    except Exception:
                        print(traceback.format_exc())

        #If any exception in any consumer task
        if not all([v_result.successful() for v_result in v_consumers_result_list]):
            print('Some exception has occurred in the consumer subprocesses. Please, check the exceptions below:')

            for v_result in v_consumers_result_list:
                if not v_result.successful():
                    try:
                        v_result.get()
                    except Exception:
                        print(traceback.format_exc())
    except Exception:
        print(traceback.format_exc())
