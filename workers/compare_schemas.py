import os
import sys
import inspect
import multiprocessing
import Spartacus.Database

from .import custom_exceptions
from .import utils


def inserted_callback(p_queue=None, p_columns=None, p_row=None, p_key=None):
    """Callback executed when a schema was created in second database. Sends a row by queue to master process.

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
        'type': 'schemas',
        'row': [
            p_row['schema_name'],
            ','.join(p_key),
            'INSERTED',
            '',
            inspect.cleandoc(doc=p_row['create_schema_ddl'])
        ]
    })


def deleted_callback(p_queue=None, p_columns=None, p_row=None, p_key=None):
    """Callback executed when a schema was dropped from second database. Sends a row by queue to master process.

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
        'type': 'schemas',
        'row': [
            p_row['schema_name'],
            ','.join(p_key),
            'DELETED',
            '',
            inspect.cleandoc(doc=p_row['drop_schema_ddl'])
        ]
    })


def compare_schemas(p_database_1=None, p_database_2=None, p_block_size=None, p_queue=None, p_is_sending_data_array=None, p_worker_index=None):
    """Used to compare schemas between databases.

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

        #Prepare schema query
        v_sql = '''\
            with obj AS (
                SELECT n.oid,
                       'pg_namespace'::regclass,
                       n.nspname AS name,
                       current_database() AS namespace,
                       (CASE WHEN n.nspname LIKE 'pg_%'
                             THEN 'SYSTEM'
                             WHEN n.nspname = r.rolname
                             THEN 'AUTHORIZATION'
                             ELSE 'NAMESPACE'
                        END) AS kind,
                       PG_GET_USERBYID(n.nspowner) AS owner,
                       'SCHEMA' AS sql_kind,
                       QUOTE_IDENT(n.nspname) AS sql_identifier
                FROM pg_namespace n
                INNER JOIN pg_roles r
                        ON n.nspowner = r.oid
                WHERE n.nspname NOT IN (
                    'information_schema',
                    'pg_catalog',
                    'pg_toast'
                )
                  AND n.nspname NOT LIKE 'pg%%temp%%'
            ),
            comment AS (
                SELECT sql_identifier,
                       FORMAT(
                           E'COMMENT ON %s %s IS %L;\n\n',
                           sql_kind,
                           sql_identifier,
                           obj_description(oid)
                       ) AS text
                FROM obj
            ),
            alterowner AS (
                SELECT sql_identifier,
                       FORMAT(
                           E'ALTER %s %s OWNER TO %s;\n\n',
                           sql_kind,
                           sql_identifier,
                           QUOTE_IDENT(owner)
                       ) AS text
                FROM obj
            ),
            privileges AS (
                SELECT QUOTE_IDENT(n.nspname) AS nspname,
                       (u_grantor.rolname)::information_schema.sql_identifier AS grantor,
                       (grantee.rolname)::information_schema.sql_identifier AS grantee,
                       (n.privilege_type)::information_schema.character_data AS privilege_type,
                       (CASE WHEN (PG_HAS_ROLE(grantee.oid, n.nspowner, 'USAGE'::text) OR n.is_grantable)
                             THEN 'YES'::text
                             ELSE 'NO'::text
                        END)::information_schema.yes_or_no AS is_grantable
                FROM (
                    SELECT n.nspname,
                           n.nspowner,
                           (ACLEXPLODE(COALESCE(n.nspacl, ACLDEFAULT('n', n.nspowner)))).grantor AS grantor,
                           (ACLEXPLODE(COALESCE(n.nspacl, ACLDEFAULT('n', n.nspowner)))).grantee AS grantee,
                           (ACLEXPLODE(COALESCE(n.nspacl, ACLDEFAULT('n', n.nspowner)))).privilege_type AS privilege_type,
                           (ACLEXPLODE(COALESCE(n.nspacl, ACLDEFAULT('n', n.nspowner)))).is_grantable AS is_grantable
                    FROM pg_namespace n
                    WHERE n.nspname NOT IN (
                        'information_schema',
                        'pg_catalog',
                        'pg_toast'
                    )
                      AND n.nspname NOT LIKE 'pg%%temp%%'
                ) n
                INNER JOIN pg_roles u_grantor
                        ON n.grantor = u_grantor.oid
                INNER JOIN (
                    SELECT r.oid,
                           r.rolname
                    FROM pg_roles r

                    UNION ALL

                    SELECT (0)::oid AS oid,
                           'PUBLIC'::name
                ) grantee
                        ON n.grantee = grantee.oid
            ),
            grants AS (
                SELECT nspname,
                       COALESCE(
                           STRING_AGG(
                               FORMAT(
                                   E'GRANT %s ON SCHEMA %s TO %s%s;\n',
                                   privilege_type,
                                   nspname,
                                   (CASE grantee WHEN 'PUBLIC'
                                                 THEN 'PUBLIC'
                                                 ELSE QUOTE_IDENT(grantee)
                                    END),
                                   (CASE is_grantable WHEN 'YES'
                                                      THEN ' WITH GRANT OPTION'
                                                      ELSE ''
                                    END),
                                   ''
                               ),
                               ''
                           )
                       ) AS text
                FROM privileges
                GROUP BY nspname
            )
            SELECT QUOTE_IDENT(n.nspname) AS schema_name,
                   FORMAT(
                       E'CREATE SCHEMA %s;\n\n',
                       QUOTE_IDENT(n.nspname)
                   ) ||
                   c.text ||
                   a.text ||
                   g.text AS create_schema_ddl,
                   FORMAT(
                       'DROP SCHEMA %s;',
                       QUOTE_IDENT(n.nspname)
                   ) AS drop_schema_ddl
            FROM pg_namespace n
            INNER JOIN comment c
                    ON QUOTE_IDENT(n.nspname) = c.sql_identifier
            INNER JOIN alterowner a
                    ON QUOTE_IDENT(n.nspname) = a.sql_identifier
            INNER JOIN grants g
                    ON QUOTE_IDENT(n.nspname) = g.nspname
            ORDER BY 1
        '''

        utils.compare_datatables(
            p_database_1=p_database_1,
            p_database_2=p_database_2,
            p_block_size=p_block_size,
            p_key=['schema_name'],
            p_sql=v_sql,
            p_inserted_callback=lambda p_columns, p_row, p_key: inserted_callback(p_queue=p_queue, p_columns=p_columns, p_row=p_row, p_key=p_key),
            p_updated_callback=None,
            p_deleted_callback=lambda p_columns, p_row, p_key: deleted_callback(p_queue=p_queue, p_columns=p_columns, p_row=p_row, p_key=p_key)
        )
    finally:
        p_queue.put(None)
        p_is_sending_data_array[p_worker_index] = False


def get_compare_schemas_tasks():
    """Get list of tasks that will compare schemas between databases.

        Args:

        Returns:
            list: list of tasks to be executed in a process pool. Each item is a dict instance with following strucutre:
                {
                    'function' (function): the function to be executed.
                    'kwds': keyworded args to be passed to the function.
                }
    """

    return [{
        'function': compare_schemas,
        'kwds': {}
    }]
