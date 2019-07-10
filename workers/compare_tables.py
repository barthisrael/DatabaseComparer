import os
import inspect
import multiprocessing
import Spartacus.Database

from .import custom_exceptions
from .import utils


def inserted_callback(p_queue=None, p_columns=None, p_row=None, p_key=None):
    """Callback executed when a table was created in second database. Sends a row by queue to master process.

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
        'type': 'tables',
        'row': [
            p_row['table_schema'],
            p_row['table_name'],
            ','.join(p_key),
            'INSERTED',
            '',
            inspect.cleandoc(doc=p_row['create_table_ddl'])
        ]
    })


def deleted_callback(p_queue=None, p_columns=None, p_row=None, p_key=None):
    """Callback executed when a table was dropped from second database. Sends a row by queue to master process.

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
        'type': 'tables',
        'row': [
            p_row['table_schema'],
            p_row['table_name'],
            ','.join(p_key),
            'DELETED',
            '',
            inspect.cleandoc(doc=p_row['drop_table_ddl'])
        ]
    })


def compare_tables(p_database_1=None, p_database_2=None, p_block_size=None, p_queue=None, p_is_sending_data_array=None, p_worker_index=None):
    """Used to compare tables between databases.

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
            with obj as (
               SELECT c.oid,
                     'pg_class'::regclass,
                     c.relname AS name,
                     n.nspname AS namespace,
                     coalesce(cc.column2,c.relkind::text) AS kind,
                     pg_get_userbyid(c.relowner) AS owner,
                     coalesce(cc.column2,c.relkind::text) AS sql_kind,
                     cast(format('%s.%s', n.nspname, c.relname)::regclass AS text) AS sql_identifier
                FROM pg_class c
                INNER JOIN (
                    SELECT oid,
                           nspname
                    FROM pg_namespace
                    WHERE nspname NOT IN (
                        'information_schema',
                        'pg_catalog',
                        'pg_toast'
                    )
                      AND nspname NOT LIKE 'pg%%temp%%'
                ) n ON n.oid=c.relnamespace
                inner join (
                     values ('r','TABLE'),
                            --('f','FOREIGN TABLE'),
                            ('p','PARTITIONED TABLE')
                ) as cc on cc.column1 = c.relkind
            ),
            columns as (
                SELECT a.attname AS name, format_type(t.oid, NULL::integer) AS type,
                    CASE
                        WHEN (a.atttypmod - 4) > 0 THEN a.atttypmod - 4
                        ELSE NULL::integer
                    END AS size,
                    a.attnotnull AS not_null,
                    def.adsrc AS "default",
                    col_description(c.oid, a.attnum::integer) AS comment,
                    con.conname AS primary_key,
                    a.attislocal AS is_local,
                    a.attstorage::text AS storage,
                    nullif(col.collcollate::text,'') AS collation,
                    a.attnum AS ord,
                    s.nspname AS namespace,
                    c.relname AS class_name,
                    format('%s.%I',text(c.oid::regclass),a.attname) AS sql_identifier,
                    c.oid,
                    format('%I %s%s%s%s',
                    	a.attname::text,
                    	format_type(t.oid, a.atttypmod),
            	        CASE
                	      WHEN length(col.collcollate) > 0
                    	  THEN ' COLLATE ' || quote_ident(col.collcollate::text)
                          ELSE ''
                    	END,
                    	CASE
                          WHEN a.attnotnull THEN ' NOT NULL'::text
                          ELSE ''::text
                    	END,
                        CASE
                          WHEN a.attidentity = 'a' THEN ' GENERATED ALWAYS AS IDENTITY'::text
                          WHEN a.attidentity = 'd' THEN ' GENERATED BY DEFAULT AS IDENTITY'::text
                          ELSE ''::text
                        END)
                    AS definition
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
               LEFT JOIN pg_constraint con
                    ON con.conrelid = c.oid AND (a.attnum = ANY (con.conkey)) AND con.contype = 'p'
               LEFT JOIN pg_type t ON t.oid = a.atttypid
               LEFT JOIN pg_collation col ON col.oid = a.attcollation
               JOIN pg_namespace tn ON tn.oid = t.typnamespace
              WHERE c.relkind IN ('r','v','c','f','p') AND a.attnum > 0 AND NOT a.attisdropped
                AND has_table_privilege(c.oid, 'select') AND has_schema_privilege(s.oid, 'usage')
              ORDER BY s.nspname, c.relname, a.attnum
            ),
            createtable as (
                select obj.namespace,
                       obj.name,
                    'CREATE '||
                  case relpersistence
                    when 'u' then 'UNLOGGED '
                    when 't' then 'TEMPORARY '
                    else ''
                  end
                  || case obj.kind when 'PARTITIONED TABLE' then 'TABLE' else obj.kind end || ' ' || obj.sql_identifier
                  || case obj.kind when 'TYPE' then ' AS' else '' end
                  || case when c.relispartition
                  then
                      E'\n' ||
                      (SELECT
                         coalesce(' PARTITION OF ' || string_agg(i.inhparent::regclass::text,', '), '')
                         FROM pg_inherits i WHERE i.inhrelid = format('%s.%s', obj.namespace, obj.name)::regclass) ||
                      E'\n'||
                        coalesce(' '||(
                          pg_get_expr(c.relpartbound, c.oid, true)
                        ),'')
                  else
                      E' (\n'||
                        coalesce(''||(
                          SELECT coalesce(string_agg('    '||definition,E',\n'),'')
                          FROM columns
                          WHERE is_local
                            AND namespace = obj.namespace
                            AND class_name = obj.name
                        )||E'\n','')||')'
                      ||
                      (SELECT
                        coalesce(' INHERITS(' || string_agg(i.inhparent::regclass::text,', ') || ')', '')
                         FROM pg_inherits i WHERE i.inhrelid = format('%s.%s', obj.namespace, obj.name)::regclass)
                  end
                  ||
                  case when c.relkind = 'p'
                  then E'\n' || ' PARTITION BY ' || pg_get_partkeydef(format('%s.%s', obj.namespace, obj.name)::regclass)
                  else '' end
                  ||
                  CASE relhasoids WHEN true THEN ' WITH OIDS' ELSE '' END
                  ||
                  coalesce(
                    E'\nSERVER '||quote_ident(fs.srvname)
                    ,'')
                  ||
                  coalesce(
                    E'\nOPTIONS (\n'||
                    (select string_agg(
                              '    '||quote_ident(option_name)||' '||quote_nullable(option_value),
                              E',\n')
                       from pg_options_to_table(ft.ftoptions))||E'\n)'
                    ,'')
                  ||
                  E';\n' as text
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
                 ) n ON n.oid=c.relnamespace
                 JOIN obj ON c.relname = obj.name AND n.nspname = obj.namespace
                 LEFT JOIN pg_foreign_table  ft ON (c.oid = ft.ftrelid)
                 LEFT JOIN pg_foreign_server fs ON (ft.ftserver = fs.oid)
                -- AND relkind in ('r','c')
            ),
            altertabledefaults as (
                select namespace,
                       class_name,
                    coalesce(
                      string_agg(
                        'ALTER TABLE '||text(format('%s.%s', namespace, class_name))||
                          ' ALTER '||quote_ident(name)||
                          ' SET DEFAULT '||"default",
                        E';\n') || E';\n\n',
                    '') as text
                   from columns
                  where "default" is not null
                  GROUP BY namespace,
                           class_name
            ),
            alterowner as (
                select namespace,
                       name,
                   case
                     when obj.kind in ('INDEX', 'PARTITIONED INDEX') then ''
                     when obj.kind = 'PARTITIONED TABLE'
                     then 'ALTER TABLE '||sql_identifier||
                          ' OWNER TO '||quote_ident(owner)||E';\n\n'
                     else 'ALTER '||sql_kind||' '||sql_identifier||
                          ' OWNER TO '||quote_ident(owner)||E';\n\n'
                   end as text
                  from obj
            ),
            privileges as (
                SELECT (u_grantor.rolname)::information_schema.sql_identifier AS grantor,
                        (grantee.rolname)::information_schema.sql_identifier AS grantee,
                        (current_database())::information_schema.sql_identifier AS table_catalog,
                        (nc.nspname)::information_schema.sql_identifier AS table_schema,
                        (c.relname)::information_schema.sql_identifier AS table_name,
                        (c.prtype)::information_schema.character_data AS privilege_type,
                        (
                            CASE
                                WHEN (pg_has_role(grantee.oid, c.relowner, 'USAGE'::text) OR c.grantable) THEN 'YES'::text
                                ELSE 'NO'::text
                            END)::information_schema.yes_or_no AS is_grantable,
                        (
                            CASE
                                WHEN (c.prtype = 'SELECT'::text) THEN 'YES'::text
                                ELSE 'NO'::text
                            END)::information_schema.yes_or_no AS with_hierarchy
                       FROM ( SELECT pg_class.oid,
                                     pg_class.relname,
                                     pg_class.relnamespace,
                                     pg_class.relkind,
                                     pg_class.relowner,
                                     (aclexplode(COALESCE(pg_class.relacl, acldefault('r', pg_class.relowner)))).grantor AS grantor,
                                     (aclexplode(COALESCE(pg_class.relacl, acldefault('r', pg_class.relowner)))).grantee AS grantee,
                                     (aclexplode(COALESCE(pg_class.relacl, acldefault('r', pg_class.relowner)))).privilege_type AS privilege_type,
                                     (aclexplode(COALESCE(pg_class.relacl, acldefault('r', pg_class.relowner)))).is_grantable AS is_grantable
                              FROM pg_class
                              WHERE pg_class.relkind <> 'S'
                              UNION
                              SELECT pg_class.oid,
                                     pg_class.relname,
                                     pg_class.relnamespace,
                                     pg_class.relkind,
                                     pg_class.relowner,
                                     (aclexplode(COALESCE(pg_class.relacl, acldefault('S', pg_class.relowner)))).grantor AS grantor,
                                     (aclexplode(COALESCE(pg_class.relacl, acldefault('S', pg_class.relowner)))).grantee AS grantee,
                                     (aclexplode(COALESCE(pg_class.relacl, acldefault('S', pg_class.relowner)))).privilege_type AS privilege_type,
                                     (aclexplode(COALESCE(pg_class.relacl, acldefault('S', pg_class.relowner)))).is_grantable AS is_grantable
                              FROM pg_class
                              WHERE pg_class.relkind = 'S') c(oid, relname, relnamespace, relkind, relowner, grantor, grantee, prtype, grantable),
                        pg_namespace nc,
                        pg_roles u_grantor,
                        ( SELECT pg_roles.oid,
                                pg_roles.rolname
                               FROM pg_roles
                            UNION ALL
                             SELECT (0)::oid AS oid,
                                'PUBLIC'::name) grantee(oid, rolname)
                      WHERE ((c.relnamespace = nc.oid) AND (c.grantee = grantee.oid) AND (c.grantor = u_grantor.oid)
                        AND (c.prtype = ANY (ARRAY['INSERT'::text, 'SELECT'::text, 'UPDATE'::text, 'DELETE'::text, 'TRUNCATE'::text, 'REFERENCES'::text, 'TRIGGER'::text]))
                        AND (pg_has_role(u_grantor.oid, 'USAGE'::text) OR pg_has_role(grantee.oid, 'USAGE'::text) OR (grantee.rolname = 'PUBLIC'::name)))
            ),
            grants as (
                select g.table_schema,
                       g.table_name,
                   coalesce(
                    string_agg(format(
                    	E'GRANT %s ON %s TO %s%s;\n',
                        privilege_type,
                        format('%s.%s', table_schema, table_name),
                        case grantee
                          when 'PUBLIC' then 'PUBLIC'
                          else quote_ident(grantee)
                        end,
                		case is_grantable
                          when 'YES' then ' WITH GRANT OPTION'
                          else ''
                        end), ''),
                    '') as text
                 FROM privileges g
                 join obj on (true)
                 WHERE table_schema=obj.namespace
                   AND table_name=obj.name
                   AND grantee<>obj.owner
                 GROUP BY table_schema,
                          table_name
            )
            SELECT ct.namespace AS table_schema,
                   ct.name AS table_name,
                   ct.text || COALESCE(atd.text, '') || COALESCE(ao.text, '') || COALESCE(g.text, '') AS create_table_ddl,
                   FORMAT('DROP TABLE %s.%s;', ct.namespace, ct.name) AS drop_table_ddl
            FROM createtable ct
            LEFT JOIN altertabledefaults atd
                   ON ct.namespace = atd.namespace
                  AND ct.name = atd.class_name
            LEFT JOIN alterowner ao
                   ON ct.namespace = ao.namespace
                  AND ct.name = ao.name
            LEFT JOIN grants g
                   ON ct.namespace = table_schema
                  AND ct.name = table_name
            ORDER BY 1,
                     2
        '''

        utils.compare_datatables(
            p_database_1=p_database_1,
            p_database_2=p_database_2,
            p_block_size=p_block_size,
            p_key=['table_schema', 'table_name'],
            p_sql=v_sql,
            p_inserted_callback=lambda p_columns, p_row, p_key: inserted_callback(p_queue=p_queue, p_columns=p_columns, p_row=p_row, p_key=p_key),
            p_deleted_callback=lambda p_columns, p_row, p_key: deleted_callback(p_queue=p_queue, p_columns=p_columns, p_row=p_row, p_key=p_key)
        )
    finally:
        p_queue.put(None)
        p_is_sending_data_array[p_worker_index] = False


def get_compare_tables_tasks():
    """Get list of tasks that will compare tables between databases.

        Args:

        Returns:
            list: list of tasks to be executed in a process pool. Each item is a dict instance with following strucutre:
                {
                    'function' (function): the function to be executed.
                    'kwds': keyworded args to be passed to the function.
                }
    """

    return [{
        'function': compare_tables,
        'kwds': {}
    }]
