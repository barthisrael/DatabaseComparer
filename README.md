# DatabaseComparer
Compare PostgreSQL databases objects and data and generates a report containing DDL/DML commands. They must be applied in source database in order to become like target database.

Sample call:

```bash
python compare_databases.py --block-size 500 --output-file output.xlsx --source-database-connection HOST1:PORT1:DATABASE1:USER1:PASSWORD1 --target-database-connection HOST2:PORT2:DATABASE2:USER2:PASSWORD2
```

The script will open as many subprocess as cores your cpu have and run tasks in parallel to get a faster result.

Will also reduce memory using block-size parameter. You can set it as you want (greater than 0) to tune memory usage according to your machine.

In the example, will allow each subprocess query blocks of at most 500 registers in each database at a time.

After comparison, will generate an output file named 'output.xlsx', containing DDL/DML commands to be applied in source database in order to become like target database.

Objects that the app deals with:

- Function
- Index
- Materialized view
- Procedure
- Schema
- Sequence
- Table check
- Table column
- Table data
- Table exclude
- Table foreign key
- Table primary key
- Table rule
- Table trigger
- Table unique
- Table
- Trigger function
- View

Objects comparison to be implemented in the soon future:

- Domain
- Event trigger
- Event trigger function
- Extension
- FDW
- Foreign table
- Foreign table column
- Foreign table data
- Logical replication
- Type

Some limitations:

- Do not deal with processing progress yet
- Do not detect operations like "RENAME" object. Will generate, e.g.,  a "DROP" and a "CREATE" statement for a renamed table
- Do not deal with dependencies. DDL and DML are generated but do not consider order of execution
