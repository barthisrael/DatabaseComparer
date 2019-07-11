# DatabaseComparer
Compare PostgreSQL databases objects and data and generates a report containing DDL/DML commands. They must be applied in source database in order to become like target database.

Sample call:

```bash
python compare_databases.py --block-size 500 --source-database-connection HOST1:PORT1:DATABASE1:USER1:PASSWORD1 --target-database-connection HOST2:PORT2:DATABASE2:USER2:PASSWORD2 --output-database-connection HOST3:PORT3:DATABASE3:USER3:PASSWORD3
```

The script will open as many subprocess as cores your cpu have for comparer workers and consumer workers and run tasks in parallel to get a faster result.

Will also reduce memory using --block-size parameter. You can set it as you want (greater than 0) to tune memory usage according to your machine.

In the example, will allow each subprocess query blocks of at most 500 registers in each database at a time.

Comparison results containing DDL/DML commands to be applied in source database in order to become like target database are put in database_comparer_report.output_report table of the report connection.

Support:

- [ ] Domain
- [ ] Event trigger
- [ ] Event trigger function
- [ ] Extension
- [ ] FDW
- [ ] Foreign table
- [ ] Foreign table column
- [ ] Foreign table data
- [x] Function
- [x] Index
- [ ] Logical replication
- [x] Materialized view
- [x] Procedure
- [x] Schema
- [x] Sequence
- [x] Table check
- [x] Table column
- [x] Table data
- [x] Table exclude
- [x] Table foreign key
- [x] Table primary key
- [x] Table rule
- [x] Table trigger
- [x] Table unique
- [x] Table
- [x] Trigger function
- [ ] Type
- [x] View

Some limitations:

- Do not deal with processing progress yet
- Do not detect operations like "RENAME" object. Will generate, e.g.,  a "DROP" and a "CREATE" statement for a renamed table
- Do not deal with dependencies. DDL and DML are generated but do not consider order of execution
