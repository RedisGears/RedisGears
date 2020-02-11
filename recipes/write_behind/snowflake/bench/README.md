# Benchmarks for Snowflake DB

The benchmarks test performance of insert, update, and delete operations.

We use either standalone Snowflake Python Connector or the connector combined with SQLAlchemy.

All the required installations are done with the Gears Write-Behind recipe.

## Invocation

First,  invoke the Gears Python virtual environment:

```
. /var/opt/redislabs/lib/modules/python3/.venv/bin/activate
```

And then:

```
SNOW_ACCT='CODE.eu-west-1' SNOW_USER='...' SNOW_PWD='...' SNOW_DB=test1 \
./sf-pyconn-1.py BATCH-SIZE
```

`BATCH-SIZE` is limited to 16364.

The benchmark scripts create a database named by `SNOW_DB` and then drop it.

Output is of the form:

```
inserting...
insert: 0:00:00.059520 (per row)
rows: 10
updating via insert+delete...
update (insert): 0:00:00.139759 (per row)
rows (after update): 10
deleting...
delete: 0:00:00.064373 (per row)
rows: 0
```

