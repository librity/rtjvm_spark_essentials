# Resources

## Spark

- https://stackoverflow.com/questions/31477598/how-to-create-an-empty-dataframe-with-a-specified-schema
- https://stackoverflow.com/questions/62602720/string-to-date-migration-from-spark-2-0-to-3-0-gives-fail-to-recognize-eee-mmm

## Scala

- https://stackoverflow.com/questions/47867743/scala-splitting-with-double-quotes-vs-single-quotes
- https://scala-lang.org/files/archive/spec/2.11/06-expressions.html

```scala
// Quotes: String an Chars like in C 
val aString = "This is a string."
val aChar = 'a'
```

## IntelliJ IDEA

- https://intellij-support.jetbrains.com/hc/en-us/community/posts/4409874924178-Ctrl-inserts-a-weird-letter-e
- https://askubuntu.com/questions/1372781/new-ctrl-period-key-sequence-default-in-21-10

## Git

- https://stackoverflow.com/questions/8728093/how-do-i-un-revert-a-reverted-git-commit
- https://stackoverflow.com/questions/5354682/how-can-i-fix-a-reverted-git-commit
- https://docs.github.com/en/pull-requests/committing-changes-to-your-project/creating-and-editing-commits/changing-a-commit-message

```bash
# Get dangling commits and blobs
$ git fsck --lost-found
```

## Parquet

- https://www.databricks.com/glossary/what-is-parquet
- https://www.jumpingrivers.com/blog/parquet-file-format-big-data-r/

## PostgreSQL

- https://wikiless.org/wiki/Java_Database_Connectivity?lang=en
- https://stackoverflow.com/questions/20194806/how-to-get-a-list-column-names-and-datatypes-of-a-table-in-postgresql
- https://stackoverflow.com/questions/109325/postgresql-describe-table

```bash
$ ./psql.sh
```

```SQL
-- Describe the database
\dt
-- Get table schema
\
d+  public.employees
-- Run queries
SELECT first_name
from public.employees;

SELECT "Title"
FROM public.movies;

-- Detroy a table 
DROP TABLE public.movies;
```
