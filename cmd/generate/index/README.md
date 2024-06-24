# fga generate index
Generate SQL statements for FGA Index materialization.

## Usage
```console
➜ ./openfga generate index -h
FGA Index materialization generator

Usage:
  openfga generate index [flags]

Flags:
      --dialect string   the SQL dialect to target for SQL production ('postgresql', 'mysql', 'materialize') (default "materialize")
      --file string      an absolute file path to an FGA model (default 'model.fga') (default "model.fga")
  -h, --help             help for index
      --name string      a unique name for the index
      --output string    an absolute file path to the output file
```

## Example
```
model
  schema 1.1

type user

type group
  relations
    define member: [user, group#member]

type document
  relations
    define allowed: [user]
    define viewer: [user, group#member] and allowed
```
```shell
➜ ./openfga generate index --name fga_index --file model.fga
```

Output:
> ℹ️ The default output SQL dialect is for Materialize SQL.
> You can change the dialect for PostgreSQL or MySQL, but take
> not of the [Limitations](#limitations) below.
```sql
CREATE VIEW fga_index AS WITH MUTUALLY RECURSIVE group_member(
  subject_type TEXT, subject_id TEXT,
  subject_relation TEXT, relation TEXT,
  object_type TEXT, object_id TEXT
) AS (
  SELECT
    subject_type,
    subject_id,
    subject_relation,
    relation,
    object_type,
    object_id
  FROM
    tuples
  WHERE
    object_type = 'group'
    AND relation = 'member'
    AND subject_type IN ('user')
    AND subject_relation = ''
  UNION
  SELECT
    r.subject_type,
    r.subject_id,
    r.subject_relation,
    'member',
    s.object_type,
    s.object_id
  FROM
    group_member r,
    tuples s
  WHERE
    s.subject_type = 'group'
    AND s.subject_relation = 'member'
    AND s.relation = 'member'
    AND s.object_type = 'group'
    AND s.subject_type = r.object_type
    AND s.subject_id = r.object_id
    AND s.subject_relation = r.relation
),
document_allowed(
  subject_type TEXT, subject_id TEXT,
  subject_relation TEXT, relation TEXT,
  object_type TEXT, object_id TEXT
) AS (
  SELECT
    subject_type,
    subject_id,
    subject_relation,
    relation,
    object_type,
    object_id
  FROM
    tuples
  WHERE
    object_type = 'document'
    AND relation = 'allowed'
    AND subject_type IN ('user')
    AND subject_relation = ''
),
document_viewer(
  subject_type TEXT, subject_id TEXT,
  subject_relation TEXT, relation TEXT,
  object_type TEXT, object_id TEXT
) AS (
  WITH operand_0 AS (
    SELECT
      subject_type,
      subject_id,
      subject_relation,
      relation,
      object_type,
      object_id
    FROM
      tuples
    WHERE
      object_type = 'document'
      AND relation = 'viewer'
      AND subject_type IN ('user')
      AND subject_relation = ''
    UNION
    SELECT
      r.subject_type,
      r.subject_id,
      r.subject_relation,
      'viewer',
      s.object_type,
      s.object_id
    FROM
      group_member r,
      tuples s
    WHERE
      s.subject_type = 'group'
      AND s.subject_relation = 'member'
      AND s.relation = 'viewer'
      AND s.object_type = 'document'
      AND s.subject_type = r.object_type
      AND s.subject_id = r.object_id
      AND s.subject_relation = r.relation
  ),
  operand_1 AS (
    SELECT
      subject_type,
      subject_id,
      subject_relation,
      'viewer',
      object_type,
      object_id
    FROM
      document_allowed
  )
  SELECT
    subject_type,
    subject_id,
    subject_relation,
    relation,
    object_type,
    object_id
  FROM
    operand_0
  WHERE
    EXISTS (
      SELECT
      FROM
        operand_1
    )
)
SELECT
  *
FROM
  group_member
UNION ALL
SELECT
  *
FROM
  document_allowed
UNION ALL
SELECT
  *
FROM
  document_viewer;
```

## Limitations
The only SQL dialect that supports every feature of the FGA model language is a unique dialect of SQL that [Materialize](https://materialize.com/) supports. Materialize is the only SQL engine that supports mutually recursive queries at this time, and that is a requirement for full support of any FGA model.

### Non-stratifiable Queries
Given the following model:

```
model
  schema 1.1

type user

type document
  relations
    define blocked: [user, document#viewer]
    define viewer: [user] but not blocked
```
For Postgres or MySQL we produce the following SQL:

```sql
CREATE VIEW fga_index AS WITH RECURSIVE document_blocked(
  subject_type, subject_id, subject_relation,
  relation, object_type, object_id
) AS (
  WITH base AS (
    SELECT
      subject_type,
      subject_id,
      subject_relation,
      relation,
      object_type,
      object_id
    FROM
      tuples
    WHERE
      object_type = 'document'
      AND relation = 'blocked'
      AND subject_type IN ('user')
      AND subject_relation = ''
    UNION
    SELECT
      r.subject_type,
      r.subject_id,
      r.subject_relation,
      'blocked',
      s.object_type,
      s.object_id
    FROM
      document_viewer r,
      tuples s
    WHERE
      s.subject_type = 'document'
      AND s.subject_relation = 'viewer'
      AND s.relation = 'blocked'
      AND s.object_type = 'document'
      AND s.subject_type = r.object_type
      AND s.subject_id = r.object_id
      AND s.subject_relation = r.relation
  ),
  subtract AS (
    SELECT
      subject_type,
      subject_id,
      subject_relation,
      'blocked',
      object_type,
      object_id
    FROM
      document_unblocked
  )
  SELECT
    subject_type,
    subject_id,
    subject_relation,
    'blocked',
    object_type,
    object_id
  FROM
    base b
  WHERE
    NOT EXISTS (
      SELECT
      FROM
        subtract s
      WHERE
        b.subject_type = s.subject_type
        AND b.subject_id = s.subject_id
        AND b.object_type = s.object_type
        AND b.object_id = s.object_id
    )
),
document_viewer(
  subject_type, subject_id, subject_relation,
  relation, object_type, object_id
) AS (
  WITH base AS (
    SELECT
      subject_type,
      subject_id,
      subject_relation,
      relation,
      object_type,
      object_id
    FROM
      tuples
    WHERE
      object_type = 'document'
      AND relation = 'viewer'
      AND subject_type IN ('user')
      AND subject_relation = ''
    UNION
    SELECT
      r.subject_type,
      r.subject_id,
      r.subject_relation,
      'viewer',
      s.object_type,
      s.object_id
    FROM
      document_blocked r,
      tuples s
    WHERE
      s.subject_type = 'document'
      AND s.subject_relation = 'blocked'
      AND s.relation = 'viewer'
      AND s.object_type = 'document'
      AND s.subject_type = r.object_type
      AND s.subject_id = r.object_id
      AND s.subject_relation = r.relation
  ),
  subtract AS (
    SELECT
      subject_type,
      subject_id,
      subject_relation,
      'viewer',
      object_type,
      object_id
    FROM
      document_blocked
  )
  SELECT
    subject_type,
    subject_id,
    subject_relation,
    'viewer',
    object_type,
    object_id
  FROM
    base b
  WHERE
    NOT EXISTS (
      SELECT
      FROM
        subtract s
      WHERE
        b.subject_type = s.subject_type
        AND b.subject_id = s.subject_id
        AND b.object_type = s.object_type
        AND b.object_id = s.object_id
    )
),
document_unblocked(
  subject_type, subject_id, subject_relation,
  relation, object_type, object_id
) AS (
  SELECT
    subject_type,
    subject_id,
    subject_relation,
    relation,
    object_type,
    object_id
  FROM
    tuples
  WHERE
    object_type = 'document'
    AND relation = 'unblocked'
    AND subject_type IN ('user')
    AND subject_relation = ''
)
SELECT
  *
FROM
  document_blocked
UNION ALL
SELECT
  *
FROM
  document_viewer
UNION ALL
SELECT
  *
FROM
  document_unblocked;
```
If you try to create this view in Postgres you get the following error:

```
ERROR:  mutual recursion between WITH items is not implemented
LINE 72: document_viewer(
```
Similarly, in MySQL you get a parsing error:

```
ERROR 1064 (42000): You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'FROM
        subtract s
      WHERE
```
This error from MySQL is also because of the mutual recursion (though the Postgres error message is more abundantly clear).

In [Materialize](https://materialize.io) they support [mutual recursion in nested/recursive CTEs](https://materialize.com/blog/recursion-in-materialize/). This is what they implement uniquely using the `WITH MUTUALLY RECURSIVE` definition, which is purely unique to Materialize.

> ℹ️ Materialize is the only production grade product on the planet right now that supports mutually recursive queries that I am aware of.
>
Creation of this view succeeds, and you can query it by:

```console
materialize=> SELECT * FROM fga_index;
 subject_type | subject_id | subject_relation | relation | object_type | object_id
--------------+------------+------------------+----------+-------------+-----------
```

If we add some tuples which establish a non-stratifiable query:
| object     | relation | subject           |
|------------|----------|-------------------|
| document:1 | viewer   | user:jon          |
| document:1 | blocked  | document:1#viewer |

```console
-- subject_type, subject_id, subject_relation, relation, object_type, object_id
postgres=# INSERT INTO tuples VALUES ('user', 'jon', '', 'viewer', 'document', '1');
postgres=# INSERT INTO tuples VALUES ('document', '1', 'viewer', 'blocked', 'document', '1', '', null);
```
and then we query the index again:

```console
materialize=> SELECT * FROM fga_index;
```

This query will hang forever because there is no fixed point iteration at which this query will resolve. Therefore it will run forever. See [Materializes documentation on Non-terminating queries](https://materialize.com/docs/sql/recursive-ctes/#non-terminating-queries) for more information.

We can tell Materialize to fail at some fixed point by adding the (ERROR AT RECURSION LIMIT 100) statement to the view definition. Namely,

```console
CREATE VIEW fga_index WITH MUTUALLY RECURSIVE (ERROR AT RECURSION LIMIT 100) ...
```

Now if we query the index we get a different result:

```console
materialize=> SELECT * FROM fga_index;
ERROR:  Evaluation error: Recursive query exceeded the recursion limit 100. (Use RETURN AT RECURSION LIMIT to not error, but return the current state as the final result when reaching the limit.)
```

To better understand what is going on here I encourage you to read more on iterative fixed-point computation of recursive Materialize queries.
https://materialize.com/docs/sql/recursive-ctes
https://www.cidrdb.org/cidr2023/papers/p14-hirn.pdf