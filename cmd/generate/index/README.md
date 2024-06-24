# fga generate index
Generate SQL statements for FGA Index materialization.

## Example
```
type user

type group
  relations
    define member: [user, group#member]

type document
  relations
    define allowed: [user]
    define viewer: [user, group#member] and allowed
```
### Example 1
```shell
fga generate index --name group_member_user_index --file model.fga --object-type group --relation member --user-type user 
```

Outputs:
```
CREATE MATERIALIZED VIEW group_member_user AS (
  SELECT object_id, subject_object_id FROM relationship_tuples
  WHERE
  object_type='group' AND relation='member' AND subject_object_type='user' AND subject_relation=''
);

CREATE MATERIALIZED VIEW group_member_group_member AS
WITH MUTUALLY RECURSIVE
  group_member_group_member(object_type TEXT, object_id TEXT, relation TEXT, subject_object_type TEXT, subject_object_id TEXT, subject_relation TEXT) AS (
    SELECT DISTINCT object_type, object_id, relation, subject_object_type, subject_object_id, subject_relation FROM relationship_tuples WHERE object_type='group' AND relation='member' AND subject_object_type='group' AND subject_relation='member'

    UNION ALL

    SELECT DISTINCT a2.object_type, a2.object_id, a2.relation, a1.subject_object_type, a1.subject_object_id, a1.subject_relation FROM group_member_group_member a1 JOIN group_member_group_member a2 ON a1.object_id = a2.subject_object_id
  )
SELECT object_type, object_id, relation, subject_object_type, subject_object_id, subject_relation FROM group_member_group_member;

CREATE MATERIALIZED VIEW group_member_user_index AS (
  SELECT object_id, subject_object_id 
  FROM group_member_user

  UNION

  SELECT gg.object_id, rt.subject_object_id FROM relationship_tuples as rt
  LEFT JOIN group_member_group_member as gg ON gg.object_type=rt.object_type AND gg.relation=rt.relation AND gg.subject_object_id=rt.object_id WHERE rt.subject_object_type='user' AND rt.subject_relation=''
);
```

### Example 2
```shell
fga generate index --name document_viewer_user_index --file model.fga --object-type document --relation viewer --user-type user 
```

Outputs:
```
CREATE MATERIALIZED VIEW document_viewer_user AS
...

CREATE MATERIALIZED VIEW document_viewer_group_member AS
...

CREATE MATERIALIZED VIEW document_allowed_user AS
...

CREATE MATERIALIZED VIEW group_member_user AS
...

CREATE MATERIALIZED VIEW group_member_group_member AS
...

CREATE MATERIALIZED VIEW document_viewer_user_index AS
...
```