WITH document_viewer_this AS (
    SELECT object_type, object_id, relation, subject_object_type, subject_object_id, subject_relation
    FROM relationship_tuples
    WHERE object_type='document' AND relation='viewer'
), document_restricted_this AS (
    SELECT object_type, object_id, relation, subject_object_type, subject_object_id, subject_relation
    FROM relationship_tuples
    WHERE object_type='document' AND relation='restricted'
), document_restricted2_this AS (
    SELECT object_type, object_id, relation, subject_object_type, subject_object_id, subject_relation
    FROM relationship_tuples
    WHERE object_type='document' AND relation='restricted2'
)

SELECT object_type, object_id, 'viewer' AS relation, subject_object_type, subject_object_id, subject_relation
FROM (
    SELECT * FROM document_viewer_this
    EXCEPT SELECT object_type, object_id, 'viewer' AS relation, subject_object_type, subject_object_id, subject_relation
    FROM document_restricted_this
    EXCEPT SELECT object_type, object_id, 'viewer' AS relation, subject_object_type, subject_object_id, subject_relation
    FROM document_restricted2_this
) AS document_viewer;

---

type user
type document
  relations
    define editor: [user]
    define viewer: editor

fga generate sql --object-type=document --relation=viewer

Output:
WITH document_editor AS (
    WITH document_editor_this AS (
        SELECT 'document' AS object_type, object_id, 'editor' AS relation, subject_object_type, subject_object_id, subject_relation
        FROM relationship_tuples
        WHERE object_type='document' AND relation='editor'
    )
    SELECT 'document' AS object_type, object_id, 'editor' AS relation, subject_object_type, subject_object_id, subject_relation
    FROM document_editor_this
)

SELECT object_type, object_id, 'viewer' AS relation, subject_object_type, subject_object_id, subject_relation
FROM (
    SELECT object_type, object_id, relation, subject_object_type, subject_object_id, subject_relation
    FROM document_editor
) AS document_viewer;