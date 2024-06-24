package materializer

import (
	"errors"
	"fmt"
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/typesystem"
)

type MaterializationDialect string

const (
	PostgresMaterializerDialect MaterializationDialect = "postgresql"
	MySQLMaterializerDialect    MaterializationDialect = "mysql"

	// Suport for materialize.io dialect.
	// See https://materialize.com/docs/sql/select/ for more information.
	MaterializeMaterializerDialect MaterializationDialect = "materialize"
)

type namedSQLStatement struct {
	name string
	sql  string
}

type MaterializerInput struct {
	Dialect    MaterializationDialect
	IndexName  string
	Typesystem *typesystem.TypeSystem
}

// Materialize produces one or more SQL statements defining the views that
// materialize some FGA index.
func Materialize(in MaterializerInput) (string, error) {
	typesys := in.Typesystem

	statements := map[string]namedSQLStatement{}
	for objectType, relations := range typesys.GetAllRelations() {
		for relationName, _ := range relations {
			namedStatement := materializeInternal(typesys, objectType, relationName)

			statements[namedStatement.name] = namedStatement
		}
	}

	var viewbody string
	var viewselect string

	i := 0
	for statementName, statement := range statements {
		if i >= len(statements)-1 {
			switch in.Dialect {
			case PostgresMaterializerDialect, MySQLMaterializerDialect:
				viewbody += fmt.Sprintf(`%s(subject_type, subject_id, subject_relation, relation, object_type, object_id) AS (%s)`, statementName, statement.sql)
			case MaterializeMaterializerDialect:
				viewbody += fmt.Sprintf(`%s(subject_type TEXT, subject_id TEXT, subject_relation TEXT, relation TEXT, object_type TEXT, object_id TEXT) AS (%s)`, statementName, statement.sql)
			default:
				return "", fmt.Errorf("unsupported SQL dialect provided '%s'", in.Dialect)
			}

			viewselect += fmt.Sprintf("SELECT * FROM %s", statementName)
		} else {
			switch in.Dialect {
			case PostgresMaterializerDialect, MySQLMaterializerDialect:
				viewbody += fmt.Sprintf(`%s(subject_type, subject_id, subject_relation, relation, object_type, object_id) AS (%s),`, statementName, statement.sql)
			case MaterializeMaterializerDialect:
				viewbody += fmt.Sprintf(`%s(subject_type TEXT, subject_id TEXT, subject_relation TEXT, relation TEXT, object_type TEXT, object_id TEXT) AS (%s),`, statementName, statement.sql)
			default:
				return "", fmt.Errorf("unsupported SQL dialect provided '%s'", in.Dialect)
			}

			viewselect += fmt.Sprintf(`SELECT * FROM %s UNION ALL `, statementName)
		}
		i++
	}

	var statementFmt string
	switch in.Dialect {
	case PostgresMaterializerDialect, MySQLMaterializerDialect:
		statementFmt = `
		CREATE VIEW %s AS WITH RECURSIVE
			%s
		%s;`
	case MaterializeMaterializerDialect:
		statementFmt = `
	CREATE VIEW %s AS WITH MUTUALLY RECURSIVE
		%s

	%s;`
	default:
		return "", fmt.Errorf("unsupported SQL dialect provided '%s'", in.Dialect)
	}

	return fmt.Sprintf(statementFmt, in.IndexName, viewbody, viewselect), nil
}

func materializeInternal(
	typesys *typesystem.TypeSystem,
	objectType, relation string,
) namedSQLStatement {
	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		panic(err)
	}

	return materializeInternalWithRewrite(typesys, objectType, relation, rel.GetRewrite())
}

func materializeInternalWithRewrite(
	typesys *typesystem.TypeSystem,
	objectType string,
	relation string,
	rewrite *openfgav1.Userset,
) namedSQLStatement {
	switch rewrite := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return materializeDirect(typesys, objectType, relation)
	case *openfgav1.Userset_ComputedUserset:
		rewrittenRelation := rewrite.ComputedUserset.GetRelation()
		return materializeComputedUserset(objectType, relation, rewrittenRelation)
	case *openfgav1.Userset_TupleToUserset:
		return materializeTupleToUserset(typesys, objectType, relation, rewrite)
	case *openfgav1.Userset_Union:
		var sql string

		childRewrites := rewrite.Union.GetChild()
		for i, childRewrite := range childRewrites {
			s := materializeInternalWithRewrite(typesys, objectType, relation, childRewrite)
			sql += s.sql

			if i < len(childRewrites)-1 {
				sql += " UNION "
			}
		}

		return namedSQLStatement{
			name: fmt.Sprintf("%s_%s", objectType, relation),
			sql:  sql,
		}

	case *openfgav1.Userset_Intersection:
		var sql string

		operands := []string{}

		childRewrites := rewrite.Intersection.GetChild()
		for i, childRewrite := range childRewrites {
			operandStatementName := fmt.Sprintf("operand_%d", i)
			operands = append(operands, operandStatementName)

			if i == 0 {
				sql += fmt.Sprintf("WITH %s AS (", operandStatementName)
			} else {
				sql += fmt.Sprintf("%s AS (", operandStatementName)
			}

			s := materializeInternalWithRewrite(typesys, objectType, relation, childRewrite)
			sql += s.sql

			if i < len(childRewrites)-1 {
				sql += "), "
			} else {
				sql += ")"
			}
		}

		if len(childRewrites) > 1 {
			sql += fmt.Sprintf("SELECT subject_type, subject_id, subject_relation, relation, object_type, object_id FROM %s WHERE EXISTS (SELECT FROM %s)", operands[0], strings.Join(operands[1:], ","))
		} else {
			sql += fmt.Sprintf("SELECT subject_type, subject_id, subject_relation, relation, object_type, object_id FROM %s", operands[0])
		}

		return namedSQLStatement{
			name: fmt.Sprintf("%s_%s", objectType, relation),
			sql:  sql,
		}

	case *openfgav1.Userset_Difference:
		baseRewrite := rewrite.Difference.GetBase()
		subtractRewrite := rewrite.Difference.GetSubtract()

		baseStatement := materializeInternalWithRewrite(typesys, objectType, relation, baseRewrite)

		subtractStatement := materializeInternalWithRewrite(typesys, objectType, relation, subtractRewrite)

		sql := fmt.Sprintf(`WITH base AS (%s), subtract AS (%s) SELECT subject_type, subject_id, subject_relation, '%s', object_type, object_id FROM base b WHERE NOT EXISTS (SELECT FROM subtract s WHERE b.subject_type=s.subject_type AND b.subject_id=s.subject_id AND b.object_type=s.object_type AND b.object_id=s.object_id)`, baseStatement.sql, subtractStatement.sql, relation)

		return namedSQLStatement{
			name: fmt.Sprintf("%s_%s", objectType, relation),
			sql:  sql,
		}
	default:
		panic("rewrite unsupported for indexing at this time")
	}
}

func materializeDirect(
	typesys *typesystem.TypeSystem,
	objectType string,
	relation string,
) namedSQLStatement {
	rel, err := typesys.GetRelation(objectType, relation)
	if err != nil {
		panic(err)
	}

	statement := namedSQLStatement{
		name: fmt.Sprintf("%s_%s", objectType, relation),
	}

	var subjectTypes []string
	var nestedStatements []string
	for _, subjectRelationRef := range rel.GetTypeInfo().GetDirectlyRelatedUserTypes() {
		subjectType := subjectRelationRef.GetType()

		if subjectRelationRef.GetRelationOrWildcard() == nil {
			subjectTypes = append(subjectTypes, fmt.Sprintf(`'%s'`, subjectType))
			continue
		}

		if subjectRelationRef.GetRelation() != "" {
			subjectRelation := subjectRelationRef.GetRelation()

			referencedTableName := fmt.Sprintf("%s_%s", subjectType, subjectRelation)

			sql := fmt.Sprintf(`SELECT r.subject_type, r.subject_id, r.subject_relation, '%s', s.object_type, s.object_id FROM %s r, tuples s WHERE s.subject_type = '%s' AND s.subject_relation = '%s' AND
			  s.relation = '%s' AND s.object_type = '%s' AND
			  s.subject_type = r.object_type AND s.subject_id = r.object_id AND
			  s.subject_relation = r.relation`, relation, referencedTableName, subjectType, subjectRelation, relation, objectType)

			nestedStatements = append(nestedStatements, sql)
		}
	}

	statement.sql = fmt.Sprintf(`SELECT subject_type, subject_id, subject_relation, relation, object_type,object_id FROM tuples WHERE object_type='%s' AND relation='%s' AND subject_type IN (%s) AND subject_relation=''`, objectType, relation, strings.Join(subjectTypes, ","))

	if len(nestedStatements) > 0 {
		statement.sql += " UNION "
	}

	for i, nestedStatement := range nestedStatements {
		if i < len(nestedStatements)-1 {
			statement.sql += fmt.Sprintf("%s UNION ", nestedStatement)
		} else {
			statement.sql += nestedStatement
		}
	}

	return statement
}

func materializeComputedUserset(
	objectType string,
	relation string,
	rewrittenRelation string,
) namedSQLStatement {
	rewrittenStatementName := fmt.Sprintf("%s_%s", objectType, rewrittenRelation)

	return namedSQLStatement{
		name: fmt.Sprintf("%s_%s", objectType, relation),
		sql:  fmt.Sprintf(`SELECT subject_type, subject_id, subject_relation, '%s', object_type,object_id FROM %s`, relation, rewrittenStatementName),
	}
}

func materializeTupleToUserset(
	typesys *typesystem.TypeSystem,
	objectType string,
	relation string,
	ttuRewrite *openfgav1.Userset_TupleToUserset,
) namedSQLStatement {
	tuplesetRelation := ttuRewrite.TupleToUserset.GetTupleset().GetRelation()
	computedRelation := ttuRewrite.TupleToUserset.GetComputedUserset().GetRelation()

	relatedTypes, err := typesys.GetDirectlyRelatedUserTypes(objectType, tuplesetRelation)
	if err != nil {
		// todo: handle error
		panic(err)
	}

	var subjectTypes []string // parent: [folder, org] - subjectTypes are ('folder', 'org')
	var quotedSubjectTypes []string
	for _, relatedType := range relatedTypes {
		subjectType := relatedType.GetType()

		_, err := typesys.GetRelation(subjectType, computedRelation)
		if err != nil {
			if errors.Is(err, typesystem.ErrRelationUndefined) {
				continue
			}

			// todo: handle error
			panic(err)
		}

		subjectTypes = append(subjectTypes, subjectType)
		quotedSubjectTypes = append(quotedSubjectTypes, fmt.Sprintf(`'%s'`, subjectType))
	}

	sql := fmt.Sprintf(`SELECT subject_type, subject_id, subject_relation, relation, object_type, object_id
    FROM tuples
    WHERE subject_type IN (%s) AND relation = '%s' AND object_type = '%s' UNION `, strings.Join(quotedSubjectTypes, ","), tuplesetRelation, objectType)

	for i, subjectType := range subjectTypes {
		if i < len(subjectTypes)-1 {
			sql += fmt.Sprintf(`SELECT i.subject_type, i.subject_id, i.subject_relation, '%s', p.object_type, p.object_id
			FROM %s p, %s i
			WHERE p.relation = '%s' AND p.object_type = '%s'
			AND p.subject_type = i.object_type AND p.subject_id = i.object_id
			AND i.relation = '%s' UNION `, computedRelation, fmt.Sprintf("%s_%s", objectType, relation), fmt.Sprintf("%s_%s", subjectType, computedRelation), tuplesetRelation, objectType, computedRelation)
		} else {
			sql += fmt.Sprintf(`SELECT i.subject_type, i.subject_id, i.subject_relation, '%s', p.object_type, p.object_id
			FROM %s p, %s i
			WHERE p.relation = '%s' AND p.object_type = '%s'
			AND p.subject_type = i.object_type AND p.subject_id = i.object_id
			AND i.relation = '%s'`, computedRelation, fmt.Sprintf("%s_%s", objectType, relation), fmt.Sprintf("%s_%s", subjectType, computedRelation), tuplesetRelation, objectType, computedRelation)
		}
	}

	return namedSQLStatement{
		name: fmt.Sprintf("%s_%s", objectType, relation),
		sql:  sql,
	}
}
