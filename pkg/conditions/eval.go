package conditions

import (
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
	"github.com/openfga/openfga/pkg/conditions/types"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"golang.org/x/exp/maps"
)

var emptyConditionEvaluationResult = ConditionEvaluationResult{}

type ConditionEvaluationResult struct {
	ConditionMet bool
}

// EvaluateConditionExpression evalutes the provided CEL condition expression with a CEL environment constructed from
// the condition's parameter type defintions and using the context provided. If more than one source of context is
// provided, and if the keys provided in those context(s) are overlapping, then the overlapping key for the last most
// context wins.
func EvaluateConditionExpression(
	conditionExpression string,
	conditionParamTypeRefs map[string]*openfgapb.ConditionTypeReference,
	contextMaps ...map[string]any,
) (ConditionEvaluationResult, error) {

	if len(contextMaps) < 1 {
		return emptyConditionEvaluationResult, fmt.Errorf("at least one context source is required for conditionss expression evaluation")
	}

	var envOpts []cel.EnvOption
	for _, customTypeOpts := range types.CustomParamTypes {
		envOpts = append(envOpts, customTypeOpts...)
	}

	conditionParamTypes := map[string]*types.ParameterType{}
	for paramName, paramTypeRef := range conditionParamTypeRefs {
		paramType, err := types.DecodeParameterType(paramTypeRef)
		if err != nil {
			return emptyConditionEvaluationResult, fmt.Errorf("failed to decode parameter type for parameter '%s': %v", paramName, err)
		}

		conditionParamTypes[paramName] = paramType
	}

	for paramName, paramType := range conditionParamTypes {
		envOpts = append(envOpts, cel.Variable(paramName, paramType.CelType()))
	}

	env, err := cel.NewEnv(envOpts...)
	if err != nil {
		return emptyConditionEvaluationResult, fmt.Errorf("failed to construct CEL env: %v", err)
	}

	ast, issues := env.Compile(conditionExpression)
	if issues != nil && issues.Err() != nil {
		return emptyConditionEvaluationResult, fmt.Errorf("failed to compile condition expression: %v", issues.Err())
	}

	prg, err := env.Program(ast)
	if err != nil {
		return emptyConditionEvaluationResult, fmt.Errorf("condition expression construction error: %s", err)
	}

	if !reflect.DeepEqual(ast.OutputType(), cel.BoolType) {
		return emptyConditionEvaluationResult, fmt.Errorf("expected a bool condition expression output, but got '%s'", ast.OutputType())
	}

	// merge context maps
	clonedMap := maps.Clone(contextMaps[0])

	for _, contextMap := range contextMaps[1:] {
		maps.Copy(clonedMap, contextMap)
	}

	typedParams, err := CastContextToTypedParameters(clonedMap, conditionParamTypeRefs)
	if err != nil {
		return emptyConditionEvaluationResult, fmt.Errorf("failed to convert context to typed parameter values: %v", err)
	}

	out, _, err := prg.Eval(typedParams)
	if err != nil {
		return emptyConditionEvaluationResult, fmt.Errorf("failed to evaluate condition expression: %v", err)
	}

	conditionMetVal, err := out.ConvertToNative(reflect.TypeOf(false))
	if err != nil {
		return emptyConditionEvaluationResult, fmt.Errorf("failed to convert condition output to bool: %v", err)
	}

	conditionMet, ok := conditionMetVal.(bool)
	if !ok {
		return emptyConditionEvaluationResult, fmt.Errorf("expected CEL type conversion to return native Go bool")
	}

	return ConditionEvaluationResult{ConditionMet: conditionMet}, nil
}
