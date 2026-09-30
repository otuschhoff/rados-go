package crush

import "fmt"

// PlacementMap owns a decoded map that cannot be mutated by callers.
// Rule certification is retained independently so unused unsupported rules do
// not prevent placement through supported rules.
type PlacementMap struct {
	crushMap   *Map
	ruleErrors map[uint32]error
}

// DecodePlacementMap decodes and validates the graph once, then certifies rules.
func DecodePlacementMap(data []byte, limits DecodeLimits) (*PlacementMap, error) {
	decoded, err := DecodeMap(data, limits)
	if err != nil {
		return nil, err
	}
	result := &PlacementMap{crushMap: decoded, ruleErrors: make(map[uint32]error, len(decoded.Rules))}
	for ruleID, rule := range decoded.Rules {
		if rule.Type != RuleTypeReplicated && rule.Type != RuleTypeErasure {
			result.ruleErrors[ruleID] = fmt.Errorf("%w: rule %d type %d", ErrPlacement, ruleID, rule.Type)
		} else {
			result.ruleErrors[ruleID] = decoded.validateCertifiedRule(rule)
		}
	}
	return result, nil
}

// Place executes a certified rule with call-local placement state.
func (placementMap *PlacementMap) Place(ruleID, seed uint32, replicas int, osdWeights []uint32) ([]int32, error) {
	if placementMap == nil || placementMap.crushMap == nil || replicas <= 0 || replicas > maxCertifiedReplicas {
		return nil, ErrPlacement
	}
	rule, ok := placementMap.crushMap.Rules[ruleID]
	if !ok {
		return nil, fmt.Errorf("%w: rule %d does not exist", ErrPlacement, ruleID)
	}
	if err := placementMap.ruleErrors[ruleID]; err != nil {
		return nil, err
	}
	return placementMap.crushMap.placeRule(rule, seed, replicas, osdWeights)
}
