package protocol

// GlobalFeatures is the Ceph daemon/client feature namespace.
type GlobalFeatures uint64

const (
	FeatureServerNautilus         GlobalFeatures = 1 << 2
	FeatureServerMimicIncarnation GlobalFeatures = 1<<57 | 1<<28
	FeatureServerNautilusMask                    = FeatureServerNautilus | FeatureServerMimicIncarnation
	FeatureMessageAddress2        GlobalFeatures = 1 << 59
	FeatureReserved               GlobalFeatures = 1 << 62
)

func (features GlobalFeatures) Has(mask GlobalFeatures) bool {
	return features&mask == mask
}

// MessengerFeatures is the independent messenger-v2 feature namespace.
type MessengerFeatures uint64

const (
	MessengerFeatureRevision1   MessengerFeatures = 1 << 0
	MessengerFeatureCompression MessengerFeatures = 1 << 1
)

func (features MessengerFeatures) Has(mask MessengerFeatures) bool {
	return features&mask == mask
}
