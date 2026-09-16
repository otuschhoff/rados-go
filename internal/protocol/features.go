package protocol

// GlobalFeatures is the Ceph daemon/client feature namespace.
type GlobalFeatures uint64

const (
	FeatureMonitorNames           GlobalFeatures = 1 << 5
	FeatureMonitorEncoding        GlobalFeatures = 1 << 15
	FeaturePGID64                 GlobalFeatures = 1 << 9
	FeatureServerNautilus         GlobalFeatures = 1 << 2
	FeatureServerMimicIncarnation GlobalFeatures = 1<<57 | 1<<28
	FeatureServerNautilusMask                    = FeatureServerNautilus | FeatureServerMimicIncarnation
	FeatureMessageAddress2        GlobalFeatures = 1 << 59
	FeatureOSDReplyMux            GlobalFeatures = 1 << 12
	FeatureNewOSDOpEncoding       GlobalFeatures = 1 << 56
	FeatureNewOSDOpReplyEncoding  GlobalFeatures = 1 << 58
	FeatureCRUSHV2                GlobalFeatures = 1 << 36
	FeatureReserved               GlobalFeatures = 1 << 62
	// FeatureOSDMapEncoding is Ceph's SIGNIFICANT_FEATURES subset: precisely
	// the capabilities that can alter full or incremental OSDMap bytes.
	FeatureOSDMapEncoding GlobalFeatures = 0x0f04088090212a04
	// FeatureMonitorClient combines the map formats decoded by P04 with modern
	// MonMap encoding and the pinned monitor's CephX admission requirements.
	FeatureMonitorClient GlobalFeatures = 0x2e070282d2354004 | FeatureMonitorNames | FeatureMonitorEncoding | FeatureOSDMapEncoding | FeatureCRUSHV2
	FeatureOSDClient     GlobalFeatures = FeatureMonitorClient | FeatureOSDReplyMux | FeaturePGID64 | FeatureNewOSDOpEncoding | FeatureNewOSDOpReplyEncoding | FeatureMessageAddress2
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
