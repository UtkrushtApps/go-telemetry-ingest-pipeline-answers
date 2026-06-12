package registry

// DeviceRegistry resolves device metadata from a device ID.
// In production this would query a database; here it is an in-memory stub.
type DeviceRegistry struct {
	records map[string]DeviceInfo
}

// DeviceInfo holds static metadata about a device.
type DeviceInfo struct {
	Facility string
	Region   string
}

// NewDeviceRegistry returns a registry pre-populated with stub records.
func NewDeviceRegistry() *DeviceRegistry {
	return &DeviceRegistry{
		records: map[string]DeviceInfo{
			"device-0": {Facility: "Plant-A", Region: "north"},
			"device-1": {Facility: "Plant-A", Region: "north"},
			"device-2": {Facility: "Plant-B", Region: "south"},
			"device-3": {Facility: "Plant-B", Region: "south"},
			"device-4": {Facility: "Plant-C", Region: "east"},
			"device-5": {Facility: "Plant-C", Region: "east"},
			"device-6": {Facility: "Plant-D", Region: "west"},
			"device-7": {Facility: "Plant-D", Region: "west"},
			"device-8": {Facility: "Plant-E", Region: "central"},
			"device-9": {Facility: "Plant-E", Region: "central"},
		},
	}
}

// Lookup returns the DeviceInfo for the given device ID and whether it was found.
func (r *DeviceRegistry) Lookup(deviceID string) (DeviceInfo, bool) {
	info, ok := r.records[deviceID]
	return info, ok
}
