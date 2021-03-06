package gat1400

import "strings"

type RegisterModel struct {
	RegisterObject *RegisterObject `json:"RegisterObject"`
}

type RegisterObject struct {
	DeviceID        string `json:"DeviceID"`
	ProtocolVersion string `json:"ProtocolVersion,omitempty"`
}

func BuildGat1400RegisterObj(deviceID, protocolVersion string) *RegisterModel {
	return &RegisterModel{
		RegisterObject: &RegisterObject{
			DeviceID:        deviceID,
			ProtocolVersion: protocolVersion,
		},
	}
}
func (rm *RegisterModel) GetViewID() string {
	if rm == nil || rm.RegisterObject == nil {
		return ""
	}
	return strings.Trim(rm.RegisterObject.DeviceID, " ")
}
