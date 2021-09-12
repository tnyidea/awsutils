package snsutils

import (
	"encoding/json"
	"reflect"
)

type SnsNotification struct {
	Type             string `json:"Type"`
	MessageId        string `json:"MessageId"`
	TopicArn         string `json:"TopicArn"`
	Subject          string `json:"Subject"`
	Message          string `json:"Message"`
	Timestamp        string `json:"Timestamp"`
	SignatureVersion string `json:"SignatureVersion"`
	Signature        string `json:"Signature"`
	SigningCertUrl   string `json:"SigningCertURL"`
	UnsubscribeUrl   string `json:"UnsubScribeURL"`
}

func (p *SnsNotification) Bytes() []byte {
	b, _ := json.Marshal(p)
	return b
}

func (p *SnsNotification) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

func (p *SnsNotification) IsZero() bool {
	return reflect.DeepEqual(*p, SnsNotification{})
}
