package sesutils

import (
	"encoding/json"
	"reflect"
)

type SESNotificationMessage struct {
	EventName string `json:"eventName"`
	Mail      struct {
		Destination []string `json:"destination"`
	} `json:"mail"`
	Receipt struct {
		Action struct {
			Type            string `json:"type"`
			TopicArn        string `json:"topicArn"`
			BucketName      string `json:"bucketName"`
			ObjectKeyPrefix string `json:"objectKeyPrefix"`
			ObjectKey       string `json:"objectKey"`
		} `json:"action"`
	} `json:"receipt"`
}

func (p *SESNotificationMessage) Bytes() []byte {
	b, _ := json.Marshal(p)
	return b
}

func (p *SESNotificationMessage) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

func (p *SESNotificationMessage) IsZero() bool {
	return reflect.DeepEqual(*p, SESNotificationMessage{})
}
