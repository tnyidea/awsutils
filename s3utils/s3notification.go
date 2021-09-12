package s3utils

import (
	"encoding/json"
)

type S3NotificationMessage struct {
	Records []S3NotificationRecord `json:"Records"`
}

type S3NotificationRecord struct {
	EventName string `json:"eventName"`
	S3        struct {
		Bucket struct {
			Name string `json:"name"`
		} `json:"bucket"`
		Object struct {
			Key string `json:"key"`
		} `json:"object"`
	} `json:"s3"`
}

func (p *S3NotificationMessage) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}
