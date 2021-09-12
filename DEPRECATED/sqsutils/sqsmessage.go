package sqsutils

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/service/sqs"
	"reflect"
)

type SqsMessage struct {
	MessageId         string            `json:"messageId"`
	ReceiptHandle     string            `json:"receiptHandle"`
	MessageAttributes map[string]string `json:"messageAttributes"`
	Body              string            `json:"body"`
}

func (p *SqsMessage) Bytes() []byte {
	b, _ := json.Marshal(p)
	return b
}

func (p *SqsMessage) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

func (p *SqsMessage) IsZero() bool {
	return reflect.DeepEqual(*p, SqsMessage{})
}

func ParseAwsSqsMessage(m sqs.Message) SqsMessage {
	return SqsMessage{
		MessageId:     *m.MessageId,
		ReceiptHandle: *m.ReceiptHandle,
		Body:          *m.Body,
	}
}
