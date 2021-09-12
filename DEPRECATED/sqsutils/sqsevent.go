package sqsutils

import "github.com/aws/aws-lambda-go/events"

func ParseAwsEventSqsMessage(m events.SQSMessage) SqsMessage {
	return SqsMessage{
		MessageId:     m.MessageId,
		ReceiptHandle: m.ReceiptHandle,
		Body:          m.Body,
	}
}
