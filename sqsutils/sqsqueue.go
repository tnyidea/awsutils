package sqsutils

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"reflect"
	"strconv"
)

type SqsQueue struct {
	ServiceKey string `json:"-"` // Should be private for output
	Name       string `json:"name"`
	Url        string `json:"url"`
}

func NewSqsQueue(queueName string, serviceKey string) (SqsQueue, error) {
	url, err := getSqsQueueUrl(queueName, serviceKey)
	if err != nil {
		return SqsQueue{}, err
	}

	return SqsQueue{
		ServiceKey: serviceKey,
		Name:       queueName,
		Url:        url,
	}, nil
}

func (p *SqsQueue) Bytes() []byte {
	b, _ := json.Marshal(p)
	return b
}

func (p *SqsQueue) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

func (p *SqsQueue) IsZero() bool {
	return reflect.DeepEqual(*p, SqsQueue{})
}

func (p *SqsQueue) QueueDepth() int {
	sqsSession, err := NewSqsSession(p.ServiceKey)
	if err != nil {
		return -1
	}

	result, err := sqsSession.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: aws.StringSlice([]string{"ApproximateNumberOfMessages"}),
		QueueUrl:       aws.String(p.Url),
	})
	if err != nil {
		return -1
	}

	depth, err := strconv.Atoi(*result.Attributes["ApproximateNumberOfMessages"])
	if err != nil {
		return -1
	}

	return depth
}

func (p *SqsQueue) SendMessage(v SqsMessage) error {
	sqsSession, err := NewSqsSession(p.ServiceKey)
	if err != nil {
		return err
	}

	var messageAttributes map[string]*sqs.MessageAttributeValue
	if v.MessageAttributes != nil {
		messageAttributes = make(map[string]*sqs.MessageAttributeValue)

		for name, value := range v.MessageAttributes {
			messageAttributes[name] = &sqs.MessageAttributeValue{
				BinaryListValues: nil,
				BinaryValue:      nil,
				DataType:         aws.String("String"),
				StringListValues: nil,
				StringValue:      aws.String(value),
			}
		}
	}

	result, err := sqsSession.SendMessage(&sqs.SendMessageInput{
		DelaySeconds:            nil,
		MessageAttributes:       messageAttributes,
		MessageBody:             aws.String(v.Body),
		MessageDeduplicationId:  nil,
		MessageGroupId:          nil,
		MessageSystemAttributes: nil,
		QueueUrl:                aws.String(p.Url),
	})
	if err != nil {
		return err
	}
	log.Println("Send Message:", result)

	return nil
}

func (p *SqsQueue) ReceiveMessage() (SqsMessage, error) {
	messages, err := p.ReceiveMessages(1)
	if err != nil {
		return SqsMessage{}, err
	}

	if len(messages) != 1 {
		return SqsMessage{}, errors.New("error: Received message count is not 1 for sqsutils.ReceiveMessage()")
	}
	return messages[0], nil
}

func (p *SqsQueue) ReceiveMessages(count int) (r []SqsMessage, err error) {
	sqsSession, err := NewSqsSession(p.ServiceKey)
	if err != nil {
		return nil, err
	}

	result, err := sqsSession.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MaxNumberOfMessages: aws.Int64(int64(count)),
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl: aws.String(p.Url),
	})
	if err != nil {
		return nil, err
	}

	for _, message := range result.Messages {
		r = append(r, ParseAwsSqsMessage(*message))
	}

	return r, nil
}

func (p *SqsQueue) ReturnMessage(v SqsMessage) error {
	sqsSession, err := NewSqsSession(p.ServiceKey)
	if err != nil {
		return err
	}

	_, err = sqsSession.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(p.Url),
		ReceiptHandle:     aws.String(v.ReceiptHandle),
		VisibilityTimeout: aws.Int64(0),
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *SqsQueue) DeleteMessage(v SqsMessage) error {
	sqsSession, err := NewSqsSession(p.ServiceKey)
	if err != nil {
		return err
	}

	_, err = sqsSession.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(p.Url),
		ReceiptHandle: aws.String(v.ReceiptHandle),
	})

	return nil
}
