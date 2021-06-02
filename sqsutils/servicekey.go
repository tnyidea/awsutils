package sqsutils

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/fubotv/smo-content-operations/utils/awsutils"
)

// TODO make a version that can be passed as an encrypted hash
func NewSqsSession(serviceKey string) (*sqs.SQS, error) {
	awsSession, err := awsutils.NewAWSSession(serviceKey)
	if err != nil {
		return nil, err
	}
	return sqs.New(awsSession), nil
}

func getSqsQueueUrl(queueName string, serviceKey string) (string, error) {
	sqsSession, err := NewSqsSession(serviceKey)
	if err != nil {
		return "", err
	}
	url, err := sqsSession.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", err
	}
	return *url.QueueUrl, nil
}
