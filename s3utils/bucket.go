package s3utils

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func getBucketRegion(awsSession *session.Session, bucket string) (string, error) {
	awsRegion := aws.StringValue(awsSession.Config.Region)
	bucketRegion, err := s3manager.GetBucketRegion(aws.BackgroundContext(), awsSession, bucket, awsRegion)
	if err != nil {
		return "", err
	}

	return bucketRegion, nil
}
