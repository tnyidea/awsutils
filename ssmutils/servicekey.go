package ssmutils

import (
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/fubotv/smo-content-operations/utils/awsutils"
)

// TODO make a version that can be passed as an encrypted hash
func NewSSMSession(serviceKey string) (*ssm.SSM, error) {
	awsSession, err := awsutils.NewAWSSession(serviceKey)
	if err != nil {
		return nil, err
	}
	return ssm.New(awsSession), nil
}
