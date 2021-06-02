package etutils

import (
	"github.com/aws/aws-sdk-go/service/elastictranscoder"
	"github.com/fubotv/smo-content-operations/utils/awsutils"
)

// TODO make a version that can be passed as an encrypted hash
func NewElasticTranscoderSession(serviceKey string) (*elastictranscoder.ElasticTranscoder, error) {
	awsSession, err := awsutils.NewAWSSession(serviceKey)
	if err != nil {
		return nil, err
	}
	return elastictranscoder.New(awsSession), nil
}
