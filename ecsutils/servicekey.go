package ecsutils

import (
	"github.com/aws/aws-sdk-go/service/ecs"
	"github.com/gbnyc26/awsutils"
)

func NewECSSession(serviceKey string) (*ecs.ECS, error) {
	awsSession, err := awsutils.NewAWSSession(serviceKey)
	if err != nil {
		return nil, err
	}
	return ecs.New(awsSession), nil
}
