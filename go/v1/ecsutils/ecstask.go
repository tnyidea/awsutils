package ecsutils

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ecs"
	"strings"
)

type ECSTask struct {
	AwsSession     *session.Session        `json:"-"`
	TaskDefinition string                  `json:"taskDefinition"`
	Network        ECSNetworkConfiguration `json:"network"`
	Cluster        string                  `json:"cluster"`
	Environment    map[string]string       `json:"environment"`
	Command        string                  `json:"command"`
}

type ECSNetworkConfiguration struct {
	VPC           string   `json:"vpc"`
	Subnets       []string `json:"subnets"`
	SecurityGroup string   `json:"securityGroup"`
}

func (p *ECSTask) RunFargateTask() (r *ecs.RunTaskOutput, err error) {
	ecsService := ecs.New(p.AwsSession)

	var envKeyValuePair []*ecs.KeyValuePair
	for key, value := range p.Environment {
		envKeyValuePair = append(envKeyValuePair, &ecs.KeyValuePair{
			Name:  aws.String(key),
			Value: aws.String(value),
		})
	}

	r, err = ecsService.RunTask(&ecs.RunTaskInput{
		// CapacityProviderStrategy: nil,
		Cluster: aws.String(p.Cluster),
		// Count:                    nil,
		// EnableECSManagedTags:     nil,
		Group:      aws.String("family:" + p.TaskDefinition),
		LaunchType: aws.String("FARGATE"),
		NetworkConfiguration: &ecs.NetworkConfiguration{
			AwsvpcConfiguration: &ecs.AwsVpcConfiguration{
				AssignPublicIp: aws.String("ENABLED"),
				SecurityGroups: []*string{
					aws.String(p.Network.SecurityGroup),
				},
				Subnets: aws.StringSlice(p.Network.Subnets),
			},
		},
		Overrides: &ecs.TaskOverride{
			ContainerOverrides: []*ecs.ContainerOverride{&ecs.ContainerOverride{
				Command: aws.StringSlice(strings.Split(p.Command, " ")),
				// Cpu:                  nil,
				Environment: envKeyValuePair,
				// EnvironmentFiles:     nil,
				// Memory:               nil,
				// MemoryReservation:    nil,
				Name: aws.String(p.TaskDefinition),
				// ResourceRequirements: nil,
			}},
			// Cpu:                           nil,
			// ExecutionRoleArn:              nil,
			// InferenceAcceleratorOverrides: nil,
			// Memory:                        nil,
			// TaskRoleArn:                   nil,
		},
		// PlacementConstraints: nil,
		// PlacementStrategy:    nil,
		// PlatformVersion:      nil,
		// PropagateTags:        nil,
		// ReferenceId:          nil,
		// StartedBy:            nil,
		// Tags:                 nil,
		TaskDefinition: aws.String(p.TaskDefinition),
	})
	if err != nil {
		return nil, err
	}

	return r, nil
}
