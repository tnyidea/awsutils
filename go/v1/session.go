package v1

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

func NewSession(region string) *session.Session {
	return NewSessionFromAwsConfig(&aws.Config{
		Region: aws.String(region),
	})
	//return session.Must(session.NewSessionWithOptions(session.Options{
	//	SharedConfigState: session.SharedConfigEnable,
	//	Config: aws.Config{
	//		Credentials: credentials.NewEnvCredentials(),
	//		Region:      aws.String(region),
	//	},
	//}))
}

func NewSessionFromAwsConfig(awsConfig *aws.Config) *session.Session {
	return session.Must(session.NewSession(awsConfig))
}
