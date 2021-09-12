package ssmutils

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ssm"
	"math"
	"strings"
)

type SSMParameterStore struct {
	AwsSession *session.Session `json:"-"`
	KeyPrefix  string           `json:"keyPrefix"`
}

func NewSSMParameterStore(awsSession *session.Session, keyPrefix ...string) SSMParameterStore {
	var prefix string
	if keyPrefix != nil {
		prefix = keyPrefix[0]
		if !strings.HasPrefix(prefix, "/") {
			prefix = "/" + prefix
		}
	}

	return SSMParameterStore{
		AwsSession: awsSession,
		KeyPrefix:  prefix,
	}
}

func (p *SSMParameterStore) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

func (p *SSMParameterStore) GetParameter(key string) (string, error) {
	ssmSession := ssm.New(p.AwsSession)

	if p.KeyPrefix != "" {
		key = p.KeyPrefix + "/" + key
	}

	var parameter string
	output, err := ssmSession.GetParameter(
		&ssm.GetParameterInput{
			Name:           aws.String(key),
			WithDecryption: aws.Bool(true),
		},
	)
	if err != nil {
		return "", err
	}
	parameter = aws.StringValue(output.Parameter.Value)

	return parameter, nil
}

func (p *SSMParameterStore) GetParameters(keys []string) (map[string]string, error) {
	ssmSession := ssm.New(p.AwsSession)

	if p.KeyPrefix != "" {
		for i, key := range keys {
			keys[i] = p.KeyPrefix + "/" + key
		}
	}

	parameters := make(map[string]string)
	lenKeys := len(keys)
	pages := int(math.Ceil(float64(lenKeys) / 10))
	for i := 0; i < pages; i++ {
		sliceMin := i * 10
		sliceMax := (i + 1) * 10
		if sliceMax > lenKeys {
			sliceMax = lenKeys
		}

		output, err := ssmSession.GetParameters(
			&ssm.GetParametersInput{
				Names:          aws.StringSlice(keys[sliceMin:sliceMax]),
				WithDecryption: aws.Bool(true),
			},
		)
		if err != nil {
			return nil, err
		}

		for _, v := range output.Parameters {
			k := aws.StringValue(v.Name)
			if p.KeyPrefix != "" {
				k = strings.ReplaceAll(k, p.KeyPrefix+"/", "")
			}
			parameters[k] = aws.StringValue(v.Value)
		}
	}

	return parameters, nil
}
