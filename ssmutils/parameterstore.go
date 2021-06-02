package ssmutils

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ssm"
	"math"
	"reflect"
	"strings"
)

type SSMParameterStore struct {
	ServiceKey string `json:"-"`
	Path       string `json:"Path"`
}

func NewSSMParameterStore(serviceKey string, path ...string) (r SSMParameterStore, err error) {
	var pathValue string
	if path != nil {
		pathValue = path[0]
		if !strings.HasPrefix(pathValue, "/") {
			pathValue = "/" + pathValue
		}
	}
	r = SSMParameterStore{
		ServiceKey: serviceKey,
		Path:       pathValue,
	}

	return r, nil
}

func (p *SSMParameterStore) Bytes() []byte {
	b, _ := json.Marshal(p)
	return b
}

func (p *SSMParameterStore) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

func (p *SSMParameterStore) IsZero() bool {
	return reflect.DeepEqual(*p, SSMParameterStore{})
}

func (p *SSMParameterStore) GetParameter(key string) (r string, err error) {
	ssmSession, err := NewSSMSession(p.ServiceKey)
	if err != nil {
		return "", err
	}

	if p.Path != "" {
		key = p.Path + "/" + key
	}

	result, err := ssmSession.GetParameter(
		&ssm.GetParameterInput{
			Name:           aws.String(key),
			WithDecryption: aws.Bool(true),
		},
	)
	if err != nil {
		return "", err
	}
	r = aws.StringValue(result.Parameter.Value)

	return r, nil
}

func (p *SSMParameterStore) GetParameters(keys []string) (r map[string]string, err error) {
	ssmSession, err := NewSSMSession(p.ServiceKey)
	if err != nil {
		return nil, err
	}

	if p.Path != "" {
		for i, key := range keys {
			keys[i] = p.Path + "/" + key
		}
	}

	r = make(map[string]string)
	lenKeys := len(keys)
	pages := int(math.Ceil(float64(lenKeys) / 10))
	for i := 0; i < pages; i++ {
		sliceMin := i * 10
		sliceMax := (i + 1) * 10
		if sliceMax > len(keys) {
			sliceMax = len(keys)
		}

		result, err := ssmSession.GetParameters(
			&ssm.GetParametersInput{
				Names:          aws.StringSlice(keys[sliceMin:sliceMax]),
				WithDecryption: aws.Bool(true),
			},
		)
		if err != nil {
			return nil, err
		}

		for _, v := range result.Parameters {
			key := aws.StringValue(v.Name)
			if p.Path != "" {
				key = strings.ReplaceAll(key, p.Path+"/", "")
			}
			r[key] = aws.StringValue(v.Value)
		}
	}

	return r, nil
}
