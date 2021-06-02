package apigwutils

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
)

type RequestMap map[string]func(context.Context, events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error)

func RequestMatches(r *events.APIGatewayProxyRequest, resourcePath string, httpMethod string) bool {
	return r.RequestContext.ResourcePath == resourcePath && r.HTTPMethod == httpMethod
}
