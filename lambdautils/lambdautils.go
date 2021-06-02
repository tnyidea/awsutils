package lambdautils

import (
	"context"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"strings"
)

// Sample Lambda ARN: arn:aws:lambda:us-west-2:364514676002:function:fubonetworks-metadata:prod
func LambdaTagFromContext(ctx context.Context) string {
	lambdaCtx, _ := lambdacontext.FromContext(ctx)
	tokens := strings.Split(lambdaCtx.InvokedFunctionArn, ":")
	if len(tokens) != 8 {
		return ""
	} else {
		return tokens[7]
	}
}
