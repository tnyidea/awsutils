package apigwutils

import (
	"encoding/base64"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"net/http"
	"time"
)

// Response Handler Functions
func defaultResponseHeaders() map[string]string {
	return map[string]string{
		"Cache-Control":               "must-revalidate, no-cache, no-store, no-transform, max-age=0",
		"Expires":                     time.Unix(0, 0).Format(http.TimeFormat),
		"Pragma":                      "no-cache",
		"X-Accel-Expires":             "0",
		"Access-Control-Allow-Origin": "'*'",
	}
}
func defaultResponseStatusMessage(httpStatusCode int, message string) (events.APIGatewayProxyResponse, error) {
	result := struct {
		Code    int    `json:"code"`
		Status  string `json:"status"`
		Message string `json:"message"`
	}{
		Code:    httpStatusCode,
		Status:  http.StatusText(httpStatusCode),
		Message: message,
	}
	b, _ := json.MarshalIndent(&result, "", "    ")
	headers := defaultResponseHeaders()
	headers["Content-Type"] = http.DetectContentType(b)
	return events.APIGatewayProxyResponse{StatusCode: httpStatusCode,
		Body:    string(b),
		Headers: headers,
	}, nil
}

// 200 Status OK
func ResponseStatusOK200(message ...string) (events.APIGatewayProxyResponse, error) {
	resultCode := http.StatusOK
	resultMessage := "Success"
	if message != nil {
		resultMessage = message[0]
	}
	return defaultResponseStatusMessage(resultCode, resultMessage)
}

// 202 Status Accepted
func ResponseStatusAccepted202(message ...string) (events.APIGatewayProxyResponse, error) {
	resultCode := http.StatusAccepted
	resultMessage := "Submitted for processing"
	if message != nil {
		resultMessage = message[0]
	}
	return defaultResponseStatusMessage(resultCode, resultMessage)
}

// 400 Status Bad Request
func ResponseStatusBadRequest400(message ...string) (events.APIGatewayProxyResponse, error) {
	resultCode := http.StatusBadRequest
	resultMessage := "Bad Request"
	if message != nil {
		resultMessage = message[0]
	}
	return defaultResponseStatusMessage(resultCode, resultMessage)
}

// 400 Status Bad Request
func ResponseStatusNotFound404(message ...string) (events.APIGatewayProxyResponse, error) {
	resultCode := http.StatusNotFound
	resultMessage := "Not Found"
	if message != nil {
		resultMessage = message[0]
	}
	return defaultResponseStatusMessage(resultCode, resultMessage)
}

// 500 Error
func ResponseStatusInternalServerError500(message ...string) (events.APIGatewayProxyResponse, error) {
	resultCode := http.StatusInternalServerError
	resultMessage := "Internal Server Error"
	if message != nil {
		resultMessage = message[0]
	}
	return defaultResponseStatusMessage(resultCode, resultMessage)
}

// Body Handler Functions
func WriteBytesToAPIGatewayProxyResponse(b []byte) (events.APIGatewayProxyResponse, error) {
	headers := defaultResponseHeaders()
	headers["Content-Type"] = http.DetectContentType(b)
	return events.APIGatewayProxyResponse{
		StatusCode:      http.StatusOK,
		Body:            base64.StdEncoding.EncodeToString(b),
		IsBase64Encoded: true,
		Headers:         headers,
	}, nil
}

func WriteJSONStringToAPIGatewayProxyResponse(s string) (events.APIGatewayProxyResponse, error) {
	headers := defaultResponseHeaders()
	headers["Content-Type"] = "application/json"
	return events.APIGatewayProxyResponse{
		StatusCode:      http.StatusOK,
		Body:            s,
		IsBase64Encoded: false,
		Headers:         headers,
	}, nil
}
