package v1

import (
	"errors"
	"strings"
)

func newS3UrlFromS3Bucket(v S3Bucket) (string, error) {
	if v.Bucket == "" || v.Prefix == "" {
		return "", errors.New("invalid S3 URL: must specify both Bucket and Prefix")
	}

	return "s3://" + v.Bucket + "/" + v.Prefix, nil
}

func newS3UrlFromS3Object(v S3Object) (string, error) {
	if v.Bucket == "" || v.ObjectKey == "" {
		return "", errors.New("invalid S3 URL: must specify both Bucket and ObjectKey")
	}

	return "s3://" + v.Bucket + "/" + v.ObjectKey, nil
}

func splitS3Url(url string) (string, string, error) {
	tokens := strings.Split(url, "//")
	if len(tokens) != 2 {
		return "", "", errors.New("invalid S3 URL: must be of format s3://bucket-name/key-name")
	}

	tokens = strings.Split(tokens[1], "/")
	if len(tokens) < 2 {
		return "", "", errors.New("invalid S3 URL: must be of format s3://bucket-name/key-name")
	}

	return tokens[0], strings.Join(tokens[1:], "/"), nil
}
