package s3utils

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"log"
	"testing"
)

func TestGetTotalSize(t *testing.T) {
	var awsSession *session.Session
	var sourceObjectPrefix string
	prefix, err := NewS3ObjectKeyPrefixFromS3Url(awsSession, sourceObjectPrefix)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	count, totalSize, err := prefix.GetTotalSize()
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(count)
	log.Println(totalSize)
}
