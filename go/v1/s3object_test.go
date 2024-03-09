package awsutils

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"log"
	"net/url"
	"strings"
	"testing"
)

func TestNewS3Object(t *testing.T) {
	var awsSession *session.Session
	var sourceBucket, sourceObjectKey string
	s3Object, err := NewS3Object(awsSession, sourceBucket, sourceObjectKey)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	log.Println(&s3Object)
}

func TestS3Copy(t *testing.T) {
	var awsSession *session.Session
	var targetBucket, targetObjectKey, testS3Url string

	s3Object, err := NewS3ObjectFromS3Url(awsSession, testS3Url)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	targetS3Object, err := NewS3Object(awsSession, targetBucket, targetObjectKey)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	err = s3Object.Copy(targetS3Object)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}

func TestRename(t *testing.T) {
	testObjectKey := "SAMPLESPACE+FILE.txt"
	//decodedObjectKey := strings.ReplaceAll(testObjectKey, "+", " ")
	decodedObjectKey, _ := url.QueryUnescape(testObjectKey)
	log.Println(decodedObjectKey)

	var awsSession *session.Session
	var sourceBucket string

	s3Object, err := NewS3Object(awsSession, sourceBucket, decodedObjectKey)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	log.Println(s3Object)

	newName := strings.ReplaceAll(decodedObjectKey, " ", "_")
	log.Println(newName)
	err = s3Object.Rename(newName)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}

func TestGetObject(t *testing.T) {
	var awsSession *session.Session
	var sourceBucket, sourceObjectKey string
	var targetBucket, targetObjectKey string

	s3Object, err := NewS3Object(awsSession, sourceBucket, sourceObjectKey)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}

	targetS3Object, err := NewS3Object(awsSession, targetBucket, targetObjectKey)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
	err = s3Object.MultipartCopy(targetS3Object)
	if err != nil {
		log.Println(err)
		t.FailNow()
	}
}
