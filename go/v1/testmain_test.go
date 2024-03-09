package awsutils

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/testcontainers/testcontainers-go/wait"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const TestDataPath = "../test/testdata"

func TestMain(m *testing.M) {
	_ = os.Setenv("AWS_REGION", "us-east-2")
	_ = os.Setenv("AWS_DEFAULT_REGION", "us-east-2")
	_ = os.Setenv("AWS_ACCESS_KEY_ID", "test")
	_ = os.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	localstackContainer, err := testcontainers.GenericContainer(context.Background(),
		testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:      "localstack/localstack:latest",
				WaitingFor: wait.ForHealthCheck(),
			},
			Started: true,
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	localstackPort, err := localstackContainer.MappedPort(context.Background(), "4566/tcp")
	if err != nil {
		log.Fatal(err)
	}
	awsEndpointUrl := "http://localhost:" + localstackPort.Port()
	_ = os.Setenv("AWS_ENDPOINT_URL", awsEndpointUrl)

	awsSession := NewSessionFromAwsConfig(&aws.Config{
		Region:           aws.String(os.Getenv("AWS_DEFAULT_REGION")),
		Endpoint:         aws.String(os.Getenv("AWS_ENDPOINT_URL")),
		S3ForcePathStyle: aws.Bool(true),
	})

	err = setupLocalstackS3Objects(awsSession, "../test/testdata/localstack/s3")
	if err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	_ = localstackContainer.Terminate(context.Background())

	os.Exit(code)
}

func setupLocalstackS3Objects(awsSession *session.Session, s3Path string) error {
	log.Println("=== initializeLocalstackS3Objects")
	defer func() {
		log.Println("=== initializeLocalstackS3Objects complete")
	}()

	entries, err := os.ReadDir(s3Path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		bucket := entry.Name()
		log.Println("--- Creating bucket:", bucket)
		_, err := NewS3Bucket(awsSession, bucket)
		if err != nil {
			return err
		}

		bucketPath := s3Path + "/" + bucket

		err = filepath.WalkDir(bucketPath, func(filepath string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}

			b, err := os.ReadFile(filepath)
			if err != nil {
				return err
			}

			objectKey := strings.ReplaceAll(filepath, bucketPath+"/", "")

			log.Println("--- Creating object:", objectKey)
			s3Object, err := NewS3Object(awsSession, bucket, objectKey)
			if err != nil {
				return err
			}
			err = s3Object.UploadBytes(b)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}
