package v1

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"strings"
	"time"
)

type S3Bucket struct {
	AwsSession *session.Session `json:"-"`
	Region     string           `json:"region"`
	Bucket     string           `json:"bucket"`
	Prefix     string           `json:"prefix"`
}

func NewS3Bucket(awsSession *session.Session, bucket string, prefix ...string) (S3Bucket, error) {
	var prefixString string
	if prefix != nil {
		prefixString = prefix[0]
	}

	s3Session := s3.New(awsSession)

	_, err := s3Session.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		log.Println("here")
		return S3Bucket{}, err
	}

	region, err := getBucketRegion(awsSession, bucket)
	if err != nil {
		return S3Bucket{}, err
	}

	return S3Bucket{
		AwsSession: awsSession,
		Region:     region,
		Bucket:     bucket,
		Prefix:     prefixString,
	}, nil
}

func NewS3BucketFromS3Url(awsSession *session.Session, url string) (S3Bucket, error) {
	bucket, prefix, err := splitS3Url(url)
	if err != nil {
		return S3Bucket{}, err
	}

	return S3Bucket{
		AwsSession: awsSession,
		Bucket:     bucket,
		Prefix:     prefix,
	}, nil
}

func (p *S3Bucket) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

func getBucketRegion(awsSession *session.Session, bucket string) (string, error) {
	s3Session := s3.New(awsSession)
	output, err := s3Session.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", err
	}

	return aws.StringValue(output.BucketRegion), nil
}

func (p *S3Bucket) S3Url() (string, error) {
	return newS3UrlFromS3Bucket(*p)
}

func (p *S3Bucket) GetTotalSize() (int64, int64, error) {
	s3Session := s3.New(p.AwsSession)

	var count int64 = 0
	var totalSize int64 = 0
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(p.Bucket),
			Prefix:     aws.String(p.Prefix),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				count++
				totalSize += *page.Contents[i].Size
			}
			startAfter = *page.Contents[len(page.Contents)-1].Key
			isTruncated = *page.IsTruncated

			return false
		})
		if err != nil {
			return 0, 0, err
		}

		if !isTruncated {
			break
		}
	}

	return count, totalSize, nil
}

func (p *S3Bucket) ListObjects() ([]S3Object, error) {
	s3Session := s3.New(p.AwsSession)

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(p.Bucket),
			Prefix:     aws.String(p.Prefix),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				pageContents := page.Contents[i]

				objectKey := *pageContents.Key
				var fileExtension string
				var fileType string
				tokens := strings.Split(objectKey, ".")
				if len(tokens) > 1 {
					fileExtension = "." + tokens[len(tokens)-1]
					fileType = strings.ToLower(fileExtension)
				}
				objectList = append(objectList, S3Object{
					AwsSession:    p.AwsSession,
					Bucket:        p.Bucket,
					ObjectKey:     objectKey,
					FileExtension: fileExtension,
					FileType:      fileType,
					ETag:          strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
					Size:          *pageContents.Size,
					StorageClass:  *pageContents.StorageClass,
					LastModified:  *pageContents.LastModified,
				})
			}
			startAfter = *page.Contents[len(page.Contents)-1].Key
			isTruncated = *page.IsTruncated

			return false
		})
		if err != nil {
			return nil, err
		}

		if !isTruncated {
			break
		}
	}

	return objectList, nil
}

func (p *S3Bucket) ListObjectsAfterTime(afterTime time.Time) ([]S3Object, error) {
	s3Session := s3.New(p.AwsSession)

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(p.Bucket),
			Prefix:     aws.String(p.Prefix),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).After(afterTime) {
					pageContents := page.Contents[i]

					objectKey := *pageContents.Key
					var fileExtension string
					var fileType string
					tokens := strings.Split(objectKey, ".")
					if len(tokens) > 1 {
						fileExtension = "." + tokens[len(tokens)-1]
						fileType = strings.ToLower(fileExtension)
					}
					objectList = append(objectList, S3Object{
						AwsSession:    p.AwsSession,
						Bucket:        p.Bucket,
						ObjectKey:     objectKey,
						FileExtension: fileExtension,
						FileType:      fileType,
						ETag:          strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
						Size:          *pageContents.Size,
						StorageClass:  *pageContents.StorageClass,
						LastModified:  *pageContents.LastModified,
					})
				}
			}
			if len(page.Contents) == 0 {
				return true
			}

			startAfter = *page.Contents[len(page.Contents)-1].Key
			isTruncated = *page.IsTruncated

			return false
		})
		if err != nil {
			return nil, err
		}

		if !isTruncated {
			break
		}
	}

	return objectList, nil
}

func (p *S3Bucket) ListObjectsBeforeTime(beforeTime time.Time) ([]S3Object, error) {
	s3Session := s3.New(p.AwsSession)

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(p.Bucket),
			Prefix:     aws.String(p.Prefix),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).Before(beforeTime) {
					pageContents := page.Contents[i]

					objectKey := *pageContents.Key
					var fileExtension string
					var fileType string
					tokens := strings.Split(objectKey, ".")
					if len(tokens) > 1 {
						fileExtension = "." + tokens[len(tokens)-1]
						fileType = strings.ToLower(fileExtension)
					}
					objectList = append(objectList, S3Object{
						AwsSession:    p.AwsSession,
						Bucket:        p.Bucket,
						ObjectKey:     objectKey,
						FileExtension: fileExtension,
						FileType:      fileType,
						ETag:          strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
						Size:          *pageContents.Size,
						StorageClass:  *pageContents.StorageClass,
						LastModified:  *pageContents.LastModified,
					})
				}
			}
			startAfter = *page.Contents[len(page.Contents)-1].Key
			isTruncated = *page.IsTruncated

			return false
		})
		if err != nil {
			return nil, err
		}

		if !isTruncated {
			break
		}
	}

	return objectList, nil
}

func (p *S3Bucket) ListObjectsBetweenTimes(afterTime time.Time, beforeTime time.Time) ([]S3Object, error) {
	s3Session := s3.New(p.AwsSession)

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(p.Bucket),
			Prefix:     aws.String(p.Prefix),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).After(afterTime) && (*page.Contents[i].LastModified).Before(beforeTime) {
					pageContents := page.Contents[i]

					objectKey := *pageContents.Key
					var fileExtension string
					var fileType string
					tokens := strings.Split(objectKey, ".")
					if len(tokens) > 1 {
						fileExtension = "." + tokens[len(tokens)-1]
						fileType = strings.ToLower(fileExtension)
					}
					objectList = append(objectList, S3Object{
						AwsSession:    p.AwsSession,
						Bucket:        p.Bucket,
						ObjectKey:     objectKey,
						FileExtension: fileExtension,
						FileType:      fileType,
						ETag:          strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
						Size:          *pageContents.Size,
						StorageClass:  *pageContents.StorageClass,
						LastModified:  *pageContents.LastModified,
					})
				}
			}
			if len(page.Contents) == 0 {
				return true
			}

			startAfter = *page.Contents[len(page.Contents)-1].Key
			isTruncated = *page.IsTruncated

			return false
		})
		if err != nil {
			return nil, err
		}

		if !isTruncated {
			break
		}
	}

	return objectList, nil
}
