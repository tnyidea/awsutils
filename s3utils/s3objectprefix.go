package s3utils

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"strings"
	"time"
)

type S3ObjectKeyPrefix struct {
	ServiceKey string `json:"-"` // Should be private for output
	Bucket     string `json:"bucket"`
	Value      string `json:"value"`
}

func NewS3ObjectPrefix(bucket string, objectKeyPrefix string, serviceKey string) (S3ObjectKeyPrefix, error) {
	return S3ObjectKeyPrefix{
		ServiceKey: serviceKey,
		Bucket:     bucket,
		Value:      objectKeyPrefix,
	}, nil
}

func NewS3ObjectKeyPrefixFromS3Url(url string, serviceKey string) (S3ObjectKeyPrefix, error) {
	tokens := strings.Split(url, "//")
	if tokens[0] != "s3:" {
		return S3ObjectKeyPrefix{}, errors.New("invalid S3 URL: invalid protocol '" + tokens[0] +
			"'. S3 URL Must be in the form of s3://bucket_name/object_key_prefix")
	}

	tokens = strings.Split(tokens[1], "/")
	if len(tokens) == 1 {
		return S3ObjectKeyPrefix{}, errors.New("invalid S3 URL: missing object prefix or bucket. S3 URL Must be in the form of s3://bucket_name/object_prefix")
	}

	return S3ObjectKeyPrefix{
		ServiceKey: serviceKey,
		Bucket:     tokens[0],
		Value:      strings.Join(tokens[1:], "/"),
	}, nil
}

func (p *S3ObjectKeyPrefix) Bytes() []byte {
	b, _ := json.MarshalIndent(p, "", "    ")
	return b
}

func (p *S3ObjectKeyPrefix) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

func (p *S3ObjectKeyPrefix) S3Url() (string, error) {
	if p.Bucket == "" || p.Value == "" {
		return "", errors.New("invalid S3 URL: must specify both Bucket and Object Prefix")
	}

	return "s3://" + p.Bucket + "/" + p.Value, nil
}

func (p *S3ObjectKeyPrefix) GetTotalSize() (int64, int64, error) {
	s3Session, err := NewS3Session(p.ServiceKey)
	if err != nil {
		return 0, 0, err
	}

	var count int64 = 0
	var totalSize int64 = 0
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(p.Bucket),
			Prefix:     aws.String(p.Value),
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

func (p *S3ObjectKeyPrefix) ListObjects() ([]S3Object, error) {
	s3Session, err := NewS3Session(p.ServiceKey)
	if err != nil {
		return nil, err
	}

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(p.Bucket),
			Prefix:     aws.String(p.Value),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				pageContents := page.Contents[i]
				objectList = append(objectList, S3Object{
					ServiceKey:   "",
					Bucket:       p.Bucket,
					ObjectKey:    *pageContents.Key,
					ETag:         strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
					Size:         *pageContents.Size,
					StorageClass: *pageContents.StorageClass,
					LastModified: *pageContents.LastModified,
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

func (p *S3ObjectKeyPrefix) ListObjectsAfterTime(afterTime time.Time) ([]S3Object, error) {
	s3Session, err := NewS3Session(p.ServiceKey)
	if err != nil {
		return nil, err
	}

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(p.Bucket),
			Prefix:     aws.String(p.Value),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).After(afterTime) {
					pageContents := page.Contents[i]
					objectList = append(objectList, S3Object{
						ServiceKey:   "",
						Bucket:       p.Bucket,
						ObjectKey:    *pageContents.Key,
						ETag:         strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
						Size:         *pageContents.Size,
						StorageClass: *pageContents.StorageClass,
						LastModified: *pageContents.LastModified,
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

func (p *S3ObjectKeyPrefix) ListObjectsBeforeTime(beforeTime time.Time) ([]S3Object, error) {
	s3Session, err := NewS3Session(p.ServiceKey)
	if err != nil {
		return nil, err
	}

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(p.Bucket),
			Prefix:     aws.String(p.Value),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).Before(beforeTime) {
					pageContents := page.Contents[i]
					objectList = append(objectList, S3Object{
						ServiceKey:   "",
						Bucket:       p.Bucket,
						ObjectKey:    *pageContents.Key,
						ETag:         strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
						Size:         *pageContents.Size,
						StorageClass: *pageContents.StorageClass,
						LastModified: *pageContents.LastModified,
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

func (p *S3ObjectKeyPrefix) ListObjectsBetweenTimes(afterTime time.Time, beforeTime time.Time) ([]S3Object, error) {
	s3Session, err := NewS3Session(p.ServiceKey)
	if err != nil {
		return nil, err
	}

	var objectList []S3Object
	var startAfter string
	var isTruncated bool

	for {
		err := s3Session.ListObjectsV2Pages(&s3.ListObjectsV2Input{
			Bucket:     aws.String(p.Bucket),
			Prefix:     aws.String(p.Value),
			FetchOwner: aws.Bool(true),
			StartAfter: aws.String(startAfter),
		}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			if *page.KeyCount == 0 {
				return true
			}
			for i := range page.Contents {
				if (*page.Contents[i].LastModified).After(afterTime) && (*page.Contents[i].LastModified).Before(beforeTime) {
					pageContents := page.Contents[i]
					objectList = append(objectList, S3Object{
						ServiceKey:   "",
						Bucket:       p.Bucket,
						ObjectKey:    *pageContents.Key,
						ETag:         strings.ReplaceAll(*page.Contents[i].ETag, "\"", ""),
						Size:         *pageContents.Size,
						StorageClass: *pageContents.StorageClass,
						LastModified: *pageContents.LastModified,
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

func (p *S3ObjectKeyPrefix) DeleteObjects() error {
	s3ObjectList, err := p.ListObjects()
	if err != nil {
		log.Println("List Error")
		return err
	}

	for i := range s3ObjectList {
		s3ObjectList[i].ServiceKey = p.ServiceKey
		err := s3ObjectList[i].Delete()
		if err != nil {
			log.Println("Delete Error")
			return err
		}
	}

	return nil
}
