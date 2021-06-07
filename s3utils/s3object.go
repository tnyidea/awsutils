package s3utils

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/gbnyc26/awsutils-go"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const S3ObjectMakePublic bool = true

type S3Object struct {
	ServiceKey   string    `json:"-"` // Should be private for output
	Region       string    `json:"region"`
	Bucket       string    `json:"bucket"`
	ObjectKey    string    `json:"objectKey"`
	Exists       bool      `json:"exists"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	StorageClass string    `json:"storageClass"`
	LastModified time.Time `json:"lastModified"`
}

func NewS3Object(bucket string, objectKey string, serviceKey string) (S3Object, error) {
	s3Object := S3Object{
		ServiceKey: serviceKey,
		Bucket:     bucket,
		ObjectKey:  objectKey,
		Exists:     true,
	}

	region, err := getBucketRegion(s3Object.Bucket, serviceKey)
	if err != nil {
		return S3Object{}, errors.New("error locating bucket region: " + err.Error())
	}
	s3Object.Region = region
	tokens := strings.Split(s3Object.ServiceKey, ":")
	tokens[0] = s3Object.Region
	s3Object.ServiceKey = strings.Join(tokens, ":")

	err = s3Object.listObjectV2()
	if err != nil {
		if awsError, defined := err.(awserr.Error); defined {
			code := awsError.Code()
			if code == s3.ErrCodeNoSuchKey {
				s3Object.Exists = false
				return s3Object, nil
			}
		}
		return S3Object{}, err
	}

	return s3Object, nil
}

func NewS3ObjectFromS3Url(url string, serviceKey string) (S3Object, error) {
	tokens := strings.Split(url, "//")
	if tokens[0] != "s3:" {
		return S3Object{}, errors.New("invalid S3 URL: invalid protocol '" + tokens[0] +
			"'. S3 URL Must be in the form of s3://bucket_name/object_key")
	}

	tokens = strings.Split(tokens[1], "/")
	if len(tokens) == 1 {
		return S3Object{}, errors.New("invalid S3 URL: missing object key or bucket. S3 URL Must be in the form of s3://bucket_name/object_key")
	}

	return NewS3Object(tokens[0], strings.Join(tokens[1:], "/"), serviceKey)
}

func (p *S3Object) Bytes() []byte {
	b, _ := json.Marshal(p)
	return b
}

func (p *S3Object) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

func (p *S3Object) IsZero() bool {
	return reflect.DeepEqual(*p, S3Object{})
}

func (p *S3Object) Filename() string {
	tokens := strings.Split(p.ObjectKey, "/")
	return tokens[len(tokens)-1]
}

func (p *S3Object) S3Url() (string, error) {
	if p.Bucket == "" || p.ObjectKey == "" {
		return "", errors.New("invalid S3 URL: must specify both Bucket and Object Key")
	}

	return "s3://" + p.Bucket + "/" + p.ObjectKey, nil
}

func (p *S3Object) listObjectV2() error {
	s3Session, err := NewS3Session(p.ServiceKey)
	if err != nil {
		return err
	}

	output, err := s3Session.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:  aws.String(p.Bucket),
		MaxKeys: aws.Int64(1),
		Prefix:  aws.String(p.ObjectKey),
	})
	if err != nil {
		return err
	}

	if *output.KeyCount != 1 {
		return awserr.New(s3.ErrCodeNoSuchKey, "No Such Key", errors.New("no such key found"))
	}
	object := output.Contents[0]

	p.ETag = strings.ReplaceAll(*object.ETag, "\"", "")
	p.Size = *object.Size
	// p.Owner = *object.Owner.DisplayName
	p.StorageClass = *object.StorageClass
	p.LastModified = *object.LastModified

	return nil
}

func (p *S3Object) Copy(target S3Object, acl ...string) error {
	s3Session, err := NewS3Session(p.ServiceKey)
	if err != nil {
		return err
	}

	var aclInput *string
	if acl != nil {
		aclInput = aws.String(acl[0])
	}
	_, err = s3Session.CopyObject(&s3.CopyObjectInput{
		ACL:        aclInput,
		CopySource: aws.String("/" + p.Bucket + "/" + p.ObjectKey),
		Bucket:     aws.String(target.Bucket),
		Key:        aws.String(target.ObjectKey),
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *S3Object) MultipartCopy(target S3Object, acl ...string) error {
	source := *p
	if (source.ServiceKey != target.ServiceKey) || source.Region != target.Region {
		return p.crossRegionMultipartCopy(target)
	}

	s3Session, err := NewS3Session(p.ServiceKey)
	if err != nil {
		return err
	}

	sourceHeadObjectResult, err := s3Session.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(source.Bucket),
		Key:    aws.String(source.ObjectKey),
	})
	if err != nil {
		return err
	}

	sourceObjectSize := *sourceHeadObjectResult.ContentLength
	// partSize := int64(math.Pow(1024, 3)) // 1 GiB
	partSize := int64(math.Pow(1024, 2) * 100) // 100 MiB
	partNumber := int64(1)

	var aclInput *string
	if acl != nil {
		aclInput = aws.String(acl[0])
	}

	uploader, err := s3Session.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		ACL:    aclInput,
		Bucket: aws.String(target.Bucket),
		Key:    aws.String(target.ObjectKey),
	})
	if err != nil {
		return err
	}

	log.Println("==Starting Multipart Copy==")
	log.Println("Source File Size:", sourceObjectSize)
	log.Println("Part Size:", partSize)

	var completedParts []*s3.CompletedPart
	for bytePosition := int64(0); bytePosition < sourceObjectSize; bytePosition += partSize {
		lastByte := int64(math.Min(float64(bytePosition+partSize-1), float64(sourceObjectSize-1)))
		byteRangeString := "bytes=" + strconv.FormatInt(bytePosition, 10) + "-" + strconv.FormatInt(lastByte, 10)
		log.Println("Copying Part Number", partNumber, ": Byte Range:", byteRangeString)

		partResult, err := s3Session.UploadPartCopy(&s3.UploadPartCopyInput{
			Bucket:          aws.String(target.Bucket),
			CopySource:      aws.String(url.PathEscape("/" + source.Bucket + "/" + source.ObjectKey)),
			CopySourceRange: aws.String(byteRangeString),
			Key:             aws.String(target.ObjectKey),
			PartNumber:      aws.Int64(partNumber),
			UploadId:        uploader.UploadId,
		})
		if err != nil {
			return err
		}

		completedParts = append(completedParts, &s3.CompletedPart{
			ETag:       partResult.CopyPartResult.ETag,
			PartNumber: aws.Int64(partNumber),
		})
		partNumber++
	}

	_, err = s3Session.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket: aws.String(target.Bucket),
		Key:    aws.String(target.ObjectKey),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
		UploadId: uploader.UploadId,
	})
	if err != nil {
		return err
	}

	log.Println("==Multipart Copy Complete==")
	return nil
}

func (p *S3Object) crossRegionMultipartCopy(target S3Object, acl ...string) error {
	source := *p

	sourceSession, err := NewS3Session(p.ServiceKey)
	if err != nil {
		return err
	}
	targetSession, err := NewS3Session(target.ServiceKey)
	if err != nil {
		return err
	}

	sourceHeadObjectResult, err := sourceSession.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(source.Bucket),
		Key:    aws.String(source.ObjectKey),
	})
	if err != nil {
		return err
	}

	sourceObjectSize := *sourceHeadObjectResult.ContentLength
	// partSize := int64(math.Pow(1024, 3)) // 1 GiB
	partSize := int64(math.Pow(1024, 2) * 100) // 100 MiB
	partNumber := int64(1)

	downloader := s3manager.NewDownloaderWithClient(sourceSession,
		func(d *s3manager.Downloader) {
			d.PartSize = partSize
		})

	var aclInput *string
	if acl != nil {
		aclInput = aws.String(acl[0])
	}
	uploader, err := targetSession.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		ACL:    aclInput,
		Bucket: aws.String(target.Bucket),
		Key:    aws.String(target.ObjectKey),
	})
	if err != nil {
		return err
	}

	log.Println("==Starting Multipart Copy==")
	log.Println("Source File Size:", sourceObjectSize)
	log.Println("Part Size:", partSize)

	var completedParts []*s3.CompletedPart
	var buffer []byte
	writeBuffer := aws.NewWriteAtBuffer(buffer)
	for bytePosition := int64(0); bytePosition < sourceObjectSize; bytePosition += partSize {
		lastByte := int64(math.Min(float64(bytePosition+partSize-1), float64(sourceObjectSize-1)))
		byteRangeString := "bytes=" + strconv.FormatInt(bytePosition, 10) + "-" + strconv.FormatInt(lastByte, 10)
		log.Println("Copying Part Number", partNumber, ": Byte Range:", byteRangeString)

		_, err := downloader.Download(writeBuffer, &s3.GetObjectInput{
			Bucket: aws.String(source.Bucket),
			Key:    aws.String(source.ObjectKey),
			Range:  aws.String(byteRangeString),
		})
		if err != nil {
			return err
		}

		partResult, err := targetSession.UploadPart(&s3.UploadPartInput{
			Body:          bytes.NewReader(writeBuffer.Bytes()),
			Bucket:        aws.String(target.Bucket),
			ContentLength: aws.Int64(partSize),
			Key:           aws.String(target.ObjectKey),
			PartNumber:    aws.Int64(partNumber),
			UploadId:      uploader.UploadId,
		})
		if err != nil {
			return err
		}

		completedParts = append(completedParts, &s3.CompletedPart{
			ETag:       partResult.ETag,
			PartNumber: aws.Int64(partNumber),
		})
		partNumber++
	}

	_, err = targetSession.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket: aws.String(target.Bucket),
		Key:    aws.String(target.ObjectKey),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
		UploadId: uploader.UploadId,
	})
	if err != nil {
		return err
	}

	log.Println("==Multipart Copy Complete==")
	return nil
}

func (p *S3Object) Delete() error {
	s3Session, err := NewS3Session(p.ServiceKey)
	if err != nil {
		return err
	}

	_, err = s3Session.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(p.ObjectKey),
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *S3Object) DownloadBytes() ([]byte, error) {
	awsSession, err := awsutils.NewAWSSession(p.ServiceKey)
	if err != nil {
		return nil, err
	}

	var b []byte
	s3DownloadBuffer := aws.NewWriteAtBuffer(b)
	s3Downloader := s3manager.NewDownloader(awsSession)
	_, err = s3Downloader.Download(s3DownloadBuffer,
		&s3.GetObjectInput{
			Bucket: aws.String(p.Bucket),
			Key:    aws.String(p.ObjectKey),
		})
	if err != nil {
		return nil, err
	}

	return s3DownloadBuffer.Bytes(), nil
}

func (p *S3Object) DownloadReader() (io.ReadCloser, error) {
	awsSession, err := awsutils.NewAWSSession(p.ServiceKey)
	if err != nil {
		return nil, err
	}

	s3DownloadBuffer := aws.NewWriteAtBuffer([]byte{})
	s3Downloader := s3manager.NewDownloader(awsSession)
	_, err = s3Downloader.Download(s3DownloadBuffer,
		&s3.GetObjectInput{
			Bucket: aws.String(p.Bucket),
			Key:    aws.String(p.ObjectKey),
		})
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(bytes.NewReader(s3DownloadBuffer.Bytes())), nil
}

// TODO have Rename replace the contents of p with target
func (p *S3Object) Rename(targetObjectKey string, acl ...string) error {
	target := *p
	target.ObjectKey = targetObjectKey
	err := p.MultipartCopy(target, acl...)
	if err != nil {
		return err
	}
	return p.Delete()
}

func (p *S3Object) UploadBytes(uploadBytes []byte, acl ...string) error {
	awsSession, err := awsutils.NewAWSSession(p.ServiceKey)
	if err != nil {
		return err
	}

	var aclInput *string
	if acl != nil {
		aclInput = aws.String(acl[0])
	}
	s3Uploader := s3manager.NewUploader(awsSession)
	_, err = s3Uploader.Upload(&s3manager.UploadInput{
		ACL:    aclInput,
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(p.ObjectKey),
		Body:   bytes.NewReader(uploadBytes),
	})
	if err != nil {
		return err
	}
	return nil
}

func (p *S3Object) UploadReader(reader io.ReadCloser, acl ...string) error {
	awsSession, err := awsutils.NewAWSSession(p.ServiceKey)
	if err != nil {
		return err
	}

	var aclInput *string
	if acl != nil {
		aclInput = aws.String(acl[0])
	}

	s3Uploader := s3manager.NewUploader(awsSession)
	_, err = s3Uploader.Upload(&s3manager.UploadInput{
		ACL:    aclInput,
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(p.ObjectKey),
		Body:   reader,
	})
	if err != nil {
		return err
	}
	return nil
}

func (p *S3Object) WriteToHttpResponse(w http.ResponseWriter) error {
	b, err := p.DownloadBytes()
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", http.DetectContentType(b))
	_, err = w.Write(b)
	if err != nil {
		return err
	}

	return nil
}

func (p *S3Object) WriteBytesToAPIGatewayProxyResponse() (events.APIGatewayProxyResponse, error) {
	b, err := p.DownloadBytes()
	if err != nil {
		return events.APIGatewayProxyResponse{}, err
	}

	return events.APIGatewayProxyResponse{StatusCode: http.StatusOK,
			Body:            base64.StdEncoding.EncodeToString(b),
			IsBase64Encoded: true,
			Headers: map[string]string{"Access-Control-Allow-Origin": "'*'",
				"Content-Type": http.DetectContentType(b)}},
		nil
}
