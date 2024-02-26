package v1

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const S3ObjectMakePublic bool = true

type S3Object struct {
	AwsSession    *session.Session `json:"-"`
	Region        string           `json:"region"`
	Bucket        string           `json:"bucket"`
	ObjectKey     string           `json:"objectKey"`
	FileExtension string           `json:"fileExtension"`
	FileType      string           `json:"fileType"`
	Exists        bool             `json:"exists"`
	ETag          string           `json:"etag"`
	Size          int64            `json:"size"`
	StorageClass  string           `json:"storageClass"`
	LastModified  time.Time        `json:"lastModified"`
	EventName     string           `json:"eventName"`
}

func NewS3Object(awsSession *session.Session, bucket string, objectKey string) (S3Object, error) {
	s3Object := S3Object{
		AwsSession: awsSession,
		Bucket:     bucket,
		ObjectKey:  objectKey,
		Exists:     true,
	}

	tokens := strings.Split(objectKey, ".")
	if len(tokens) > 1 {
		fileExtension := tokens[len(tokens)-1]
		s3Object.FileExtension = "." + fileExtension
		s3Object.FileType = "." + strings.ToLower(fileExtension)
	}

	bucketRegion, err := getBucketRegion(awsSession, s3Object.Bucket)
	if err != nil {
		return S3Object{}, errors.New("error locating bucket region: " + err.Error())
	}
	s3Object.Region = bucketRegion

	err = s3Object.listObjectV2()
	if err != nil {
		var awsError awserr.Error
		if errors.As(err, &awsError) {
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

func NewS3ObjectFromS3Url(awsSession *session.Session, url string) (S3Object, error) {
	bucket, objectKey, err := splitS3Url(url)
	if err != nil {
		return S3Object{}, err
	}

	return NewS3Object(awsSession, bucket, objectKey)
}

func NewS3ObjectFromS3EventBytes(awsSession *session.Session, s3EventBytes []byte) (S3Object, error) {
	var s3EventMessage S3EventMessage
	err := json.Unmarshal(s3EventBytes, &s3EventMessage)
	if err != nil {
		return S3Object{}, err
	}

	return NewS3ObjectFromS3EventMessage(awsSession, s3EventMessage)
}

func NewS3ObjectFromS3EventMessage(awsSession *session.Session, s3EventMessage S3EventMessage) (S3Object, error) {
	record := s3EventMessage.Records[0]
	s3EventType := record.EventName

	bucket := record.S3.Bucket.Name
	objectKey := record.S3.Object.Key
	if objectKey == "" {
		return S3Object{}, errors.New("error: Object key is empty")
	}

	s3Object, err := NewS3Object(awsSession, bucket, objectKey)
	if err != nil {
		return S3Object{}, err
	}
	s3Object.EventName = s3EventType

	return s3Object, nil
}

func (p *S3Object) String() string {
	b, _ := json.MarshalIndent(p, "", "    ")
	return string(b)
}

func (p *S3Object) Filename() (string, error) {
	if p.ObjectKey == "" {
		return "", errors.New("invalid S3Object: ObjectKey is empty")
	}

	tokens := strings.Split(p.ObjectKey, "/")
	return tokens[len(tokens)-1], nil
}

func (p *S3Object) S3Url() (string, error) {
	return newS3UrlFromS3Object(*p)
}

func (p *S3Object) listObjectV2() error {
	s3Session := s3.New(p.AwsSession)

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
	s3Session := s3.New(p.AwsSession)

	var aclInput *string
	if acl != nil {
		aclInput = aws.String(acl[0])
	}
	_, err := s3Session.CopyObject(&s3.CopyObjectInput{
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
	if source.Region != target.Region {
		return p.crossRegionMultipartCopy(target)
	}

	s3Session := s3.New(p.AwsSession)

	sourceHeadObjectOutput, err := s3Session.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(source.Bucket),
		Key:    aws.String(source.ObjectKey),
	})
	if err != nil {
		return err
	}

	sourceObjectSize := *sourceHeadObjectOutput.ContentLength
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

	sourceS3Session := s3.New(source.AwsSession)
	targetS3Session := s3.New(target.AwsSession)

	sourceHeadObjectResult, err := sourceS3Session.HeadObject(&s3.HeadObjectInput{
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

	downloader := s3manager.NewDownloaderWithClient(sourceS3Session,
		func(d *s3manager.Downloader) {
			d.PartSize = partSize
		})

	var aclInput *string
	if acl != nil {
		aclInput = aws.String(acl[0])
	}
	uploader, err := targetS3Session.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
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

		partResult, err := targetS3Session.UploadPart(&s3.UploadPartInput{
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

	_, err = targetS3Session.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
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
	s3Session := s3.New(p.AwsSession)

	_, err := s3Session.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(p.Bucket),
		Key:    aws.String(p.ObjectKey),
	})
	if err != nil {
		return err
	}

	return nil
}

func (p *S3Object) Rename(targetObjectKey string, acl ...string) error {
	// TODO have Rename replace the contents of p with target

	target := *p
	target.ObjectKey = targetObjectKey
	err := p.MultipartCopy(target, acl...)
	if err != nil {
		return err
	}
	return p.Delete()
}

func (p *S3Object) DownloadBytes() ([]byte, error) {
	var b []byte
	s3DownloadBuffer := aws.NewWriteAtBuffer(b)
	s3Downloader := s3manager.NewDownloader(p.AwsSession)
	_, err := s3Downloader.Download(s3DownloadBuffer,
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
	s3DownloadBuffer := aws.NewWriteAtBuffer([]byte{})
	s3Downloader := s3manager.NewDownloader(p.AwsSession)
	_, err := s3Downloader.Download(s3DownloadBuffer,
		&s3.GetObjectInput{
			Bucket: aws.String(p.Bucket),
			Key:    aws.String(p.ObjectKey),
		})
	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(bytes.NewReader(s3DownloadBuffer.Bytes())), nil
}

func (p *S3Object) UploadBytes(uploadBytes []byte, acl ...string) error {
	var aclInput *string
	if acl != nil {
		aclInput = aws.String(acl[0])
	}
	s3Uploader := s3manager.NewUploader(p.AwsSession)
	_, err := s3Uploader.Upload(&s3manager.UploadInput{
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
	var aclInput *string
	if acl != nil {
		aclInput = aws.String(acl[0])
	}

	s3Uploader := s3manager.NewUploader(p.AwsSession)
	_, err := s3Uploader.Upload(&s3manager.UploadInput{
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
