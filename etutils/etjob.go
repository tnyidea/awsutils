package etutils

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/elastictranscoder"
	"github.com/fubotv/smo-content-operations/utils/awsutils/s3utils"
	"log"
)

type ElasticTranscoderJob struct {
	ServiceKey      string           `json:"-"` // Should be private for output
	PipelineId      string           `json:"pipelineId"`
	PresetId        string           `json:"presetId"`
	InputS3Object   s3utils.S3Object `json:"inputS3Object"`
	OutputS3Object  s3utils.S3Object `json:"outputS3Object"`
	OutputOverwrite bool             `json:"outputOverwrite"`
}

func (p *ElasticTranscoderJob) Submit() (string, error) {
	etSession, err := NewElasticTranscoderSession(p.ServiceKey)
	if err != nil {
		return "", err
	}

	// Elastic Transcoder will fail if the output file exists
	// We give the option to overwrite; if true delete the output file
	if p.OutputS3Object.Exists && p.OutputOverwrite {
		err = p.OutputS3Object.Delete()
		if err != nil {
			log.Println(err)
		}
	}

	response, err := etSession.CreateJob(&elastictranscoder.CreateJobInput{
		Input: &elastictranscoder.JobInput{
			AspectRatio:        nil,
			Container:          nil,
			DetectedProperties: nil,
			Encryption:         nil,
			FrameRate:          nil,
			InputCaptions:      nil,
			Interlaced:         nil,
			Key:                aws.String(p.InputS3Object.ObjectKey),
			Resolution:         nil,
			TimeSpan:           nil,
		},
		Inputs:          nil,
		Output:          nil,
		OutputKeyPrefix: nil,
		Outputs: []*elastictranscoder.CreateJobOutput{
			{
				AlbumArt:            nil,
				Captions:            nil,
				Composition:         nil,
				Encryption:          nil,
				Key:                 aws.String(p.OutputS3Object.ObjectKey),
				PresetId:            aws.String(p.PresetId),
				Rotate:              nil,
				SegmentDuration:     nil,
				ThumbnailEncryption: nil,
				ThumbnailPattern:    nil,
				Watermarks:          nil,
			},
		},
		PipelineId: aws.String(p.PipelineId),
		Playlists:  nil,
	})
	if err != nil {
		return "", err
	}

	return *response.Job.Id, nil
}
