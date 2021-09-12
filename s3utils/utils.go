package s3utils

import (
	"errors"
	"log"
	"strings"
)

func SplitS3Url(url string) (string, string, error) {
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

func RenameUnsafeS3ObjectKey(s3ObjectKey string) (bool, string) {
	var targetObjectKey string

	s3ObjectKeySpaces := strings.Contains(s3ObjectKey, " ")
	s3ObjectKeyPeriodCount := strings.Count(s3ObjectKey, ".")
	s3ObjectKeyAmpersands := strings.Contains(s3ObjectKey, "&")
	s3ObjectKeyParentheses := strings.Contains(s3ObjectKey, "(") || strings.Contains(s3ObjectKey, ")")
	s3ObjectKeyColons := strings.Contains(s3ObjectKey, ":")
	renameS3ObjectKey := s3ObjectKeySpaces || s3ObjectKeyPeriodCount > 1 ||
		s3ObjectKeyAmpersands || s3ObjectKeyParentheses || s3ObjectKeyColons

	if renameS3ObjectKey {
		targetObjectKey = s3ObjectKey

		log.Println("--- Checking for Spaces in Object Key ---")
		if s3ObjectKeySpaces {
			log.Println("| Correcting Object Key for spaces")
			targetObjectKey = strings.ReplaceAll(targetObjectKey, " ", "_")
		}
		log.Println("--- Object Key Space Check Complete ---")

		log.Println("--- Checking for Extra Period Characters in Object Key ---")
		if s3ObjectKeyPeriodCount > 1 {
			log.Println("| Correcting Object Key for period count")
			targetObjectKey = strings.Replace(targetObjectKey, ".", "_", s3ObjectKeyPeriodCount-1)
		}
		log.Println("--- Object Key Period Check Complete ---")

		log.Println("--- Checking for Ampersand Characters in Object Key ---")
		if s3ObjectKeyAmpersands {
			log.Println("| Correcting Object Key for ampersand characters")
			targetObjectKey = strings.ReplaceAll(targetObjectKey, "&", "_")
		}
		log.Println("--- Object Key Ampersand Check Complete ---")

		log.Println("--- Checking for Parenthesis Characters in Object Key ---")
		if s3ObjectKeyParentheses {
			log.Println("| Correcting Object Key for parenthesis characters")
			targetObjectKey = strings.ReplaceAll(targetObjectKey, "(", "_")
			targetObjectKey = strings.ReplaceAll(targetObjectKey, ")", "_")
		}
		log.Println("--- Object Key Parenthesis Check Complete ---")

		log.Println("--- Checking for Colon Characters in Object Key ---")
		if s3ObjectKeyColons {
			log.Println("| Correcting Object Key for colon characters")
			targetObjectKey = strings.ReplaceAll(targetObjectKey, ":", "_")
		}
		log.Println("--- Object Key Colon Check Complete ---")
	}

	return renameS3ObjectKey, targetObjectKey
}
