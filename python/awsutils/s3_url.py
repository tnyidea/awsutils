import awsutils


def new_s3_url_from_s3_bucket(s3_bucket: awsutils.S3Bucket) -> (str, Exception):
    if s3_bucket.bucket == '' or s3_bucket.prefix == '':
        return '', ValueError('invalid S3Bucket: must specify both bucket and prefix values')

    return f's3://{s3_bucket.bucket}/{s3_bucket.prefix}', None


def new_s3_url_from_s3_object(s3_object: awsutils.S3Object) -> (str, Exception):
    if s3_object.bucket == '' or s3_object.object_key == '':
        return '', ValueError('invalid S3Object: must specify both bucket and object_key values')

    return f's3://{s3_object.bucket}/{s3_object.object_key}', None


def split_s3_url(url: str) -> (str, str, Exception):
    tokens = url.split("//")
    if len(tokens) < 2:
        return '', '', ValueError("invalid S3 URL: must be of format s3://bucket-name/key-name")

    tokens = tokens[1].split("/")
    if len(tokens) < 2:
        return '', '', ValueError("invalid S3 URL: must be of format s3://bucket-name/key-name")

    return tokens[0], '/'.join(tokens[1:]), None
