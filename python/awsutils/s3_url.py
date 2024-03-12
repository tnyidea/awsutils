def split_s3_url(url: str) -> dict:
    tokens = url.split("//")
    if len(tokens) < 2:
        raise ValueError("invalid S3 URL: must be of format s3://bucket-name/key-name")

    tokens = tokens[1].split("/")
    if len(tokens) < 2:
        raise ValueError("invalid S3 URL: must be of format s3://bucket-name/key-name")

    return {'bucket': tokens[0], 'objectKey': '/'.join(tokens[1:])}
