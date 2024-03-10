def split_s3_url(url: str) -> (str, str, Exception):
    tokens = url.split("//")
    if len(tokens) < 2:
        return '', '', ValueError("invalid S3 URL: must be of format s3://bucket-name/key-name")

    tokens = tokens[1].split("/")
    if len(tokens) < 2:
        return '', '', ValueError("invalid S3 URL: must be of format s3://bucket-name/key-name")

    return tokens[0], '/'.join(tokens[1:]), None
