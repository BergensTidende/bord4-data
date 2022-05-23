import boto3

"""
Utils for working with S3 from notebooks
"""

def publish_to_s3(bucket, folder, report_name, report_date, data, content_type='application/json', file_extension='json', skip_archive=False):
    """
    Pushes a file to a public S3 folder and sets cache headers and surrogate control
    Saves an archived versions aswell.
    NB: All files are set with ACL public-read

    :param bucket: Name of the S3 bucket.
    :param folder: The folder in the bucket
    :param report_name: The name of this report type
    :param report_date The report_date in YYYY-MM-DD
    :param datadata The text content to save (not bytes)
    :param content_type The content type of the file. Defaults to application/json
    :param file_extension The file extension to add. Defaults to json

    """
    s3 = boto3.resource('s3')
    content_length = len(data)

    s3meta = {
          "surrogate-key": "bt bt_spesial bt_spesial_brann",
          "surrogate-control": "max-age: 1800",
          "cache-control": "max-age: 60"
        }

    #put latest version
    s3key = f"{folder}/{report_name}/{report_name}_latest.{file_extension}"
    print(f"https://www.bt.no/{s3key}")
    s3object = s3.Object(bucket, s3key)
    s3object.put(
        Body=(bytes(data.encode('UTF-8'))), ACL='public-read', ContentType=content_type, Metadata=s3meta
    )

    if not skip_archive:
      #put archived version
      s3key = f"{folder}/{report_name}/archive/{report_date}_{report_name}.{file_extension}"
      print(f"https://www.bt.no/{s3key}")
      s3object = s3.Object(bucket, s3key)
      s3object.put(
          Body=(bytes(data.encode('UTF-8'))), ACL='public-read', ContentType=content_type, Metadata=s3meta
      )

def save_to_s3(bucket, key=None, filecontent=None):
    """
    Saves a file to S3 bucket. No metadata is set

    :param bucket: Name of the S3 bucket.
    :param key: Key of the file
    :param filecontent: The binary filecontent to save
    """
    if bucket and key and filecontent:
        s3 = boto3.resource('s3')
        s3object = s3.Object(bucket, key)
        s3object.put(
            Body=filecontent,
        )
    else:
        raise('Missing parameters')


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

