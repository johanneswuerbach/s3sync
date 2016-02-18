## Faster S3 sync

s3cmd is slow, aws-cli can't handle prefixes, we can do better.

### Usage
```bash
$ s3sync s3://my-bucket/my-prefix dest-folder
```
