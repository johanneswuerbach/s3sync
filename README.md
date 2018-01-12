## Faster S3 sync

s3cmd is slow, aws-cli can't handle prefixes, we can do better.

### Build

Building requires [glide](https://glide.sh/) and go `1.6+` to be installed.

```bash
$ make dependencies
$ make build
```

### Usage
```bash
# Sync all objects to from my-bucket starting with my-prefix to dest-folder
$ s3sync s3://my-bucket/my-prefix dest-folder
# Server-side copy bucket A to bucket B
$ s3sync s3://A/my-prefix s3://B
```
