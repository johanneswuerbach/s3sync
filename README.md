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
$ s3sync s3://my-bucket/my-prefix dest-folder
```
