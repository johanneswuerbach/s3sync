package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	concurrency = 10
	awsRegion   = "us-east-1"
)

var objectsCount = 0

func worker(id int, jobs <-chan *s3.Object, downloader *s3manager.Downloader, awsBucket string, destDir string) {
	for object := range jobs {
		os.MkdirAll(path.Dir(path.Join(destDir, *object.Key)), 0777)
		file, err := os.Create(path.Join(destDir, *object.Key))
		if err != nil {
			log.Fatal("Failed to create file", err)
		}
		defer file.Close()

		numBytes, err := downloader.Download(file,
			&s3.GetObjectInput{
				Bucket: aws.String(awsBucket),
				Key:    object.Key,
			})

		file.Close()

		if err != nil {
			fmt.Println("Failed to download file", err)
			return
		}

		fmt.Println("worker", id, "downloaded", file.Name(), numBytes, "bytes", objectsCount)
	}
}

func workerBucket(id int, jobs <-chan *s3.Object, svc *s3.S3, awsBucket, destBucket, destPrefix string) {
	for object := range jobs {
		fmt.Println("Copy", path.Join(awsBucket, *object.Key), destBucket, path.Join(destPrefix, *object.Key))

		_, err := svc.CopyObject(&s3.CopyObjectInput{
			Bucket:     aws.String(destBucket),
			CopySource: aws.String(path.Join(awsBucket, *object.Key)),
			Key:        aws.String(path.Join(destPrefix, *object.Key)),
		})

		if err != nil {
			fmt.Println("Failed to copy file", err)
			return
		}

	}
}

func main() {
	jobs := make(chan *s3.Object)

	s3Url := os.Args[1]
	dest := os.Args[2]

	u, err := url.Parse(s3Url)
	if err != nil {
		log.Fatal(err)
	}

	s3Bucket := u.Host
	s3Prefix := u.Path[1:]

	session := session.New(&aws.Config{Region: aws.String(awsRegion)})
	svc := s3.New(session)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(s3Bucket),
		Prefix: aws.String(s3Prefix),
	}

	var wg sync.WaitGroup
	if strings.HasPrefix(dest, "s3://") {
		fmt.Println(dest)
		u, err = url.Parse(dest)
		if err != nil {
			log.Fatal(err)
		}

		destBucket := u.Host
		destPrefix := u.Path[1:]

		for w := 1; w <= concurrency; w++ {
			wg.Add(1)
			go func(w int) {
				workerBucket(w, jobs, svc, s3Bucket, destBucket, destPrefix)
				defer wg.Done()
			}(w)
		}
	} else {
		err = os.MkdirAll(dest, 0700)
		if err != nil {
			log.Fatal(err)
		}

		downloader := s3manager.NewDownloader(session)

		for w := 1; w <= concurrency; w++ {
			wg.Add(1)
			go func(w int) {
				worker(w, jobs, downloader, s3Bucket, dest)
				defer wg.Done()
			}(w)
		}
	}

	log.Printf("Looking for objects in bucket: %s, prefix: %s", s3Bucket, s3Prefix)

	err = svc.ListObjectsPages(params, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		for _, object := range page.Contents {
			jobs <- object
		}
		objectsCount += len(page.Contents)
		return true
	})

	close(jobs)
	wg.Wait()

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found %d objects to download.\n", objectsCount)
}
