package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
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

func main() {
	jobs := make(chan *s3.Object)

	s3Url := os.Args[1]
	destDir := os.Args[2]

	u, err := url.Parse(s3Url)
	if err != nil {
		log.Fatal(err)
	}

	s3Bucket := u.Host
	s3Prefix := u.Path[1:]

	err = os.MkdirAll(destDir, 0700)
	if err != nil {
		log.Fatal(err)
	}

	session := session.New(&aws.Config{Region: aws.String(awsRegion)})
	svc := s3.New(session)
	downloader := s3manager.NewDownloader(session)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(s3Bucket),
		Prefix: aws.String(s3Prefix),
	}

	var wg sync.WaitGroup
	for w := 1; w <= concurrency; w++ {
		wg.Add(1)
		go func(w int) {
			worker(w, jobs, downloader, s3Bucket, destDir)
			defer wg.Done()
		}(w)
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
