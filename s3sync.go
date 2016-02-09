package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	concurrency = 10
	awsBucket   = "BUCKET"
	awsPrefix   = "PREFIX"
	awsRegion   = "us-east-1"
)

var objectsCount = 0

func worker(id int, jobs <-chan *s3.Object, downloader *s3manager.Downloader) {
	for object := range jobs {
		file, err := os.Create(*object.Key)
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

	session := session.New(&aws.Config{Region: aws.String(awsRegion)})
	svc := s3.New(session)
	downloader := s3manager.NewDownloader(session)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(awsBucket),
		Prefix: aws.String(awsPrefix),
	}

	for w := 1; w <= concurrency; w++ {
		go worker(w, jobs, downloader)
	}

	err := svc.ListObjectsPages(params, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		for _, object := range page.Contents {
			jobs <- object
		}
		objectsCount += len(page.Contents)
		return true
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found %d objects to download.\n", objectsCount)
}
