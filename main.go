package main

import (
	"os"
	"log"
	"strconv"
	"github.com/andrewfrench/backup"
	"github.com/eawsy/aws-lambda-go-core/service/lambda/runtime"
)

type incoming struct {}

func Handle(evt *incoming, ctx *runtime.Context) (interface{}, error) {
	log.Printf("Parsing environment variables")
	table := envMust("DYNAMODB_TABLE")
	region := envMust("DYNAMODB_REGION")
	bucket := envMust("BACKUP_BUCKET")
	maxConsumedCapacityString := envMust("MAX_CAPACITY")
	maxConsumedCapacity, err := strconv.ParseFloat(maxConsumedCapacityString, 64)
	if err != nil {
		log.Printf("Unable to convert maximum consumed capacity string to float64: %s", err.Error())
		return nil, err
	}

	b, err := backup.New(table, region, bucket, maxConsumedCapacity)
	if err != nil {
		log.Printf("Failed to create Backup struct: %s", err.Error())
		return nil, err
	}

	err = b.Execute()
	if err != nil {
		log.Printf("Failed to execute backup: %s", err.Error())
		return nil, err
	}

	log.Printf("Lambda complete")

	return err, nil
}

func envMust(key string) string {
	e := os.Getenv(key)
	log.Printf("%s: %s", key, e)
	if len(e) == 0 {
		log.Fatalf("Environment variable %s does not exist, exiting", key)
	}

	return e
}
