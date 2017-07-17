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

/*
func Handle(evt *incoming, ctx *runtime.Context) (interface{}, error) {
	log.Print("Parsing environment variables")
	table := envMust("DYNAMODB_TABLE")
	region := envMust("DYNAMODB_REGION")
	bucket := envMust("BACKUP_BUCKET")

	uuidString := uuid()
	t := time.Now()
	dateTime := fmt.Sprintf(
		"%d-%02d-%02d-%02d-%02d-%02d",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(),
	)

	log.Print("Creating session")
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		log.Fatalf("Faild to create session: %s", err.Error())
	}

	log.Print("Generating scan input")
	params := &dynamodb.ScanInput{
		TableName: aws.String(table),
		ConsistentRead: aws.Bool(true),
	}

	log.Print("Executing database scan")
	output, err := dynamodb.New(sess).Scan(params)
	if err != nil {
		log.Fatalf("Failed to execute query: %s", err.Error())
	}

	log.Print("Database scan complete, serializing")
	objects := []string{}
	for _, item := range output.Items {
		s := makeObjectString(item)
		objects = append(objects, s)
	}
	serialized := strings.Join(objects, "\n")
	log.Print("Serialization complete")

	log.Print("Creating manifest")
	m := manifest{
		Name: "DynamoDB-export",
		Version: 3,
		Entries: []entry{
			{
				Mandatory: true,
				Url:       fmt.Sprintf("s3://%s/%s/%s/%s", bucket, table, dateTime, uuidString),
			},
		},
	}

	marshalledManifest, err := json.Marshal(m)
	if err != nil {
		log.Fatalf("Failed to marshal manifest struct: %s", err.Error())
	}

	upload(sess, bucket, fmt.Sprintf("%s/%s/%s", table, dateTime, uuidString), []byte(serialized))
	upload(sess, bucket, fmt.Sprintf("%s/%s/%s", table, dateTime, "manifest"), []byte(marshalledManifest))
	upload(sess, bucket, fmt.Sprintf("%s/%s/%s", table, dateTime, "_SUCCESS"), []byte(""))
	log.Print("Backup process complete")

	return nil, err
}

func makeObjectString(item map[string]*dynamodb.AttributeValue) string {
	components := []string{}
	for k, it := range item {
		components = append(components, makeAttributeString(k, it))
	}

	joined := strings.Join(components, ",")
	return fmt.Sprintf("{%s}", joined)
}

func makeAttributeString(key string, att *dynamodb.AttributeValue) string {
	var objString string
	if att.M != nil {
		objString = fmt.Sprintf("\"m\":%s", makeObjectString(att.M))
	}

	if att.BOOL != nil {
		objString = fmt.Sprintf("\"bOOL\":%t", *att.BOOL)
	}

	if att.B != nil {
		objString = fmt.Sprintf("\"b\":\"%s\"", att.B)
	}

	if att.BS != nil {
		bs := []string{}
		for _, b := range att.BS {
			bs = append(bs, fmt.Sprintf("\"%s\"", b))
		}

		objString = fmt.Sprintf("\"bS\":[%s]", strings.Join(bs, ","))
	}

	if att.L != nil {
		l := []string{}
		for _, i := range att.L {
			l = append(l, fmt.Sprintf("{%s}", makeAttributeString("", i)))
		}

		objString = fmt.Sprintf("\"l\":[%s]", strings.Join(l, ","))
	}

	if att.N != nil {
		objString = fmt.Sprintf("\"n\":\"%s\"", *att.N)
	}

	if att.NS != nil {
		ns := []string{}
		for _, n := range att.NS {
			ns = append(ns, fmt.Sprintf("\"%s\"", *n))
		}

		objString = fmt.Sprintf("\"nS\":[%s]", strings.Join(ns, ","))
	}

	if att.NULL != nil {
		objString = fmt.Sprintf("\"nULLValue\":\"%t\"", *att.NULL)
	}

	if att.S != nil {
		objString = fmt.Sprintf("\"s\":%s", strconv.Quote(*att.S))
	}

	if att.SS != nil {
		ss := []string{}
		for _, s := range att.SS {
			ss = append(ss, strconv.Quote(*s))
		}

		objString = fmt.Sprintf("\"sS\":[%s]", strings.Join(ss, ","))
	}

	// Add a key if one was defined
	if len(key) > 0 {
		return fmt.Sprintf("\"%s\":{%s}", key, objString)
	}

	if len(objString) > 0 {
		return objString
	} else {
		log.Fatalf("Cannot serialize type: %s", key)
		return ""
	}
}

func uuid() string {
	a := random.RandomString(8)
	b := random.RandomString(4)
	c := random.RandomString(4)
	d := random.RandomString(4)
	e := random.RandomString(12)
	return fmt.Sprintf("%s-%s-%s-%s-%s", a, b, c, d, e)
}

func upload(sess *session.Session, bucket, key string, data []byte) {
	log.Printf("Uploading bucket: %s, key: %s to S3", bucket, key)

	result, err := s3manager.NewUploader(sess).Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key: aws.String(key),
		Body: bytes.NewReader(data),
	})

	if err != nil {
		log.Fatalf("Failed to upload %s: %s", key, err.Error())
	}

	log.Printf("%s upload complete: %s", key, result.Location)
}

func envMust(key string) string {
	e := os.Getenv(key)
	log.Printf("%s: %s", key, e)
	if len(e) == 0 {
		log.Fatalf("Environment variable %s does not exist, exiting", key)
	}

	return e
}
*/