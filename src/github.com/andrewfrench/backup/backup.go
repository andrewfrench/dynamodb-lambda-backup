package backup

import (
	"fmt"
	"log"
	"math"
	"time"
	"bytes"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/andrewfrench/random"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Backup struct {
	// Public
	Region string
	Table string
	OutputBucket string
	MaxConsumedCapacity float64

	// private
	uuid       string
	limit      int64
	datetime   string
	tableData  []byte
	session    *session.Session
	connection *dynamodb.DynamoDB
	startKey   map[string]*dynamodb.AttributeValue
	iteration  int
	objectCount int
}

type manifest struct {
	Name    string  `json:"name"`
	Version int     `json:"version"`
	Entries []entry `json:"entries"`
}

type entry struct {
	Url       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

func New(table, region, outputBucket string, maxConsumedCapacity float64) (*Backup, error) {
	t := time.Now()
	datetime := fmt.Sprintf(
		"%d-%02d-%02d-%02d-%02d-%02d",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(),
	)

	dynamoSession, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return &Backup{}, err
	}

	return &Backup{
		Region: region,
		Table: table,
		OutputBucket: outputBucket,
		MaxConsumedCapacity: maxConsumedCapacity,

		uuid:       uuidGen(),
		limit:      int64(1),
		datetime:   datetime,
		session:    dynamoSession,
		connection: dynamodb.New(dynamoSession),
		iteration:  0,
		objectCount: 0,
	}, err
}

func (b *Backup) Execute() error {
	log.Printf("Executing backup process")

	for range time.Tick(time.Second) {
		b.iteration++

		objects, err := b.scanItems()
		if err != nil {
			return err
		}

		b.objectCount += len(objects)
		for _, object := range objects {
			serializedObject := b.serializeObject(object)
			b.tableData = append(b.tableData, serializedObject...)
			b.tableData = append(b.tableData, '\n')
		}

		if len(b.startKey) == 0 {
			break
		}
	}

	log.Printf("Gathered %d objects over %d iterations", b.objectCount, b.iteration)

	var err error
	err = b.uploadBackupData()
	if err != nil {
		return err
	}

	err = b.uploadManifest()
	if err != nil {
		return err
	}

	err = b.uploadSuccessFile()
	if err != nil {
		return err
	}

	log.Printf("Backup execution complete")

	return err
}

func (b *Backup) scanItems() ([]map[string]*dynamodb.AttributeValue, error) {
	log.Printf("Creating Scan input struct")

	input := &dynamodb.ScanInput{
		TableName: aws.String(b.Table),
		ConsistentRead: aws.Bool(true),
		Limit: aws.Int64(b.limit),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}

	if len(b.startKey) > 0 {
		input.ExclusiveStartKey = b.startKey
	}

	log.Printf("Executing scan")
	output, err := b.connection.Scan(input)
	if err != nil {
		log.Printf("Error while executing scan: %s", err.Error())
		return []map[string]*dynamodb.AttributeValue{}, err
	}

	log.Printf("Consumed capacity: %f", *output.ConsumedCapacity.CapacityUnits)
	b.updateLimit(*output.ConsumedCapacity.CapacityUnits)

	log.Printf("Setting start key")
	b.startKey = output.LastEvaluatedKey

	return output.Items, err
}

func (b *Backup) updateLimit(consumedCapacityUnits float64) {
	log.Printf("Recalculating scan item limit, currently: %d", b.limit)

	ratio := b.MaxConsumedCapacity / consumedCapacityUnits

	b.limit = int64(math.Floor(float64(b.limit) * ratio))

	if b.limit < 1 {
		log.Printf("Limit <1, clamping to 1")
		b.limit = 1
	}

	log.Printf("New limit: %d", b.limit)
}

func (b *Backup) uploadSuccessFile() error {
	log.Printf("Uploading _SUCCESS file")

	return b.uploadData(b.S3Key("_SUCCESS"), []byte{})
}

func (b *Backup) uploadManifest() error {
	log.Printf("Uploading manifest file")

	m := manifest{
		Name: "DynamoDB-export",
		Version: 3,
		Entries: []entry{
			{
				Mandatory: true,
				Url:       fmt.Sprintf("s3://%s/%s/%s/%s", b.OutputBucket, b.Table, b.datetime, b.uuid),
			},
		},
	}

	serializedManifest, err := json.Marshal(m)
	if err != nil {
		log.Printf("Error marshalling manifest: %s", err.Error())
	}

	return b.uploadData(b.S3Key("manifest"), serializedManifest)
}

func (b *Backup) uploadBackupData() error {
	log.Printf("Uploading backup data")

	return b.uploadData(b.S3Key(b.uuid), b.tableData)
}

func (b *Backup) S3Key(name string) string {
	return fmt.Sprintf("%s/%s/%s", b.Table, b.datetime, name)
}

func uuidGen() string {
	log.Printf("Generating UUID")

	a := random.RandomString(8)
	b := random.RandomString(4)
	c := random.RandomString(4)
	d := random.RandomString(4)
	e := random.RandomString(12)
	return fmt.Sprintf("%s-%s-%s-%s-%s", a, b, c, d, e)
}

func (b *Backup) uploadData(key string, data []byte) error {
	log.Printf("Executing transfer to S3")
	result, err := s3manager.NewUploader(b.session).Upload(&s3manager.UploadInput{
		Bucket: &b.OutputBucket,
		Key: &key,
		Body: bytes.NewReader(data),
	})

	if err != nil {
		log.Printf("Error uploading %s to S3: %s", key, err.Error())
		return err
	}

	log.Printf("Uploaded %s to s3://%s", key, result.Location)

	return err
}
