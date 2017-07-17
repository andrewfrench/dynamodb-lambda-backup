package backup

import (
	"fmt"
	"strings"
	"strconv"
	"log"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (b *Backup) serializeObject(item map[string]*dynamodb.AttributeValue) []byte {
	attributeStrings := []string{}
	for k, it := range item {
		attributeStrings = append(attributeStrings, b.makeAttributeString(k, it))
	}

	joined := strings.Join(attributeStrings, ",")
	serializedString := fmt.Sprintf("{%s}", joined)
	return []byte(serializedString)
}

func (b *Backup) makeAttributeString(key string, att *dynamodb.AttributeValue) string {
	var objString string
	if att.M != nil {
		objString = fmt.Sprintf("\"m\":%s", b.serializeObject(att.M))
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
			l = append(l, fmt.Sprintf("{%s}", b.makeAttributeString("", i)))
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

