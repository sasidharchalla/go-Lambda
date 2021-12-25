package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

const (
	host     = "ccl-psql-dev.cclxlbtddgmn.ap-south-1.rds.amazonaws.com"
	port     = 5432
	user     = "postgres"
	password = "Kasvibesc!!09"
	dbname   = "ccldevdb"
)

type PoDocumentDetails struct {
	DocumentName string     `json:"document_name"`
	FileName     string     `json:"file_name"`
	DocKind      NullString `json:"doc_kind"`
}

type LastDocDetails struct {
	DocIdno int `json:"docid_no"`
}

type FileResponse struct {
	FileName        string `json:"fileName"`
	FileLink        string `json:"fileLink"`
	FileData        string `json:"fileData"`
	FileContentType string `json:"fileContentType"`
}

type Input struct {
	Type         string `json:"type"`
	PoId         string `json:"po_id"`
	FileName     string `json:"file_name"`
	DocKind      string `json:"doc_kind"`
	DocumentName string `json:"document_name"`
	FileContent  string `json:"document_content"`
}

type NullString struct {
	sql.NullString
}

// MarshalJSON for NullString
func (ns *NullString) MarshalJSON() ([]byte, error) {
	if !ns.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(ns.String)
}

func poDocumentsUpload(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	headers := map[string]string{"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept"}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	var input Input
	err := json.Unmarshal([]byte(request.Body), &input)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Println(err)
		return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
	}
	defer db.Close()

	// check db
	err = db.Ping()

	if err != nil {
		log.Println(err)
		return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
	}

	var rows *sql.Rows

	fmt.Println("Connected!")
	var documentDetail PoDocumentDetails
	if input.Type == "getDocumentsOnPo" {
		sqlStatement := `select docname, filename ,dockind from dbo.pur_gc_po_master_documents where poid=$1`
		rows, err = db.Query(sqlStatement, input.PoId)

		if err != nil {
			log.Println("Unable to get files uploaded for specific po")
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}

		defer rows.Close()
		var documents []PoDocumentDetails
		for rows.Next() {
			var dt PoDocumentDetails
			err = rows.Scan(&dt.DocumentName, &dt.FileName, &dt.DocKind)
			documents = append(documents, dt)
		}

		res, _ := json.Marshal(documents)
		return events.APIGatewayProxyResponse{200, headers, nil, string(res), false}, nil
	} else if input.Type == "uploadDocument" {

		sqlStatement := `select docidsno from dbo.pur_gc_po_master_documents order by docidsno DESC LIMIT 1`
		rows1, err1 := db.Query(sqlStatement)

		if err1 != nil {
			log.Println("Unable to get last updated id")
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}

		var lastDoc LastDocDetails
		for rows1.Next() {
			err = rows1.Scan(&lastDoc.DocIdno)
		}

		docIdsno := lastDoc.DocIdno + 1
		docId := "FAC-" + strconv.Itoa(docIdsno)
		fileName := "Document_GC_" + docId + ".pdf"

		sqlStatement1 := `INSERT INTO dbo.pur_gc_po_master_documents (docid, docidsno, poid, docname, filename, dockind) VALUES ($1, $2, $3, $4, $5, $6)`
		rows, err = db.Query(sqlStatement1, docId, docIdsno, input.PoId, input.DocumentName, fileName, input.DocKind)

		if err != nil {
			log.Println("Insert to po document master failed")
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}
		log.Println("Successfully uploaded file in db with ", docIdsno, docId, fileName)

		k, _ := uploadDocToS3(input.FileContent, fileName)
		log.Println("Successfully uploaded file in s3 bucket ", k, fileName)
		return events.APIGatewayProxyResponse{200, headers, nil, string(fileName), false}, nil
	} else if input.Type == "removeDocument" {

		sqlStatement := `delete from dbo.pur_gc_po_master_documents where filename=$1`
		rows, err = db.Query(sqlStatement, input.FileName)

		log.Println("Successfully removed file in db with ", documentDetail.FileName)
		return events.APIGatewayProxyResponse{200, headers, nil, string("Removed Successfully"), false}, nil
	} else if input.Type == "downloadDocument" {

		log.Println("starting downloaded ", input.FileName)
		fileResponse := DownloadFile(input.FileName)
		log.Println("Successfully downloaded ", input.FileName)
		response, err := json.Marshal(fileResponse)
		if err != nil {
			log.Println(err.Error())
		}

		return events.APIGatewayProxyResponse{200, headers, nil, string(response), false}, nil
	}

	return events.APIGatewayProxyResponse{200, headers, nil, string("Success"), false}, nil
}

func main() {
	lambda.Start(poDocumentsUpload)
}

func uploadDocToS3(data string, fileDir string) (string, error) {

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-south-1"),
	})

	// Create an uploader with the session and default options
	uploader := s3manager.NewUploader(sess)
	dec, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		log.Println(err)
		return "", err
	}

	s3Output, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String("ccl-lambda-bucket"),
		Key:    aws.String("ccl-lambda-bucket" + "/" + fileDir),
		Body:   bytes.NewReader(dec),
	})
	if err != nil {
		log.Println(err)
		return "", err
	}
	log.Println(s3Output)
	log.Println("fileLocation: " + s3Output.Location)
	return s3Output.Location, nil
}

func DownloadFile(fileName string) FileResponse {
	// The session the S3 Uploader will use
	svc := s3.New(session.New())

	var fileResponse FileResponse
	fileResponse.FileData = Base64Encoder(svc, "ccl-lambda-bucket"+"/"+fileName)
	fileResponse.FileName = fileName
	fileResponse.FileContentType = "application/pdf"

	return fileResponse
}

func Base64Encoder(s3Client *s3.S3, link string) string {
	input := &s3.GetObjectInput{
		Bucket: aws.String("ccl-lambda-bucket"),
		Key:    aws.String(link),
	}
	result, err := s3Client.GetObject(input)
	if err != nil {
		log.Println(err.Error())
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(result.Body)
	fmt.Println(buf)
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}
