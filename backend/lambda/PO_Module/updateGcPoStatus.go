package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"

	//SES
	"bytes"
	"net/smtp"
	"text/template"
)

const (
	host     = "ccl-psql-dev.cclxlbtddgmn.ap-south-1.rds.amazonaws.com"
	port     = 5432
	user     = "postgres"
	password = "Kasvibesc!!09"
	dbname   = "ccldevdb"
	//email-SMTP
	from_email = "itsupport@continental.coffee"
	userid     = "itsupport@continental.coffee"
	smtp_pass  = "is@98765"
	// smtp server configuration.
	smtpHost = "smtp.gmail.com"
	smtpPort = "587"
)

const poTemp = `<!DOCTYPE html>
	    <html>
		<head>
			<img src="https://s3.ap-south-1.amazonaws.com/beta-a2z.cclproducts.com/static/media/CCLEmailTemplate.png">
		</head>
		<body>
			<h3>Hello {{.EName}},</h3>
			<p>{{.EMessage}}</p>
			<p>Regards,</p>
			<p>{{.EDept}}</p>
		</body>
	</html>`
const vendorTemp = `<!DOCTYPE html>
	<html>
		<head>
			<img src="https://s3.ap-south-1.amazonaws.com/beta-a2z.cclproducts.com/static/media/CCLEmailTemplate.png">
		</head>
		<body>
			<h3>Hello {{.EName}},</h3>
			<p>You are requested to provide the green coffee specification details for Purchase Order : {{.PONO}}</p>
			<p>Please click the below link :</p>
			<p>https://qa.cclproducts.com/vendor</p>
			<p>Use the following credentials </p>
			<p>UserId: {{.EVendoremailid}} </p>
			<p>OTP: Vendor@1234 </p>
			<p>Regards,</p>
			<p>CCL Purchase Department</p>
		</body>
	</html>`

// {{.VEmail}}

// Email is input request data which is used for sending email using aws ses service
type Email struct {
	ToEmail string `json:"to_email"`
	ToName  string `json:"name"`
	Subject string `json:"subject"`
	Message string `json:"message"`
}

type Input struct {
	Type          string `json:"type"`
	CreatedUserID string `json:"createduserid"`
	PoId          string `json:"po_id"`
	PoNO          string `json:"po_no"`
	VendorEmail   string `json:"vendor_email"`
	SendEmail     bool   `json:"notify_email"`
	UserEmail     string `json:"emailid"`
}

func updateGcPoStatus(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	headers := map[string]string{"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept"}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	var input Input
	var email Email
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

	fmt.Println("Connected!")

	if input.PoId != "" {
		log.Println("Selected POId is : ", input.PoId)
		sqlStatementGE := `select po.pono,initcap(ven.vendorname),ven.email from
							dbo.pur_gc_po_con_master_newpg po
							right join dbo.pur_vendor_master_newpg ven
							on ven.vendorid=po.vendorid
							where po.poid=$1`

		rowsGE, errGE := db.Query(sqlStatementGE, input.PoId)
		if errGE != nil {
			log.Println(errGE)
			return events.APIGatewayProxyResponse{500, headers, nil, errGE.Error(), false}, nil
		}
		defer rowsGE.Close()
		for rowsGE.Next() {
			errGE = rowsGE.Scan(&input.PoNO, &email.ToName, &input.VendorEmail)
		}
		log.Println("Scanned Vendor email is :", input.VendorEmail)
		//Email Module
		//Email is triggered when PO is Approved

		if input.Type == "None" {
			sqlStatement := `update dbo.pur_gc_po_con_master_newpg set status ='2' where poid=$1`
			_, err := db.Query(sqlStatement, input.PoId)

			if err != nil {
				log.Println("Unable to change status to pending state")
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}

			return events.APIGatewayProxyResponse{200, headers, nil, string("Successfully changed"), false}, nil
		} else if input.Type == "changeToInprogessStatus" {
			email.ToEmail = input.VendorEmail
			// ToAddressEmail := []string{input.UserEmail+","+input.VendorEmail}

			sub := "Link to submit Green Coffee Specification for CCL Green Coffee Purchase Order: " + input.PoNO
			email.Message = "PO Status has been changed"
			if input.VendorEmail != "" {
				smtpSendEmail(vendorTemp, sub, email.ToName, email.Message, input.PoNO, "PO Department", email.ToEmail, input.VendorEmail)
			}

			sqlStatement := `update dbo.pur_gc_po_con_master_newpg set status ='3' where poid=$1`
			_, err := db.Query(sqlStatement, input.PoId)

			if err != nil {
				log.Println("Unable to change status to in progess state")
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}
			log.Println("Entered Email Trigger Module")
			if input.VendorEmail != "" {

			}
			return events.APIGatewayProxyResponse{200, headers, nil, string("Successfully changed"), false}, nil
		} else if input.Type == "changeToPendingStatus" {
			// Sending email.
			email.ToEmail = input.UserEmail
			email.Subject = "PO has been sent for approval: " + input.PoNO
			email.Message = "PO Status has been changed"
			smtpSendEmail(poTemp, email.Subject, email.ToName, email.Message, input.PoNO, "PO Department", email.ToEmail, "")
			log.Println("Entered Status Change Module")
			sqlStatement := `update dbo.pur_gc_po_con_master_newpg set status ='2' where poid=$1`
			_, err := db.Query(sqlStatement, input.PoId)

			if err != nil {
				log.Println("Unable to change status to pending state")
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}

			return events.APIGatewayProxyResponse{200, headers, nil, string("Status Successfully changed and Email Sent to Vendor"), false}, nil

		} else if input.Type == "close" {

			log.Println("Entered Close Status Change Module")
			sqlStatement := `update dbo.pur_gc_po_con_master_newpg set status ='6' where poid=$1`
			_, err := db.Query(sqlStatement, input.PoId)

			if err != nil {
				log.Println("Unable to change status to Closed state")
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}

			return events.APIGatewayProxyResponse{200, headers, nil, string("PO Status is set to Closed"), false}, nil

		} else if input.SendEmail {
			log.Println("Send Email button clicked- Entered Email Trigger Module")
			email.ToEmail = input.VendorEmail
			// ToAddressEmail := []string{input.UserEmail+","+input.VendorEmail}

			sub := "Link to submit Green Coffee Specification for CCL Green Coffee Purchase Order: " + input.PoNO
			email.Message = "PO Status has been changed"
			if input.VendorEmail != "" {
				smtpSendEmail(vendorTemp, sub, email.ToName, email.Message, input.PoNO, "PO Department", email.ToEmail, input.VendorEmail)
			}

			sqlStatement := `update dbo.pur_gc_po_con_master_newpg set status ='3' where poid=$1`
			_, err := db.Query(sqlStatement, input.PoId)

			if err != nil {
				log.Println("Unable to change status to in progess state")
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}
			log.Println("Entered Email Trigger Module")
			if input.VendorEmail != "" {

			}
			return events.APIGatewayProxyResponse{200, headers, nil, string("Successfully changed"), false}, nil

		}

	} else {
		return events.APIGatewayProxyResponse{500, headers, nil, string("Email Id is missing for the GC Supplier/Vendor"), false}, nil

	}

	return events.APIGatewayProxyResponse{200, headers, nil, string("Success"), false}, nil
}

func main() {
	lambda.Start(updateGcPoStatus)
}

func smtpSendEmail(temp, subject, name, message, pono, dept, to_email, vendoremailid string) (string, error) {
	log.Println("Entered SMTP Email Module")
	// Receiver email address.
	to := []string{
		to_email,
	}
	// Authentication.
	auth := smtp.PlainAuth("", from_email, smtp_pass, smtpHost)

	t := template.Must(template.New(temp).Parse(temp))

	var body bytes.Buffer

	mimeHeaders := "MIME-version: 1.0;\nContent-Type: text/html; charset=\"UTF-8\";\n\n"
	body.Write([]byte(fmt.Sprintf("Subject:"+subject+"\n%s\n\n", mimeHeaders)))
	//   body.Write([]byte(fmt.Sprintf("Subject: This is a test subject \n%s\n\n", mimeHeaders)))

	t.Execute(&body, struct {
		EName          string
		EMessage       string
		EDept          string
		PONO           string
		EVendoremailid string
	}{
		EName:          name,
		EMessage:       message,
		EDept:          dept,
		PONO:           pono,
		EVendoremailid: vendoremailid,
	})

	// Sending email.
	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, from_email, to, body.Bytes())
	if err != nil {
		fmt.Println(err)

	}
	return "Email Sent!", nil
}
