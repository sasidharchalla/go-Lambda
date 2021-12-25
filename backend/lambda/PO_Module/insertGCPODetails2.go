//Checkedin with pwd changes -Aug21
//Updated tax query Aug-22
//Updated query taxidsno -Aug23
//Updated total Quantity in insert-Aug27
//updated query to add other charges & rates Aug29
//Deployed updated -Sep1, Sep3 with is not null check
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"bytes"
	"net/smtp"
	"text/template"

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
	//email-SMTP
	from_email = "itsupport@continental.coffee"
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
			<p>{{.EDept}}</p>
		</body>
	</html>`

type Email struct {
	ToName      string `json:"name"`
	Subject     string `json:"subject"`
	Message     string `json:"message"`
	ToUserEmail string `json:"to_email"`
}

type PurchaseOrderDetails struct {
	Create          bool   `json:"po_create"`
	Update          bool   `json:"po_update"`
	View            bool   `json:"po_view"`
	Status          string `json:"status"`
	CreatedUserID   string `json:"createduserid"`
	CreatedUserName string `json:"createdusername"`
	UserEmail       string `json:"user_email"`
	//Contract Information
	Contract string `json:"contract"`
	// CIdsNo			        int    `json:"contract_idsno"`
	// CNO           			string `json:"contract_no"`
	// CNOsno        			int    `json:"contract_cnosno"`

	//PO Info Section::
	POTypeID        string `json:"po_type_id"`
	PoId            string `json:"poid"`
	PoIdsNo         int    `json:"poidsno"`
	PoNO            string `json:"po_no"`
	PoNOsno         int    `json:"po_nosno"`
	PoDate          string `json:"po_date"`
	POCategory      string `json:"po_category"`
	POSubCategory   string `json:"po_sub_category"`
	SupplierTypeID  string `json:"supplier_type_id"`
	SupplierCountry string `json:"supplier_country"`
	//---------Currency & Advance Information//------------------
	CurrencyID   string `json:"currency_id"`
	CurrencyName string `json:"currency_name"`
	CurrencyCode string `json:"currency_code"`

	//Supplier/Vendor Information
	SupplierName    string `json:"supplier_name"`
	SupplierID      string `json:"supplier_id"`
	SupplierType    string `json:"supplier_type"`
	SupplierEmail   string `json:"supplier_email"`
	SupplierAddress string `json:"supplier_address"`

	//Vendor      			string `json:"supplier_id"`
	// VendorType  			string `json:"vendor_type"`
	QuotNo    string `json:"quot_no"`
	QuotDate  string `json:"quot_date"`
	QuotPrice string `json:"quot_price"`

	LastPoIdsno int `json:"last_poidsno"`
	//currency & incoterms
	IncoTermsID string `json:"incotermsid"`
	IncoTerms   string `json:"incoterms"`
	Origin      string `json:"origin"`
	PortOfLoad  string `json:"ports"`
	// TransportMode		 	string `json:"mode_of_transport"`
	Insurance          string `json:"insurance"`
	PlaceOfDestination string `json:"place_of_destination"`
	Forwarding         string `json:"forwarding"`
	NoOfContainers     string `json:"no_of_containers"`
	ContainerType      string `json:"container_type"`
	PaymentTerms       string `json:"payment_terms"`
	Comments           string `json:"comments"`
	PaymentTermsDays   string `json:"payment_terms_days"` //int to string
	//Billing & Delivery Info
	POBillTypeID   string `json:"billing_at_id"`
	POBillTypeName string `json:"billing_at_name"`
	POBillAddress  string `json:"billing_at_address"`
	PODelTypeID    string `json:"delivery_at_id"`
	PODelTypeName  string `json:"delivery_at_name"`
	PODelAddress   string `json:"delivery_at_address"`

	//Green Coffee Info Section-Done--------------------------

	ItemID        string  `json:"item_id"`
	ItemName      string  `json:"item_name"`
	TotalQuantity string  `json:"total_quantity"`
	MT_Quantity   float64 `json:"quantity_mt"`
	TotalBalQuan  string  `json:"total_Balance_quantity"`

	//-----------GC- Composition Info
	Density       string `json:"density"`
	Moisture      string `json:"moisture"`
	Browns        string `json:"browns"`
	Blacks        string `json:"blacks"`
	BrokenBits    string `json:"brokenbits"`
	InsectedBeans string `json:"insectedbeans"`
	Bleached      string `json:"bleached"`
	Husk          string `json:"husk"`
	Sticks        string `json:"sticks"`
	Stones        string `json:"stones"`
	BeansRetained string `json:"beansretained"`

	//Price Information-Done------------------------------

	PurchaseType       string    `json:"purchase_type"`
	TerminalMonth      time.Time `json:"terminal_month"`
	TerminalPrice      string    `json:"terminal_price"`
	BookedTerminalRate string    `json:"booked_terminal_rate"`
	BookedDifferential string    `json:"booked_differential"`
	FixedTerminalRate  string    `json:"fixed_terminal_rate"`
	FixedDifferential  string    `json:"fixed_differential"`
	PurchasePrice      string    `json:"purchase_price"`
	MarketPrice        string    `json:"market_price"`
	POMargin           string    `json:"po_margin"`
	TotalPrice         string    `json:"totalPrice"`

	//domestic section
	PurchasePriceInr string `json:"purchasePriceInr"`
	MarketPriceInr   string `json:"marketPriceInr"`
	// FinalPriceInr    string `json:"finalPriceInr"`
	GrossPrice     string `json:"grossPrice"`
	DTerminalPrice string `json:"terminalPrice"`
	Advance        string `json:"advance"`      //changed
	AdvanceType    string `json:"advance_type"` //changed
	PoQty          string `json:"po_qty"`
	Price          string `json:"price"`
	LastTaxIdsno   int    `json:"last_taxidsno"`
	TaxIdsno       int    `json:"taxidsno"`
	TaxId          string `json:"tax_id"`
	//Status -PO
	ApprovalStatus bool `json:"approval_status"`

	//GC Information-Dispatch Section

	DispatchType  string `json:"dispatch_type"`
	DispatchCount string `json:"dispatch_count"`

	LastDetIDSNo int    `json:"last_det_ids_no"`
	DetIDSNo     int    `json:"det_ids_no"`
	DetID        string `json:"det_id_no"`
	// DispatchID			string `json:"dispatch_id"`
	ItemDispatchDetails []ItemDispatch `json:"item_dispatch"`

	// Domestic Tax Info
	SGST string `json:"sgst"`
	CGST string `json:"cgst"`
	IGST string `json:"igst"`
	//Other Information
	TaxDuties        string `json:"taxes_duties"`
	ModeOfTransport  string `json:"mode_of_transport"`
	TransitInsurance string `json:"transit_insurance"`
	PackForward      string `json:"packing_forwarding"`
	OtherCharges     string `json:"otherCharges"`
	// DomesticCharges     DomesticCharges `json:"domestic_otherCharges"`
	Rate            string         `json:"rate"`
	AuditLogDetails []AuditLogGCPO `json:"audit_log_gc_po"`

	//Extra fields added
	FixationDate string `json:"fixation_date"`
	NoofBags     string `json:"no_of_bags"`
	NetWeight    string `json:"net_weight"`
	//Other Charges--Domestic
	DPackForward  string `json:"packing_forward_charges"`
	DInstallation string `json:"installation_charges"`
	DFreight      string `json:"freight_charges"`
	DHandling     string `json:"handling_charges"`
	DMisc         string `json:"misc_charges"`
	DHamali       string `json:"hamali_charges"`
	DMandiFee     string `json:"mandifee_charges"`
	DFullTax      string `json:"fulltax_charges"`
	DInsurance    string `json:"insurance_charges"`
}
type ItemDispatch struct {
	DispatchID       string `json:"dispatch_id"`
	DispatchQuantity string `json:"dispatch_quantity"`
	DispatchDate     string `json:"dispatch_date"`
	DSNo             string `json:"number"`
	DDate            string `json:"date"`
}
type AuditLogGCPO struct {
	CreatedDate    string `json:"createddate"`
	CreatedUserid  string `json:"createduserid"`
	ModifiedDate   string `json:"modifieddate"`
	ModifiedUserid string `json:"modifieduserid"`
	Description    string `json:"description"`
}

// type DomesticCharges struct{

// }

func NewNullString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}
func NewNullTime(t time.Time) sql.NullTime {
	if t.IsZero() {
		return sql.NullTime{}
	}
	return sql.NullTime{
		Time:  t,
		Valid: true,
	}
}
func insertGCPODetails(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	headers := map[string]string{"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept"}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	var po PurchaseOrderDetails
	var audit AuditLogGCPO
	var email Email
	// var dc DomesticCharges

	err := json.Unmarshal([]byte(request.Body), &po)
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
	var rows *sql.Rows

	if po.Create {
		log.Println("Entered Create Module")

		log.Println("Created user id is: ", po.CreatedUserID)
		// Find created user username
		sqlStatementCUser1 := `SELECT username,emailid 
							FROM dbo.users_master_newpg 
							where userid=$1`
		rows, err = db.Query(sqlStatementCUser1, po.CreatedUserID)
		for rows.Next() {
			err = rows.Scan(&po.CreatedUserName, &po.UserEmail)
		}

		//Find latest poid
		sqlStatementPOF1 := `SELECT poidsno 
							FROM dbo.pur_gc_po_con_master_newpg 
							where poidsno is not null
							ORDER BY poidsno DESC 
							LIMIT 1`
		rows, err = db.Query(sqlStatementPOF1)

		// var po InputAdditionalDetails
		for rows.Next() {
			err = rows.Scan(&po.LastPoIdsno)
		}
		//Generating PO NOs----------------
		po.PoIdsNo = po.LastPoIdsno + 1
		po.PoId = "POID-" + strconv.Itoa(po.PoIdsNo)
		po.PoNOsno = po.PoIdsNo
		//Generating Contract Nos---------------
		// po.CIdsNo = po.LastPoIdsno + 1
		// po.CNOsno = po.CIdsNo
		// po.CId = "CID-" + strconv.Itoa(po.CIdsNo)
		// po.CNO = "CCL/" + strconv.Itoa(po.CNOsno) + "/" + "2021-2022"
		floatquan, _ := strconv.ParseFloat(po.TotalQuantity, 64)
		log.Println("Formattted total quantity: ", floatquan)
		po.MT_Quantity = floatquan / 1000
		log.Println("quantity in MT: ", po.MT_Quantity)
		po.TotalBalQuan = po.TotalQuantity //total balance quantity in kgs
		audit.CreatedUserid = po.CreatedUserID
		audit.CreatedDate = po.PoDate
		audit.Description = "PO Created"

		if po.SupplierType == "1001" {
			log.Println("Entered Import Module")
			if po.CurrencyID == "" {
				po.CurrencyID = "HO-102"
			}
			log.Println("Selected supplier type Import Code:", po.SupplierType)
			log.Println("Entered Terinal month", po.TerminalMonth)
			po.PoNO = "S4002-" + strconv.Itoa(po.PoIdsNo)
			po.POSubCategory = "Import"
			sqlStatementImp1 := `INSERT INTO dbo.pur_gc_po_con_master_newpg (
							cid,
							poid,
							poidsno,
							pono,
							ponosno,
							podate,
							pocat,
							posubcat,
							vendorid,
							dispatchterms,
							origin,
							poloading,
							insurance,
							destination,
							forwarding,
							currencyid,
							nocontainers,
							container_type,
							payment_terms,
							remarks,
							billing_at_id,
							delivery_at_id,
							itemid,
							total_quantity,
							quantity_mt,
							balance_quantity,
							taxes_duties,
							transport_mode,
							transit_insurence,
							packing_forward,
							othercharges,
							rate,
							status,
							approvalstatus,
							reqapproval,
							createdon,
							createdby,
							netweight, noofbags, fixationdate,
							gross_price,
							accpay_status,qc_status)VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,$15,$16,$17,$18,$19,$20,$21,$22,$23,
								$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35, $36,$37,$38,$39,$40,$41,$42,$43)`
			_, err := db.Query(sqlStatementImp1,
				po.Contract,
				po.PoId,
				po.PoIdsNo,
				po.PoNO,
				po.PoNOsno,
				po.PoDate,
				po.POCategory,
				po.POSubCategory,
				po.SupplierID,
				po.IncoTerms,
				po.Origin,
				po.PortOfLoad,
				po.Insurance,
				po.PlaceOfDestination,
				po.Forwarding,
				po.CurrencyID,
				NewNullString(po.NoOfContainers),
				po.ContainerType,
				po.PaymentTerms,
				po.Comments,
				po.POBillTypeID,
				po.PODelTypeID,
				po.ItemID,
				po.TotalQuantity,
				po.MT_Quantity,
				po.TotalBalQuan,
				po.TaxDuties,
				po.ModeOfTransport,
				po.TransitInsurance,
				po.PackForward,
				po.OtherCharges,
				NewNullString(po.Rate),
				1,
				false,
				false,
				po.PoDate,
				po.CreatedUserID, po.NetWeight, po.NoofBags, po.FixationDate, po.GrossPrice,
				"Pending", "Backlog")
			log.Println("Insert into PO Table Executed")

			if err != nil {
				log.Println(err.Error())
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}
			log.Println("Entered GC Price Info Module")
			// if po.TerminalMonth == ""{
			// po.TerminalMonth="NULL"
			// }
			sqlStatementGCPinfo1 := `update dbo.pur_gc_po_con_master_newpg
									 set
									 purchase_type=$1,
									terminal_month=$2,
									booked_term_rate=$3,
									booked_differential=$4, 
									fixed_term_rate=$5,
									fixed_differential=$6,
										purchase_price=$7,
										market_price=$8,
										po_margin=$9,
										total_price=$10
										where pono=$11`
			_, errp1 := db.Query(sqlStatementGCPinfo1,
				po.PurchaseType,
				NewNullTime(po.TerminalMonth),
				NewNullString(po.BookedTerminalRate),
				NewNullString(po.BookedDifferential),
				NewNullString(po.FixedTerminalRate),
				NewNullString(po.FixedDifferential),
				NewNullString(po.PurchasePrice),
				NewNullString(po.MarketPrice),
				NewNullString(po.POMargin),
				NewNullString(po.TotalPrice),
				po.PoNO)
			log.Println("GC Price Info Query Executed")
			if errp1 != nil {
				log.Println("unable to insert GC Price Info Details", errp1)
			}

			//-----------------END OF IMPORT MODULE---------------------------------------
		} else if po.SupplierType == "1002" {
			log.Println("Selected supplier type Domestic Code:", po.SupplierType)
			if po.CurrencyID == "" {
				po.CurrencyID = "HO-101"
			}
			//Find latest TaxID
			sqlStatementTGen1 := `SELECT taxidsno FROM dbo.pur_gc_po_details_taxes_newpg
									where taxidsno is not null
									 order by taxidsno desc limit 1`
			rowsTG1, errTG1 := db.Query(sqlStatementTGen1)
			if errTG1 != nil {
				log.Println("unable to find latest tax idsno", errTG1)
			}
			// var po InputAdditionalDetails
			for rowsTG1.Next() {
				errTG1 = rowsTG1.Scan(&po.LastTaxIdsno)
			}
			po.TaxIdsno = po.LastTaxIdsno + 1
			po.TaxId = "DTAX-" + strconv.Itoa(po.TaxIdsno)
			//-----------DOMESTIC INFO INSERT-----------------------
			po.POSubCategory = "Domestic"

			po.PoNO = "CCL/" + "/" + po.POCategory + "/" + strconv.Itoa(po.PoIdsNo) + "/" + strconv.Itoa(time.Now().Year()) + "-" + strconv.Itoa(time.Now().Year()+1)
			sqlStatementDom1 := `INSERT INTO dbo.pur_gc_po_con_master_newpg (
									poid,
									poidsno,
									pono,
									ponosno,
									podate,
									pocat,
									posubcat,
									vendorid,
									currencyid,
									advancetype,
									advance,
									payment_terms_days,
									billing_at_id,
									delivery_at_id,
									itemid,
									total_quantity,
									quantity_mt,
									balance_quantity,
									taxes_duties,
									transport_mode,
									transit_insurence,
									packing_forward,
									othercharges,
									rate,
									status,
									approvalstatus,
									createdon,
									createdby,
									taxid,
									remarks,
									gross_price,
									accpay_status,qc_status)VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33)`

			_, err := db.Query(sqlStatementDom1,
				po.PoId,
				po.PoIdsNo,
				po.PoNO,
				po.PoNOsno,
				po.PoDate,
				po.POCategory,
				po.POSubCategory,
				po.SupplierID,
				po.CurrencyID,
				po.AdvanceType,
				po.Advance,
				po.PaymentTermsDays,
				po.POBillTypeID,
				po.PODelTypeID,
				po.ItemID,
				po.TotalQuantity,
				po.MT_Quantity,
				po.TotalBalQuan,
				po.TaxDuties,
				po.ModeOfTransport,
				po.TransitInsurance,
				po.PackForward,
				po.OtherCharges,
				NewNullString(po.Rate),
				1,
				false,
				po.PoDate,
				po.CreatedUserID,
				po.TaxId,
				po.Comments,
				po.GrossPrice,
				"Pending", "Backlog")
			if err != nil {
				log.Println("unable to insert Domestic PO details to PO", err)
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil

			}
			//GC Domestic Price Info
			log.Println("Entered GC Price Info Module for PO :", po.PoNO)
			log.Println("Total Price for PO: ", po.TotalPrice)

			sqlStatementGCPinfo1 := `update dbo.pur_gc_po_con_master_newpg
									 set
									 purchase_type=$1,
									 terminal_month=$2,
									 terminal_price=$3,
									 purchase_price=$4,
									 market_price=$5,
									 total_price=$6
									 where pono=$7`
			_, errp1 := db.Query(sqlStatementGCPinfo1,
				po.PurchaseType,
				NewNullTime(po.TerminalMonth),
				NewNullString(po.DTerminalPrice),
				NewNullString(po.PurchasePriceInr),
				NewNullString(po.MarketPriceInr),
				NewNullString(po.TotalPrice),
				po.PoNO)
			log.Println("GC Price Info Query Executed")
			if errp1 != nil {
				log.Println("unable to insert GC Price Info Details", errp1)
				return events.APIGatewayProxyResponse{500, headers, nil, errp1.Error(), false}, nil

			}
			//---------------Insert Tax info for Domestic PO-------------------
			log.Println("Packforward charges: ", po.DPackForward)
			sqlStatementDTax1 := `INSERT INTO dbo.pur_gc_po_details_taxes_newpg(
									taxidsno,pono,taxid,itemid, sgst, cgst, igst,
									pack_forward, installation, freight, handling,
									misc, hamali, mandifee, full_tax, insurance)
							  		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`
			_, errDTax1 := db.Query(sqlStatementDTax1,
				po.TaxIdsno,
				po.PoNO,
				po.TaxId,
				po.ItemID,
				NewNullString(po.SGST),
				NewNullString(po.CGST),
				NewNullString(po.IGST),
				NewNullString(po.DPackForward),
				NewNullString(po.DInstallation),
				NewNullString(po.DFreight),
				NewNullString(po.DHandling),
				NewNullString(po.DMisc),
				NewNullString(po.DHamali),
				NewNullString(po.DMandiFee),
				NewNullString(po.DFullTax),
				NewNullString(po.DInsurance))

			log.Println("Domestic Tax Insert Query Executed")
			if errDTax1 != nil {
				log.Println("unable to insert Dometic tax info", errDTax1)
				return events.APIGatewayProxyResponse{500, headers, nil, errDTax1.Error(), false}, nil

			}

		}
		//------------Quote if its a special Coffee type------------------------
		if po.QuotNo != "" {
			sqlStatementQuoteInfo1 := `update dbo.pur_gc_po_con_master_newpg
										set 
										quote_no=$1,
										quote_date=$2,
										quote_price=$3
											where pono=$4`
			defer rows.Close()
			rows, err = db.Query(sqlStatementQuoteInfo1, po.QuotNo, po.QuotDate, po.QuotPrice, po.PoNO)
			if err != nil {
				log.Println("unable to insert Quote details to PO", err)
			}
		}

		//--------------------------END---------------------------------------------------

		//----------------GC Dispatch-Single/Multiple Info-------------------
		log.Println("Entered PO Create Module")
		sqlStatementDT1 := `select detidsno from dbo.pur_gc_po_dispatch_master_newpg
							where detidsno is not null
							order by detidsno desc limit 1`

		rows, err = db.Query(sqlStatementDT1)

		// var po InputAdditionalDetails
		for rows.Next() {
			err = rows.Scan(&po.LastDetIDSNo)
		}
		log.Println("Last DETIDSNO from table:", po.LastDetIDSNo)
		po.DetIDSNo = po.LastDetIDSNo + 1
		log.Println("New DETIDSNO from table:", po.DetIDSNo)
		po.DetID = "GCDIS-" + strconv.Itoa(po.DetIDSNo)
		log.Println("New DETID from table:", po.DetID)

		if po.DispatchType != "" {
			log.Println("Dispatch Type Selected:", po.DispatchType)

			for _, dis := range po.ItemDispatchDetails {
				log.Println("Loop Entered")
				if po.DispatchType == "Single" {
					po.DispatchCount = "1"
					po.DispatchType = "Single"
				} else {
					po.DispatchType = "Multiple"
				}

				log.Println("Values of dispatch details are ", dis)
				sqlStatementMD1 := `insert into dbo.pur_gc_po_dispatch_master_newpg(
					pono,
					detid,
					detidsno,
					itemid,
					quantity,
					dispatch_date,
					dispatch_count,
					dispatch_type,
					createdon,
					createdby) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
				rows, err = db.Query(sqlStatementMD1,
					po.PoNO,
					po.DetID,
					po.DetIDSNo,
					po.ItemID,
					dis.DispatchQuantity,
					dis.DispatchDate,
					po.DispatchCount,
					po.DispatchType,
					po.PoDate,
					NewNullString(po.CreatedUserID))
				log.Println("Row Inserted Successfully")
				if po.DispatchType == "Multiple" {
					po.DetIDSNo = po.DetIDSNo + 1
					po.DetID = "GCDIS-" + strconv.Itoa(po.DetIDSNo)

				}

			}
			if err != nil {
				log.Println("unable to insert GC Multi dispatch Details", err)
			}
			log.Println("Inserted details are :", po.ItemDispatchDetails)

		}
		// Insert Audit Info
		log.Println("Entered Audit Module for PO Type")
		sqlStatementADT := `INSERT INTO dbo.auditlog_pur_gc_master_newpg(
						pono,createdby, created_date, description)
						VALUES($1,$2,$3,$4)`
		_, errADT := db.Query(sqlStatementADT,
			po.PoNO,
			audit.CreatedUserid,
			audit.CreatedDate,
			audit.Description)

		log.Println("Audit Insert Query Executed")
		if errADT != nil {
			log.Println("unable to insert Audit Details", errADT)
		}
		// Sending email.
		log.Println("Email Module Entered")
		log.Println("Scanned username & Email id: ", po.CreatedUserName, po.UserEmail)
		email.Subject = "GC Purchase order created: " + po.PoNO
		email.Message = "A green coffee purchase order is created " + po.PoNO
		email.ToUserEmail = po.UserEmail
		// email.ToUserEmail="sriram.n@continental.coffee"
		email.ToName = po.CreatedUserName
		email.Message = "Green coffee purchase order has been created"
		if email.ToUserEmail != "" {
			log.Println("Scanned user email is :", email.ToUserEmail)
			smtpSendEmail(poTemp, email.Subject, email.ToName, email.Message, po.PoNO, "PO Department", email.ToUserEmail)
			log.Println("Email Sent...!")
		}

	}

	return events.APIGatewayProxyResponse{200, headers, nil, string("success"), false}, nil
}

func main() {
	lambda.Start(insertGCPODetails)
}
func smtpSendEmail(temp, subject, name, message, pono, dept, to_email string) (string, error) {
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
		EName    string
		EMessage string
		EDept    string
		PONO     string
	}{
		EName:    name,
		EMessage: message,
		EDept:    dept,
		PONO:     pono,
	})

	// Sending email.
	err := smtp.SendMail(smtpHost+":"+smtpPort, auth, from_email, to, body.Bytes())
	if err != nil {
		fmt.Println(err)

	}
	return "Email Sent!", nil
}
