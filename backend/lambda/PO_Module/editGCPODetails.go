//Deployed with Pwd changes-Aug22
//Deployed updated -Sep1
//Deployed updated -Sep6
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

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

type PurchaseOrderDetails struct {
	Create          bool   `json:"po_create"`
	Update          bool   `json:"po_update"`
	View            bool   `json:"po_view"`
	DispatchUpdate  bool   `json:"dispatch_items_update"`
	CreatedUserID   string `json:"createduserid"`
	GCCreatedUserID string `json:"gccreateduserid"`
	// CreatedUserName 		string `json:"createdusername"`
	//Contract Information
	Contract string `json:"contract"`

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

	PlaceOfDestination string `json:"place_of_destination"`
	Forwarding         string `json:"forwarding"`
	NoOfContainers     string `json:"no_of_containers"`
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
	//new fields
	NoOfBags      string `json:"no_of_bags"`
	NetWt         string `json:"net_weight"`
	ContainerType string `json:"container_type"`

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

	ApprovalStatus bool `json:"approval_status"`

	//GC Information-Dispatch Section

	DispatchType  string `json:"dispatch_type"`
	DispatchCount string `json:"dispatch_count"`

	LastDetIDSNo int    `json:"last_det_ids_no"`
	DetIDSNo     int    `json:"det_ids_no"`
	DetID        string `json:"det_id_no"`
	// DispatchID			string `json:"dispatch_id"`
	ItemDispatchDetails []ItemDispatch `json:"item_dispatch"`

	//Domestic Price
	// TotalPrice			 string `json:"final_price"`

	//domestic section
	PurchasePriceInr string `json:"purchasePriceInr"`
	MarketPriceInr   string `json:"marketPriceInr"`
	// FinalPriceInr		 string `json:"finalPriceInr"`
	GrossPrice     string `json:"grossPrice"`
	DTerminalPrice string `json:"terminalPrice"`
	Advance        string `json:"advance"`      //changed
	AdvanceType    string `json:"advance_type"` //changed
	PoQty          string `json:"po_qty"`
	Price          string `json:"price"`

	// Domestic Tax Info

	//Other Information
	TaxDuties        string `json:"taxes_duties"`
	ModeOfTransport  string `json:"mode_of_transport"`
	TransitInsurance string `json:"transit_insurance"`

	Rate            string         `json:"rate"`
	AuditLogDetails []AuditLogGCPO `json:"audit_log_gc_po"`
	//Other charges+tax info
	TaxId        string `json:"tax_id"`
	SGST         string `json:"sgst"`
	CGST         string `json:"cgst"`
	IGST         string `json:"igst"`
	PackForward  string `json:"packing_forwarding"`
	OtherCharges string `json:"otherCharges"`
	// Installation			string  `json:"installation"`
	// Freight					string  `json:"freight"`
	// Handling				string  `json:"handling"`
	// Misc 					string  `json:"misc"`
	// Hamali 					string  `json:"hamali"`
	Insurance string `json:"insurance"`
	// MandiFee 				string  `json:"mandifee"`
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

func editGCPODetails(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	headers := map[string]string{"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept"}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	var po PurchaseOrderDetails
	var audit AuditLogGCPO

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

	if po.Update {
		log.Println("Entered Edit Module")
		log.Println("Created user id is: ", po.CreatedUserID)
		// Find created user username
		// sqlStatementCUser1 := `SELECT username
		// 					FROM dbo.users_master_newpg
		// 					where userid=$1`
		// rows, err = db.Query(sqlStatementCUser1, po.CreatedUser)
		// for rows.Next() {
		// 	err = rows.Scan(&po.CreatedUserName)
		// }
		floatquan, _ := strconv.ParseFloat(po.TotalQuantity, 64)
		log.Println("Formattted total quantity: ", floatquan)
		po.MT_Quantity = floatquan / 1000
		log.Println("quantity in MT: ", po.MT_Quantity)
		po.TotalBalQuan = po.TotalQuantity

		if po.SupplierType == "1001" {
			//IMPORT MODULE
			log.Println("Selected supplier type Import Code:", po.SupplierType)
			if po.CurrencyID == "" {
				po.CurrencyID = "HO-102"
			}

			sqlStatementImp1 := `update dbo.pur_gc_po_con_master_newpg
							set
							cid=$1,
							vendorid=$2,
							dispatchterms=$3,
							origin=$4,
							poloading=$5,
							insurance=$6,
							destination=$7,
							forwarding=$8,
							currencyid=$9,
							nocontainers=$10,
							payment_terms=$11,
							remarks=$12,
							billing_at_id=$13,
							delivery_at_id=$14,
							taxes_duties=$15,
							transport_mode=$16,
							transit_insurence=$17,
							packing_forward=$18,
							othercharges=$19,
							rate=$20,
							noofbags=$21,
							netweight=$22,
							container_type=$23,
							purchase_type=$24,
		   					terminal_month=$25,
		  					booked_term_rate=$26,
		  					booked_differential=$27, 
		   					fixed_term_rate=$28,
		  					fixed_differential=$29,
			  				purchase_price=$30,
			   				market_price=$31,
			   				po_margin=$32,
			   				total_price=$33,
							gross_price=$34,
							quantity_mt=$35,
							balance_quantity=$36							
							where pono=$37`
			_, err := db.Query(sqlStatementImp1,
				po.Contract,
				po.SupplierID,
				po.IncoTerms,
				po.Origin,
				po.PortOfLoad,
				po.Insurance,
				po.PlaceOfDestination,
				po.Forwarding,
				po.CurrencyID,
				NewNullString(po.NoOfContainers),
				po.PaymentTerms,
				po.Comments,
				po.POBillTypeID,
				po.PODelTypeID,
				po.TaxDuties,
				po.ModeOfTransport,
				po.TransitInsurance,
				po.PackForward,
				po.OtherCharges,
				NewNullString(po.Rate),
				po.NoOfBags,
				po.NetWt,
				po.ContainerType,
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
				NewNullString(po.GrossPrice),
				po.MT_Quantity,
				po.TotalBalQuan,
				po.PoNO)
			log.Println("Update into PO Table Executed")
			if err != nil {
				log.Println(err.Error())
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}

		} else if po.SupplierType == "1002" {
			log.Println("Selected supplier type Domestic Code:", po.SupplierType)
			//-----------DOMESTIC INFO INSERT-----------------------
			//Green coffee id,name,quantity-Missing
			if po.CurrencyID == "" {
				po.CurrencyID = "HO-101"
			}

			sqlStatementImp1 := `Update dbo.pur_gc_po_con_master_newpg
									set
									vendorid=$1,
									currencyid=$2,
									advancetype=$3,
									advance=$4,
									payment_terms_days=$5,
									billing_at_id=$6,
									delivery_at_id=$7,
									taxes_duties=$8,
									transport_mode=$9,
									transit_insurence=$10,
									packing_forward=$11,
									othercharges=$12,
									rate=$13,
									purchase_type=$14,
									terminal_month=$15,
									terminal_price=$16,
									purchase_price=$17,
									market_price=$18,
									total_price=$19,
									remarks=$20,
									gross_price=$21,
									quantity_mt=$22,
									balance_quantity=$23
									where 
									pono=$24`
			_, err := db.Query(sqlStatementImp1,
				po.SupplierID,
				po.CurrencyID,
				po.AdvanceType,
				po.Advance,
				po.PaymentTermsDays,
				po.POBillTypeID,
				po.PODelTypeID,
				po.TaxDuties,
				po.ModeOfTransport,
				po.TransitInsurance,
				po.PackForward,
				po.OtherCharges,
				NewNullString(po.Rate),
				po.PurchaseType,
				NewNullTime(po.TerminalMonth),
				NewNullString(po.DTerminalPrice),
				NewNullString(po.PurchasePriceInr),
				NewNullString(po.MarketPriceInr),
				NewNullString(po.TotalPrice),
				po.Comments,
				NewNullString(po.GrossPrice),
				po.MT_Quantity,
				po.TotalBalQuan,
				po.PoNO)
			if err != nil {
				log.Println("unable to Update details to PO", err)
			}

			//-----------Update Domestic Tax Information----------------
			//Delete other charges & taxes and insert freshly
			sqlStatementDelTax := `delete from dbo.pur_gc_po_details_taxes_newpg
									where pono=$1`
			_, errDel := db.Query(sqlStatementDelTax, po.PoNO)
			if errDel != nil {
				log.Println("unable to delete tax information")
			}
			//Find last Taxids
			//Find latest TaxID
			var lasttaxidsno, taxidsno int
			sqlStatementTGen1 := `SELECT taxidsno FROM dbo.pur_gc_po_details_taxes_newpg
									where taxidsno is not null
									 order by taxidsno desc limit 1`
			rowsTG1, errTG1 := db.Query(sqlStatementTGen1)
			if errTG1 != nil {
				log.Println("unable to find latest tax idsno", errTG1)
			}
			// var po InputAdditionalDetails
			for rowsTG1.Next() {
				errTG1 = rowsTG1.Scan(&lasttaxidsno)
			}
			taxidsno = lasttaxidsno + 1
			po.TaxId = "DTAX-" + strconv.Itoa(taxidsno)
			//Insert Tax info
			sqlStatementDTax1 := `INSERT INTO dbo.pur_gc_po_details_taxes_newpg(
				taxidsno,pono,taxid,itemid, sgst, cgst, igst,
				pack_forward, installation, freight, handling,
				misc, hamali, mandifee, full_tax, insurance)
				  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`
			_, errDTax1 := db.Query(sqlStatementDTax1,
				taxidsno,
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
			}

			//Update new tax id to PO Table

			sqlStatementDTax2 := `update dbo.pur_gc_po_con_master_newpg
									set
									taxid=$1
									where pono=$2`
			_, errDTax2 := db.Query(sqlStatementDTax2,
				po.TaxId,
				po.PoNO)

			log.Println("Domestic Tax Update Query Executed")
			if errDTax2 != nil {
				log.Println("unable to insert Dometic tax info", errDTax1)
			}

		}
		//---PO Item Update---------
		log.Println("Updating Item Details")
		sqlStatementITU1 := `Update dbo.pur_gc_po_con_master_newpg
				set
				itemid=$1,
				total_quantity=$2
				where
				pono=$3`
		_, errITU1 := db.Query(sqlStatementITU1,
			po.ItemID,
			po.TotalQuantity,
			po.PoNO)
		log.Println("Item Details for PO updated")
		if errITU1 != nil {
			log.Println("unable to update PO Item details", errITU1)
		}

		// 	//-------Delete dispatches for the PO--------------//
		sqlStatementDD1 := `delete from dbo.pur_gc_po_dispatch_master_newpg 
								where 
								pono=$1`
		_, errDD1 := db.Query(sqlStatementDD1, po.PoNO)
		if errDD1 != nil {
			log.Println("unable to insert GC Multi dispatch Details", errDD1)
		}
		log.Println("Dispatches for the PO have been deleted", po.PoNO)
		//----------------Create Fresh Dispatches-------------//
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
			_, errMD1 := db.Query(sqlStatementMD1,
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

			if errMD1 != nil {
				log.Println("unable to insert GC Multi dispatch Details", errMD1)
			}
			log.Println("Inserted details are :", po.ItemDispatchDetails)
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
		// Insert Audit Info
		log.Println("Entered Audit Module for PO Type")
		// Find created user username
		sqlStatementAUser1 := `SELECT u.userid 
								FROM dbo.users_master_newpg u
								inner join dbo.pur_gc_po_con_master_newpg po on po.createdby=u.userid
								where po.pono=$1`
		rows, err = db.Query(sqlStatementAUser1, po.PoNO)
		for rows.Next() {
			err = rows.Scan(&po.GCCreatedUserID)
		}
		audit.CreatedUserid = po.GCCreatedUserID
		audit.CreatedDate = po.PoDate
		audit.Description = "PO Modified"
		// sd.InvoiceDate = time.Now().Format("2006-01-02")
		audit.ModifiedDate = time.Now().Format("2006-01-02")
		audit.ModifiedUserid = po.CreatedUserID

		sqlStatementADT := `INSERT INTO dbo.auditlog_pur_gc_master_newpg(
						pono,createdby, created_date, description,modifiedby, modified_date)
						VALUES($1,$2,$3,$4,$5,$6)`
		_, errADT := db.Query(sqlStatementADT,
			po.PoNO,
			audit.CreatedUserid,
			audit.CreatedDate,
			audit.Description,
			audit.ModifiedUserid,
			audit.ModifiedDate)

		log.Println("Audit Insert Query Executed")
		if errADT != nil {
			log.Println("unable to insert Audit Details", errADT)
		}

	}

	return events.APIGatewayProxyResponse{200, headers, nil, string("success"), false}, nil
}

func main() {
	lambda.Start(editGCPODetails)
}
