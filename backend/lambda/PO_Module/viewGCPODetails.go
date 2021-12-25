//updated total_quantity-Sep1
//updated audit query-Sep6
//updated fixationdate, new fields-Sep17
//Updated GC & supplier names- Sep23
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	// "strconv"
	// "time"

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
	Status          string `json:"status"`
	CreatedUserID   string `json:"createduserid"`
	GCCreatedUserID string `json:"gccreateduserid"`
	GCCoffeeType    string `json:"coffee_type"'`
	Type            string `json:"type"`
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

	ItemID        string `json:"item_id"`
	ItemName      string `json:"item_name"`
	TotalQuantity string `json:"total_quantity"`
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

	PurchaseType       string `json:"purchase_type"`
	TerminalMonth      string `json:"terminal_month"`
	BookedTerminalRate string `json:"booked_terminal_rate"`
	BookedDifferential string `json:"booked_differential"`
	FixedTerminalRate  string `json:"fixed_terminal_rate"`
	FixedDifferential  string `json:"fixed_differential"`
	PurchasePrice      string `json:"purchase_price"`
	MarketPrice        string `json:"market_price"`
	POMargin           string `json:"po_margin"`
	// FinalPrice			 string `json:"final_price"`

	Advance     string `json:"advance"`      //changed
	AdvanceType string `json:"advance_type"` //changed
	PoQty       string `json:"po_qty"`
	// Price 				 string `json:"price"`

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
	//domestic section
	PurchasePriceInr string `json:"purchasePriceInr"`
	MarketPriceInr   string `json:"marketPriceInr"`
	FinalPriceInr    string `json:"finalPriceInr"`
	DTerminalPrice   string `json:"terminalPrice"`
	TotalPrice       string `json:"totalPrice"`
	//Other Information
	TaxDuties        string `json:"taxes_duties"`
	ModeOfTransport  string `json:"mode_of_transport"`
	TransitInsurance string `json:"transit_insurance"`
	PackForward      string `json:"packing_forwarding"`
	//Other charges
	OtherCharges    string         `json:"otherCharges"`
	Rate            string         `json:"rate"`
	GrossPrice      string         `json:"grossPrice"`
	AuditLogDetails []AuditLogGCPO `json:"audit_log_gc_po"`
	//Consolidated Finance

	QCStatus      string `json:"qcStatus"`
	APStatus      string `json:"apStatus"`
	PayableAmount string `json:"payable_amount"`
	//new fields
	NoOfBags     string  `json:"no_of_bags"`
	NetWt        string  `json:"net_weight"`
	MTQuantity   float64 `json:"quantity_mt"`
	FixationDate string  `json:"fixation_date"`
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
	DispatchID        string `json:"dispatch_id"`
	DispatchQuantity  string `json:"dispatch_quantity"`
	DispatchDate      string `json:"dispatch_date"`
	DSNo              string `json:"number"`
	DDate             string `json:"date"`
	DeliveredQuantity string `json:"delivered_quantity"`
	BalanceQuantity   string `json:"balance_quantity"`
}
type AuditLogGCPO struct {
	CreatedDate    string `json:"createddate"`
	CreatedUserid  string `json:"createduserid"`
	ModifiedDate   string `json:"modifieddate"`
	ModifiedUserid string `json:"modifieduserid"`
	Description    string `json:"description"`
}

func viewGCPODetails(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	headers := map[string]string{"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept"}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	var po PurchaseOrderDetails

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
	// var rows *sql.Rows

	if po.PoNO != "" {
		log.Println("Entered PO View Module")
		log.Println("selected PO NO:", po.PoNO)
		//check if po is import or domestic
		sqlStatementIDC1 := `SELECT posubcat FROM dbo.pur_gc_po_con_master_newpg
						where pono=$1`
		rowsIDC1, errIDC1 := db.Query(sqlStatementIDC1, po.PoNO)
		if errIDC1 != nil {
			log.Println("Fetching PO Details from DB failed")
			log.Println(errIDC1.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, errIDC1.Error(), false}, nil
		}
		// defer rows.Close()
		for rowsIDC1.Next() {

			errIDC1 = rowsIDC1.Scan(&po.POSubCategory)
		}
		var con, posupid, poitemid, pobtid, podtid, pocurid,
			status, incoid, origin, poload, ins, dest,
			forward, cont, conttype, payterms, comm,
			taxdut, transmode, transins, packfor,
			othercharges, rate, bags, netwt, fixdate,
			purtype, termmonth, btr, bd, ftr, fd, pprice, ppriceinr, mkprice,
			margin, totprice, grossprice, advancetype, advance, paytermdays, markpriceinr, dtermprice, venaddress, venemail, vencountry sql.NullString
		if po.POSubCategory == "Import" {
			po.SupplierType = "Import"
			sqlStatementPOV1 := `SELECT total_quantity,cid,poid, podate, pocat,vendorid,itemid,
						billing_at_id, delivery_at_id,currencyid,status,dispatchterms, origin,
						poloading, insurance, destination, forwarding,nocontainers,container_type,
						payment_terms,remarks,taxes_duties, transport_mode, transit_insurence, packing_forward,
						othercharges,rate,noofbags,netweight,
						purchase_type, terminal_month, booked_term_rate,booked_differential, fixed_term_rate, fixed_differential,
						purchase_price, market_price, po_margin, total_price,gross_price,fixationdate,quantity_mt
						FROM dbo.pur_gc_po_con_master_newpg
						where pono=$1`
			rows1, err1 := db.Query(sqlStatementPOV1, po.PoNO)
			log.Println("PO Master Query Executed")
			if err1 != nil {
				log.Println("Fetching PO Details from DB failed")
				log.Println(err.Error())
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}
			defer rows1.Close()

			for rows1.Next() {

				err1 = rows1.Scan(&po.TotalQuantity, &con, &po.PoId, &po.PoDate, &po.POCategory,
					&posupid, &poitemid, &pobtid, &podtid, &pocurid, &status,
					&incoid, &origin, &poload, &ins, &dest, &forward, &cont,
					&conttype, &payterms, &comm, &taxdut, &transmode, &transins,
					&packfor, &othercharges, &rate, &bags, &netwt,
					&purtype, &termmonth, &btr, &bd, &ftr, &fd, &pprice, &mkprice,
					&margin, &totprice, &grossprice, &fixdate, &po.MTQuantity)

			}
			po.Contract = con.String
			po.IncoTermsID = incoid.String
			po.Origin = origin.String
			po.PortOfLoad = poload.String //still missing
			po.Insurance = ins.String     //still missing
			po.PlaceOfDestination = dest.String
			po.Forwarding = forward.String
			po.NoOfContainers = cont.String
			po.ContainerType = conttype.String
			po.PaymentTerms = payterms.String
			po.NoOfBags = bags.String
			po.NetWt = netwt.String
			// po.MTQuantity=mtquan.String

			po.PurchaseType = purtype.String
			po.TerminalMonth = termmonth.String

			po.BookedTerminalRate = btr.String
			po.BookedDifferential = bd.String
			po.FixedTerminalRate = ftr.String
			po.FixedDifferential = fd.String

			po.MarketPrice = mkprice.String
			po.POMargin = margin.String

			//Fetch Incoterms details:
			if po.IncoTermsID != "" {
				log.Println("get incoterms for id :", po.IncoTermsID)
				sqlStatementIT1 := `SELECT incoterms FROM dbo.cms_incoterms_master where incotermsid=$1`
				rowsIT1, errIT1 := db.Query(sqlStatementIT1, po.IncoTermsID)
				if errIT1 != nil {
					log.Println("Fetching Incoterms Details from DB failed")

				}

				defer rowsIT1.Close()
				for rowsIT1.Next() {
					errIT1 = rowsIT1.Scan(&po.IncoTerms)

				}
			}

		} else {
			//DOMESTIC PO VIEW
			po.SupplierType = "Domestic"

			sqlStatementDPOV1 := `SELECT poid, podate, pocat, posubcat, 
							vendorid,itemid, billing_at_id, delivery_at_id,
							currencyid,status,
							advancetype, advance, payment_terms_days, 
							taxes_duties, transport_mode, transit_insurence, 
							packing_forward,othercharges,rate,remarks,
							purchase_type,terminal_month,terminal_price,
							purchase_price,market_price,total_price,gross_price,total_quantity,fixationdate
							FROM dbo.pur_gc_po_con_master_newpg
							where pono=$1`
			rowsd1, errd1 := db.Query(sqlStatementDPOV1, po.PoNO)
			log.Println("PO Master Query Executed")
			if errd1 != nil {
				log.Println("Fetching PO Details from DB failed")
				log.Println(errd1.Error())
				return events.APIGatewayProxyResponse{500, headers, nil, errd1.Error(), false}, nil
			}
			// defer rows.Close()
			for rowsd1.Next() {
				errd1 = rowsd1.Scan(&po.PoId, &po.PoDate, &po.POCategory, &po.POSubCategory,
					&posupid, &poitemid, &pobtid, &podtid, &pocurid,
					&status, &advancetype, &advance,
					&paytermdays, &taxdut, &transmode, &transins,
					&packfor, &othercharges, &rate, &comm, &purtype,
					&termmonth, &dtermprice, &ppriceinr,
					&markpriceinr, &totprice, &grossprice, &po.TotalQuantity, &fixdate)
			}

			po.AdvanceType = advancetype.String
			po.Advance = advance.String
			po.PaymentTermsDays = paytermdays.String
			po.PurchasePriceInr = ppriceinr.String
			po.MarketPriceInr = markpriceinr.String
			// po.FinalPriceInr
			po.DTerminalPrice = dtermprice.String

		}
		//------COMMON to IMPORT && DOMESTIC-------//

		po.SupplierID = posupid.String
		po.ItemID = poitemid.String
		po.POBillTypeID = pobtid.String
		po.PODelTypeID = podtid.String
		po.CurrencyID = pocurid.String
		po.Status = status.String
		po.Comments = comm.String
		po.TaxDuties = taxdut.String
		po.ModeOfTransport = transmode.String
		po.TransitInsurance = transins.String
		po.PackForward = packfor.String
		po.OtherCharges = othercharges.String
		po.Rate = rate.String
		po.PurchasePrice = pprice.String
		po.TotalPrice = totprice.String
		po.GrossPrice = grossprice.String
		po.FixationDate = fixdate.String
		po.PurchaseType = purtype.String
		po.TerminalMonth = termmonth.String
		//---------------_Fetch Billing Address Info------------------------
		log.Println("Entered Billing Module")
		sqlStatementPOVB2 := `SELECT 
						 potypeid,
						 initcap(bdi.potypename),
						 initcap(bdi.potypefullname)||','||initcap(bdi.address) as billingaddress
						 from dbo.pur_po_types bdi
						 where 
						 bdi.potypeid=(select pom.billing_at_id from dbo.pur_gc_po_con_master_newpg pom where pom.pono=$1)`
		rowsb2, errb2 := db.Query(sqlStatementPOVB2, po.PoNO)
		log.Println("PO Types Query Executed")
		if errb2 != nil {
			log.Println("Issue in fetching billing address from DB failed")
			log.Println(errb2.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, errb2.Error(), false}, nil
		}

		defer rowsb2.Close()
		for rowsb2.Next() {
			errb2 = rowsb2.Scan(&po.POBillTypeID, &po.POBillTypeName, &po.POBillAddress)
			log.Println(po.POBillAddress)

			log.Println(po)
		}
		//---------------_Fetch Delivery Address Info------------------------
		log.Println("Entered PO Delivery Module")
		sqlStatementPOVD2 := `SELECT 
						  initcap(bdi.potypename),
						 initcap(bdi.potypefullname)||','||initcap(bdi.address) as billingaddress
						 from dbo.pur_po_types bdi
						 where 
						 bdi.potypeid=(select pom.delivery_at_id from dbo.pur_gc_po_con_master_newpg pom where pom.pono=$1)`
		rowsd2, errd2 := db.Query(sqlStatementPOVD2, po.PoNO)
		log.Println("PO Delivery Address Query Executed")
		if errd2 != nil {
			log.Println("Fetching PO Delivery Details from DB failed")
			log.Println(errd2.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}

		defer rowsd2.Close()
		for rowsd2.Next() {
			errd2 = rowsd2.Scan(&po.PODelTypeName, &po.PODelAddress)

		}
		//-------__Fetch Vendor Information---------------------
		log.Println("Entered PO Vendor Module")
		sqlStatementPOV3 := `SELECT				
						vm.vendortypeid,
						vm.country,
						initcap(vm.vendorname),
						initcap(vm.address1)||','||initcap(vm.address2)||','||initcap(vm.city)||','||pincode||','||initcap(vm.state)||','||'Phone:'||vm.phone||','||'Mobile:'||vm.mobile||','||'GST NO:'||vm.gstin address,
						vm.email
						from 
						dbo.pur_vendor_master_newpg vm
						where vm.vendorid=(select pom.vendorid from dbo.pur_gc_po_con_master_newpg pom where pom.pono=$1)`
		rows3, err3 := db.Query(sqlStatementPOV3, po.PoNO)
		log.Println("Vendor Details fetch Query Executed")
		if err3 != nil {
			log.Println("Fetching Vendor Details from DB failed")
			log.Println(err3.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}
		defer rows3.Close()
		for rows3.Next() {
			err3 = rows3.Scan(&po.SupplierTypeID, &vencountry, &po.SupplierName, &venaddress, &venemail)
		}
		po.SupplierCountry = vencountry.String
		po.SupplierAddress = venaddress.String
		po.SupplierEmail = venemail.String
		//-------------Fetch Currencuy Info----------------------------
		log.Println("Entered Currency Fetch Module")
		sqlStatementPOV4 := `SELECT currencyname,currencycode
							from dbo.project_currency_master 
							where currencyid=$1`
		rows4, err4 := db.Query(sqlStatementPOV4, po.CurrencyID)
		log.Println("Currency Details fetch Query Executed")
		if err4 != nil {
			log.Println("Fetching Currency Details from DB failed")
			log.Println(err4.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err4.Error(), false}, nil
		}
		defer rows4.Close()
		for rows4.Next() {
			err4 = rows4.Scan(&po.CurrencyName, &po.CurrencyCode)

		}
		if po.AdvanceType == "101" {
			po.AdvanceType = "Percentage"
		} else {
			po.AdvanceType = "Amount"
		}
		log.Println("Currency Name & Code are: ", po.CurrencyName, po.CurrencyCode)

		//----------_Fetch Green Coffee Item Information--------------------
		if po.ItemID != "" {
			log.Println("Entered GC Item Fetch Module")
			sqlStatementPOV5 := `SELECT im.itemid,initcap(im.itemname),im.cat_type
							from dbo.inv_gc_item_master_newpg im
							where
							im.itemid=$1`
			rows5, err5 := db.Query(sqlStatementPOV5, po.ItemID)
			log.Println("GC Details fetch Query Executed")
			if err5 != nil {
				log.Println("Fetching GC Details from DB failed")
				log.Println(err5.Error())
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}
			defer rows5.Close()
			for rows5.Next() {
				err5 = rows5.Scan(&po.ItemID, &po.ItemName, &po.GCCoffeeType)
			}
		}

		// // ---------------------Fetch GC Composition Details--------------------------------------//
		log.Println("The GC Composition for the Item #", po.ItemID)
		sqlStatementPOGC1 := `SELECT density, moisture, browns, blacks, brokenbits, insectedbeans, bleached, husk, sticks, stones, beansretained
						FROM dbo.pur_gc_po_composition_master_newpg where itemid=$1`
		rows7, err7 := db.Query(sqlStatementPOGC1, po.ItemID)
		log.Println("GC Fetch Query Executed")
		if err7 != nil {
			log.Println("Fetching GC Composition Details from DB failed")
			log.Println(err7.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}

		for rows7.Next() {
			err7 = rows7.Scan(&po.Density, &po.Moisture, &po.Browns, &po.Blacks, &po.BrokenBits, &po.InsectedBeans, &po.Bleached, &po.Husk, &po.Sticks,
				&po.Stones, &po.BeansRetained)

		}

		//---------------------Fetch Multiple Dispatch Info-------------------------------------//
		//Old query:`SELECT detid,quantity,dispatch_type,dispatch_count,dispatch_date
		//from dbo.pur_gc_po_dispatch_master_newpg where pono=$1`
		log.Println("Fetching Single/Multiple Dispatch Information the Contract #")
		sqlStatementMDInfo1 := `select d.detid,d.dispatch_date,d.quantity, d.dispatch_type,d.dispatch_count,
							m.delivered_quantity, (m.expected_quantity-m.delivered_quantity) as balance_quantity
							from dbo.pur_gc_po_dispatch_master_newpg d
							left join dbo.inv_gc_po_mrin_master_newpg as m on m.detid=d.detid
							where d.pono=$1`
		rows9, err9 := db.Query(sqlStatementMDInfo1, po.PoNO)
		log.Println("Multi Dispatch Info Fetch Query Executed")
		if err9 != nil {
			log.Println("Multi Dispatch Info Fetch Query failed")
			log.Println(err9.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}
		var dispid, dispdate, dispquan, disptype, dispcoun, delquan, balquan sql.NullString
		for rows9.Next() {
			var mid ItemDispatch
			err9 = rows9.Scan(&dispid, &dispdate, &dispquan, &disptype, &dispcoun, &delquan, &balquan)
			mid.DispatchID = dispid.String
			mid.DispatchDate = dispdate.String
			mid.DispatchQuantity = dispquan.String
			po.DispatchType = disptype.String
			po.DispatchCount = dispcoun.String
			mid.DeliveredQuantity = delquan.String
			mid.BalanceQuantity = balquan.String
			gcMultiDispatch := append(po.ItemDispatchDetails, mid)
			po.ItemDispatchDetails = gcMultiDispatch
			log.Println("added one")
			// po.DispatchType=mid.DispatchType
			// po.DispatchCount=mid.DispatchCount
		}
		log.Println("Multi Dispatch Details:", po.ItemDispatchDetails)

		//---------------Fetch Domestic Tax info for Domestic PO-------------------

		if po.POSubCategory == "Domestic" {
			log.Println("Selected supplier type Domestic Code:", po.POSubCategory)

			var sgst, cgst, igst, packforward, install, freight, handling, misc, hamali, mandifee, fulltax, insurance sql.NullString
			sqlStatementDTax1 := `SELECT sgst, cgst, igst,pack_forward, installation,
							 freight, handling, misc, hamali, mandifee, full_tax,
							  insurance FROM dbo.pur_gc_po_details_taxes_newpg 
							  where pono=$1`
			rowsDTax1, errDTax1 := db.Query(sqlStatementDTax1, po.PoNO)
			log.Println("Domestic Tax Info Fetch Query Executed")
			if errDTax1 != nil {
				log.Println("Domestic Tax Info Fetch Query failed")
				log.Println(errDTax1.Error())
				return events.APIGatewayProxyResponse{500, headers, nil, errDTax1.Error(), false}, nil
			}

			defer rowsDTax1.Close()
			for rowsDTax1.Next() {
				errDTax1 = rowsDTax1.Scan(&sgst, &cgst, &igst, &packforward, &install, &freight,
					&handling, &misc, &hamali, &mandifee, &fulltax, &insurance)
			}
			//Other Charges--Domestic
			po.SGST = sgst.String
			po.CGST = cgst.String
			po.IGST = igst.String
			po.DPackForward = packforward.String
			po.DInstallation = install.String
			po.DFreight = freight.String
			po.DHandling = handling.String
			po.DMisc = misc.String
			po.DHamali = hamali.String
			po.DMandiFee = mandifee.String
			po.DFullTax = fulltax.String
			po.DInsurance = insurance.String

		}

		//----------Quote Info for Speciality Green Coffee Item Information--------------------
		if po.GCCoffeeType != "regular" {
			log.Println("Entered Quote date & Quote Info Fetch Module for speciaity Coffee")
			sqlStatementSPQ := `SELECT 
							 pom.quote_no,
							 pom.quote_date,
							 pom.quote_price
							 from dbo.pur_gc_po_con_master_newpg pom
							 where pom.pono=$1`
			rowsSPQ, errSPQ := db.Query(sqlStatementSPQ, po.PoNO)
			log.Println("Quote Info Fetch Module for speciaity Coffee Query Executed")
			if errSPQ != nil {
				log.Println("Quote Info Fetch Module for speciaity Coffee from DB failed")
				log.Println(errSPQ.Error())
				return events.APIGatewayProxyResponse{500, headers, nil, errSPQ.Error(), false}, nil
			}
			defer rowsSPQ.Close()
			for rowsSPQ.Next() {
				errSPQ = rowsSPQ.Scan(&po.QuotNo, &po.QuotDate, &po.QuotPrice)
			}
			log.Println(po.QuotNo, po.QuotDate)
		}
		//------Consolidated Finance Status------------------//
		sqlStatementCFS := `SELECT accpay_status,qc_status,payable_amount
						FROM dbo.pur_gc_po_con_master_newpg
						where pono=$1`
		rowsCFS, errCFS := db.Query(sqlStatementCFS, po.PoNO)
		log.Println("Consolidated Finance Status Query Executed")
		if errCFS != nil {
			log.Println("Fetching Consolidated Finance Status from DB failed")
			log.Println(errCFS.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, errCFS.Error(), false}, nil
		}

		defer rowsCFS.Close()
		for rowsCFS.Next() {
			errCFS = rowsCFS.Scan(&po.QCStatus, &po.APStatus, &po.PayableAmount)
		}

		//---------------------Fetch Audit Log Info-------------------------------------//
		log.Println("Fetching Audit Log Info #")
		sqlStatementAI := `select u.username as createduser, gc.created_date,
			gc.description, v.username as modifieduser, gc.modified_date
   		from dbo.auditlog_pur_gc_master_newpg gc
   		inner join dbo.users_master_newpg u on gc.createdby=u.userid
  		 left join dbo.users_master_newpg v on gc.modifiedby=v.userid
   		where gc.pono=$1 order by logid desc limit 1`
		rowsAI, errAI := db.Query(sqlStatementAI, po.PoNO)
		log.Println("Audit Info Fetch Query Executed")
		if errAI != nil {
			log.Println("Audit Info Fetch Query failed")
			log.Println(err9.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}

		for rowsAI.Next() {
			var al AuditLogGCPO
			errAI = rowsAI.Scan(&al.CreatedUserid, &al.CreatedDate, &al.Description, &al.ModifiedUserid, &al.ModifiedDate)
			auditDetails := append(po.AuditLogDetails, al)
			po.AuditLogDetails = auditDetails
			log.Println("added one")

		}
		log.Println("Audit Details:", po.AuditLogDetails)

		res, _ := json.Marshal(po)
		return events.APIGatewayProxyResponse{200, headers, nil, string(res), false}, nil

	} else {
		return events.APIGatewayProxyResponse{200, headers, nil, string("Couldn't find PO Details"), false}, nil
	}
	return events.APIGatewayProxyResponse{200, headers, nil, string("success"), false}, nil
}

func main() {
	lambda.Start(viewGCPODetails)
}
