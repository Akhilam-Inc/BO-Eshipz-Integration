import frappe
import requests
import json
from datetime import datetime

def on_submit(self, method):
    create_eshipz_order(self)


@frappe.whitelist()
def create_eshipz_order(doc):
    try:
        if isinstance(doc, str):
            doc = json.loads(doc)
        elif isinstance(doc, frappe.model.document.Document):
            doc = doc.as_dict()
        
        # Get BO Eshipz Configuration
        if not doc.get('is_return'):
            eshipz_config = frappe.get_single("BO Eshipz Configuration")
            enabled = eshipz_config.is_enable
            token = eshipz_config.get_password('api_token')
            url = eshipz_config.url

            api_endpoint = f"{url}/api/v1/orders"

            if not enabled or not token or not url:
                frappe.msgprint("To create order in eshipz enable the BO Eshipz Configuration!")
                return

            currency = frappe.db.get_single_value("Global Defaults", "default_currency")

            items = [{
                "description": "Pets Products",
                "quantity": "1",
                "value": {
                    "currency": currency,
                    "amount": doc.get('rounded_total')
                },
                "sku": "Pets Products"
            }]
            
            delivery_notes = get_unique_delivery_notes(doc.get('name'))
            delivery_note_docs = [frappe.get_doc("Delivery Note", dn.delivery_note) for dn in delivery_notes]
            if not delivery_note_docs:
                return

            parcels = get_parcels_from_delivery_notes(delivery_note_docs)
            if not parcels:
                frappe.msgprint("No Parcels found in the Delivery Notes. Sending Order without Parcels.")
                return
                
            if not parcels:
                parcels= []
                is_mps = False
            else:
                is_mps = len(parcels) > 1 or parcels[0]['quantity'] > 1

            parsed_date = datetime.strptime(doc.get('posting_date'), "%Y-%m-%d")
            formatted_date = parsed_date.strftime("%Y-%m-%d %H:%M")
            formatted_invoice_date = parsed_date.strftime("%Y-%m-%d")

            if doc.get('custom_bo_eshipz_payment_mode') == "Prepaid":
                is_cod = False
                cod_amount = 0.00
            else:
                is_cod = True
                cod_amount = doc.get('custom_bo_collectible_amount')
                if cod_amount == 0.00:
                    frappe.msgprint("Please set a value for collectible amount.")
                    return

            add = frappe.db.get_value("Address", doc.get('shipping_address_name'), ["address_line1", "address_line2"])
            full_address = ", ".join(filter(None, add)) if add else ""

            country = frappe.db.get_value("Address", doc.get('shipping_address_name'), "country")
            country_code = frappe.db.get_value("Country", country, "code")

            payload = json.dumps({
                "data": [
                    {
                        "order_id": doc.get('name'),
                        "store_name": "other",
                        "order_created_on": formatted_date,
                        "is_cod": is_cod,
                        "shipment_value": doc.get('rounded_total'),
                        "order_currency": currency,
                        "cod_amount": cod_amount,
                        "order_status": "pending",
                        "shipment_type": "Parcel",
                        "receiver_address": {
                            "first_name": doc.get('customer_name'),
                            "address": full_address,
                            "city": frappe.db.get_value("Address", doc.get('shipping_address_name'), "city") or "",
                            "state": frappe.db.get_value("Address", doc.get('shipping_address_name'), "state") or "",
                            "country": country_code.upper(),
                            "email": frappe.db.get_value("Address", doc.get('shipping_address_name'), "email_id") or "",
                            "zipcode": frappe.db.get_value("Address", doc.get('shipping_address_name'), "pincode") or "",
                            "phone": frappe.db.get_value("Address", doc.get('shipping_address_name'), "phone") or ""
                        },
                        "items": items,
                        "is_mps": is_mps,
                        "parcels": parcels,
                        "gst_invoices": [
                            {
                                "invoice_number": doc.get('name'),
                                "invoice_date": formatted_invoice_date,
                                "invoice_value": doc.get('rounded_total'),
                            }
                        ]
                    }
                ]
            })

            headers = {'X-API-TOKEN': token, 'Content-Type': 'application/json'}
            response = requests.post(api_endpoint, headers=headers, data=payload)

            response_json = json.loads(response.text)
            status_value = response_json.get('status')
            if str(status_value) == "201":
                frappe.db.set_value("Sales Invoice", doc.get('name'), "custom_is_eshipz_order_created_bo", True)
                frappe.msgprint("Order successfully created in Eshipz.")
                
    except Exception as e:
        frappe.log_error(message=frappe.get_traceback(), title="Eshipz Order Creation Failed for BO Eshipz")
        frappe.msgprint("Failed to send data to Eshipz.")

 
def get_unique_delivery_notes(sales_invoice):
    unique_delivery_notes = frappe.db.sql("""
    SELECT DISTINCT delivery_note
    FROM `tabSales Invoice Item`
    WHERE delivery_note IS NOT NULL and parent = %(sales_invoice)s
    """,{"sales_invoice":sales_invoice},as_dict=1)

    return unique_delivery_notes


def get_parcels_from_delivery_notes(delivery_notes):
    box_types = {}  
    parcels = []
    
    unique_box_types = set(box.box_type for delivery_note in delivery_notes for box in delivery_note.custom_bo_boxes)
    if not unique_box_types:
        return parcels
    
    for box_type in unique_box_types:
        box_types[box_type] = frappe.db.get_value("Bo Box Type", box_type, ["length", "breadth", "height"], as_dict=True)
    
    for delivery_note in delivery_notes:
        if delivery_note.custom_is_eshipz_order_created_bo == 0:
            for box in delivery_note.custom_boxes:
                if not box.box_type == "Dummy Box":
                    box_type_details = box_types.get(box.box_type, {}) 
                    parcel = {
                        # "reference_number": box.item,
                        "quantity": box.get("qty"),
                        "weight": {
                            "unit_of_measurement": "kg",
                            "value": box.weight or 0.00
                        },
                        "dimensions": {
                            "length": box_type_details.get("length", 0.00),
                            "width": box_type_details.get("breadth", 0.00),
                            "unit_of_measurement": "cm",

                            "height": box_type_details.get("height", 0.00)
                        }
                    }
                    parcels.append(parcel)
    
    return parcels


@frappe.whitelist()
def get_shipping_details(sales_invoice):
    try:
        eshipz_config = frappe.get_single("BO Eshipz Configuration")
        if not eshipz_config.is_enable or not eshipz_config.get_password('api_token'):
            frappe.throw("Please Enable Token In BO Eshipz Configuration Document!")

        token = eshipz_config.get_password('api_token')

        # Prepare URLs and headers for API requests
        shipment_url = f"https://app.eshipz.com/api/v1/get-shipments?db_filters={{\"customer_referenc\": {{\"$in\": [\"{sales_invoice}\"]}}}}"
        tracking_url = "https://app.eshipz.com/api/v1/trackings"
        headers = {
            'Content-Type': 'application/json',
            'X-API-TOKEN': token
        }

        # Prepare data for the tracking request
        tracking_data = json.dumps({
            "q_num": sales_invoice
        })

        shipment_response = requests.get(shipment_url, headers=headers)
        tracking_response = requests.post(tracking_url, headers=headers, data=tracking_data)

        shipment_data = shipment_response.json()
        tracking_data = tracking_response.json()

        if shipment_data and tracking_data:
            vendor_name = shipment_data[0].get('vendor_name')
            tracking_status = tracking_data[0].get('tag')
            tracking_number = tracking_data[0].get('tracking_number')
            return {
                "tracking_status": tracking_status,
                "vendor_name": vendor_name,
                "tracking_number": tracking_number
            }

        else:
            frappe.msgprint("Shipment Not Created")
            frappe.db.set_value("Sales Invoice", sales_invoice, "custom_bo_eshipz_shipment_status", "Shipment Not Created")

    except Exception as e:
        frappe.log_error("Error getting shipping details for BO Eshipz", message=frappe.get_traceback())
        frappe.msgprint("An error occurred while getting shipping details!")




@frappe.whitelist()
def update_shipment_details(sales_invoice, ewaybill_no,posting_date,invoice_amount):
    try:
        eshipz_config = frappe.get_single("BO Eshipz Configuration")
        enabled = eshipz_config.is_enable
        token = eshipz_config.get_password('api_token')

        if not enabled or not token:
            frappe.throw("Please Enable Token In BO Eshipz Configuration Document!")

        parsed_date = datetime.strptime(posting_date, "%Y-%m-%d")
        # formatted_date = parsed_date.strftime("%d/%m/%Y")
        formatted_date = parsed_date.strftime("%Y-%m-%d")

        url = "https://app.eshipz.com/api/v1/update-ewaybill/{customer_reference}".format(customer_reference = sales_invoice)

        payload = json.dumps({
            "gst_invoices": [
                {
                    "ewaybill_number": ewaybill_no.strip(),
                    "invoice_number": sales_invoice.strip(),
                    "invoice_date":formatted_date,
                    "invoice_value":invoice_amount
                }
            ]
        })
        
        headers = {
            'Content-Type': 'application/json',
            'X-API-TOKEN': token
        }

        response = requests.post(url, headers=headers, data=payload)
        
        # Log the API response
        # frappe.log_error("Response from API call for update shipping details", message=response.text)
        if response.status_code == 200:
            frappe.msgprint("Shipment details updated successfully.")
        else:
            frappe.throw(f"Failed to update shipment details: {response.text}")
    except Exception as e:
        frappe.log_error("Update Shipment Details Error for BO Eshipz",message=response.text)
        frappe.msgprint("An Error Occured While Update Shipping Details!")



