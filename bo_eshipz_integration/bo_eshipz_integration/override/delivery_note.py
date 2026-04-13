import frappe
import requests
import json
from datetime import datetime
from erpnext.stock.stock_ledger import NegativeStockError

def validate(self,method):
    if self.is_return:
        for item in self.custom_bo_boxes:
            item.qty = abs(item.qty) * -1

def before_submit(self,method):
    if self.custom_bo_boxes:
        for idx, item in enumerate(self.custom_bo_boxes, start=1):
            if item.qty and not item.box_type:
                frappe.throw(f"Row <strong>{idx}</strong> has a package value <strong>{item.qty}</strong> but no box type specified. Please correct this before submitting.")

def on_submit(self, method):
    """Update Pick List status when Delivery Note is submitted."""

    # Get unique pick list names
    pick_lists = {item.against_pick_list for item in self.items if item.against_pick_list}

    for pick_list in pick_lists:
        pl = frappe.get_doc("Pick List", pick_list)

        # Pick List is submitted (docstatus = 1)
        # Use db_set → directly updates DB
        pl.db_set("status", "Completed")

# Eshipz Order Creation
@frappe.whitelist()
def create_eshipz_order(self):
    try:
        self = json.loads(self)

        eshipz_config = frappe.get_single("BO Eshipz Configuration")
        enabled = eshipz_config.is_enable
        token = eshipz_config.get_password('api_token')
        url = eshipz_config.url

        api_endpoint = f"{url}/api/v1/orders"

        if not enabled or not token or not url:
            frappe.msgprint("To create auto shipment in order enable the Eshipz applicaion!")
            return
        
        currency = frappe.db.get_single_value("Global Defaults","default_currency")

        items = []
        mapped_item = {
            "description": "Pets Products",
            "quantity": "1",
            "value": {
                "currency": currency,
                "amount": self['rounded_total']
            },
            "sku": "Pets Products"
        }
        items.append(mapped_item)

        parcels = get_parcels_data(self['custom_bo_boxes'])
        if len(parcels) == 1 and parcels[0]['quantity'] == 1:
            is_mps = False
        else:
            is_mps = True

        parsed_date = datetime.strptime(self['posting_date'], "%Y-%m-%d")
        formatted_date = parsed_date.strftime("%Y-%m-%d %H:%M")
        formatted_invoice_date = parsed_date.strftime("%Y-%m-%d")

        if self['custom_bo_eshipz_payment_mode'] == "Prepaid":
            is_cod = False
            cod_amount = 0.00
        else:
            is_cod = True
            cod_amount = self['custom_bo_collectible_amount']

        #Prepare full address
        add = frappe.db.get_value("Address",self['shipping_address_name'],["address_line1","address_line2"])
        if add:
            if add[0] and add[1]:
                full_address = add[0] + "," + add[1]
            else:
                full_address = add[0]
        else:
            full_address = ""
        
        country = frappe.db.get_value("Address",self['shipping_address_name'],"country")
        country_code = frappe.db.get_value("Country",country,"code")
        
        # Prepare data for POST request
        payload = json.dumps({
            "data": [
                {
                    "order_id": self['name'],
                    "store_name": "other",
                    "order_created_on": formatted_date,
                    "is_cod": is_cod,
                    "shipment_value": self['rounded_total'],
                    "order_currency": currency,
                    "cod_amount": cod_amount,
                    "order_status": "pending",
                    "shipment_type": "Parcel",
                    "receiver_address": {
                        "first_name": self['customer_name'],
                        "address": full_address or "",
                        "city": frappe.db.get_value("Address",self['shipping_address_name'],"city") or "",
                        "state": frappe.db.get_value("Address",self['shipping_address_name'],"state") or "",
                        "country":country_code.upper(),
                        "email": frappe.db.get_value("Address",self['shipping_address_name'],"email_id") or "",
                        "zipcode": frappe.db.get_value("Address",self['shipping_address_name'],"pincode") or "",
                        "phone": frappe.db.get_value("Address",self['shipping_address_name'],"phone") or ""
                    },
            
                    "items": items,
                    "is_mps": is_mps,
                    "parcels": parcels,
                }
            ]
        })
        headers = {'X-API-TOKEN': token,'Content-Type': 'application/json'}
        response = requests.post(api_endpoint, headers=headers, data=payload)

        response_json = json.loads(response.text)
        status_value = response_json.get('status')
        if str(status_value) == "201":
            frappe.db.set_value("Delivery Note",self['name'],"custom_is_eshipz_order_created_bo",True)
            frappe.msgprint("Order successfully created in Eshipz.")
            return "success"
    except Exception as e:
        frappe.log_error("Eshipz Order Creation Failed From Delivery Note for BO Eshipz",message=frappe.get_traceback())
        frappe.msgprint("Failed to send data to Eshipz.")


def get_parcels_data(parcels):
    parcel_data = []

    for par in parcels:
        if not par['box_type'] == "Dummy Box":
            parcel = {
                # "reference_number": box.item,
                "quantity": par.get("qty"),
                "weight": {
                    "unit_of_measurement": "kg",
                    "value": par['weight'] or 0.00
                },
                "dimensions": {
                    "length": frappe.db.get_value("Bo Box Type",par['box_type'],"length") or 0.00,
                    "width": frappe.db.get_value("Bo Box Type",par['box_type'],"breadth") or 0.00,
                    "unit_of_measurement": "cm",

                    "height": frappe.db.get_value("Bo Box Type",par['box_type'],"height")
                }
            }
            parcel_data.append(parcel)
    return parcel_data