# Copyright (c) 2025, Akhilam Inc. and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import json
from datetime import datetime
import requests
from collections import defaultdict
from collections import Counter


class DispatchandTransferForm(Document):
	# pass
	def on_submit(self):
		if not self.is_eshipz_order_created:
			create_eshipz_order(self)

@frappe.whitelist()
def get_company_address(company):
    address = frappe.db.sql("""
        SELECT dl.parent AS address_name
        FROM `tabDynamic Link` dl
        WHERE dl.link_doctype = 'Company'
        AND dl.link_name = %s
        AND dl.parenttype = 'Address'
        LIMIT 1
    """, company, as_dict=True)

    return address[0].address_name if address else None


@frappe.whitelist()
def create_eshipz_order(doc):
	try:
		if isinstance(doc, str):
			doc = json.loads(doc)
		elif isinstance(doc, frappe.model.document.Document):
			doc = doc.as_dict()
		
		# Get Eshipz configuration
		eshipz_config = frappe.get_single("BO Eshipz Configuration")
		enabled = eshipz_config.is_enable
		token = eshipz_config.get_password('api_token')
		url = eshipz_config.url

		api_endpoint = f"{url}/api/v1/orders"

		if not enabled or not token or not url:
			frappe.msgprint("To create order in eshipz enable the BO Eshipz Configuration!")
			return

		currency = frappe.db.get_single_value("Global Defaults", "default_currency")

		# -----------------------
		# Get sender/receiver info based on order_type
		# -----------------------
		order_type = doc.get("order_type")
		receiver_name, receiver_address_link = None, None
		sender_name, sender_address_link = None, None


		if order_type == "Dispatch":
			receiver_name = doc.get("customer")
			receiver_address_link = doc.get("customer_address")
			sender_name = doc.get("sender_name")
			sender_address_link =  doc.get("sender_address")

		elif order_type == "Samples and Spare Parts":
			sender_name = doc.get("sender_name")
			sender_address_link = doc.get("sender_address")
			receiver_name = doc.get("sales_person")
			receiver_address_link = doc.get("sales_person_address")

		# safety check
		if not receiver_name or not receiver_address_link:
			frappe.msgprint("Receiver details are not set. Please check before creating order.")
			return
		if not sender_name or not sender_address_link:
			frappe.msgprint("Sender details are not set. Please check before creating order.")
			return

		# helper to build address block
		def build_address(name, address_link):
			add = frappe.db.get_value("Address", address_link,
				["address_line1", "address_line2", "city", "state", "country", "pincode", "phone", "email_id"],
				as_dict=True)
			if not add:
				return {}
			country_code = frappe.db.get_value("Country", add.country, "code") if add.country else ""
			return {
				"first_name": name,
				"address": ", ".join(filter(None, [add.address_line1, add.address_line2])),
				"city": add.city or "",
				"state": add.state or "",
				"country": (country_code or "").upper(),
				"email": add.email_id or "",
				"zipcode": add.pincode or "",
				"phone": add.phone or ""
			}

		receiver_address_block = build_address(receiver_name, receiver_address_link)
		sender_address_block = build_address(sender_name, sender_address_link)

		# -----------------------
		# Items & parcels
		# -----------------------
		items = [{
			"description": "Pets Products",
			"quantity": "1",
			"value": {
				"currency": currency,
				"amount": doc.get('order_amount')
			},
			"sku": "Pets Products"
		}]

		parcels = get_parcels(doc.get("name"))
		if not parcels:
			frappe.msgprint(f"No parcels found in Dispatch and Transfer Form: {doc.get('name')}.")
			return
		is_mps = len(parcels) > 1 or parcels[0]['quantity'] > 1

		parsed_date = datetime.strptime(doc.get('order_date'), "%Y-%m-%d")
		now = datetime.now()
		parsed_date = parsed_date.replace(hour=now.hour, minute=now.minute)
		formatted_date = parsed_date.strftime("%Y-%m-%d %H:%M")
		is_cod = True if doc.get('is_code') else False
		cod_amount = doc.get("cod_amount") if is_cod else 0

		payload = json.dumps({
			"data": [
				{
					"order_id": doc.get('name'),
					"store_name": "other",
					"order_created_on": formatted_date,
					"is_cod": is_cod,
					"shipment_value": doc.get('order_amount'),
					"order_currency": currency,
					"cod_amount": cod_amount,
					"order_status": "pending",
					"shipment_type": "Parcel",
					"receiver_address": receiver_address_block,
					"sender_address": sender_address_block,
					"items": items,
					"is_mps": is_mps,
					"parcels": parcels,
					"invoice_number": doc.get('name')
				}
			]
		})

		headers = {'X-API-TOKEN': token, 'Content-Type': 'application/json'}
		response = requests.post(api_endpoint, headers=headers, data=payload)

		response_json = json.loads(response.text)
		status_value = response_json.get('status')
		if str(status_value) == "201":
			frappe.db.set_value("Dispatch and Transfer Form", doc.get('name'), "is_eshipz_order_created", True)
			if doc.get('pick_list_ref'):
				frappe.db.set_value("Pick List", doc.get('pick_list_ref'), "custom_is_eshipz_order_created", True)
			frappe.msgprint("Order successfully created in Eshipz.")

	except Exception as e:
		frappe.log_error(message=frappe.get_traceback(), title="BO Eshipz Order Creation Failed For Dispatch and Transfer Form")
		frappe.msgprint("Failed to send data to Eshipz.")



def get_parcels(doc_name):
	"""
	Fetch parcels for the given document name.
	"""
	try:
		doc = frappe.get_doc("Dispatch and Transfer Form", doc_name)
		parcels = []
		# Group parcels by box_type and sum qty and weight
		grouped = defaultdict(lambda: {"quantity": 0, "weight": 0.0})

		for item in doc.parcels:
			box_type = item.box_type
			grouped[box_type]["quantity"] += item.qty
			grouped[box_type]["weight"] += float(item.weight or 0)

		# Prepare the result as a list of dicts
		result = []
		for box_type, values in grouped.items():
			result.append({
				"box_type": box_type,
				"quantity": values["quantity"],
				"weight": values["weight"]
			})
	
		for re in result:
			box_dimension = frappe.db.get_value("Bo Box Type", re.get("box_type"), ["length", "breadth", "height","weight"], as_dict=True)
			parcel = {
				# "reference_number": box.item,
				"quantity": re.get("quantity"),
				"weight": {
					"unit_of_measurement": "kg",
					"value":box_dimension.get("weight", 0.00)
				},
				"dimensions": {
					"length": box_dimension.get("length", 0.00),
					"width": box_dimension.get("breadth", 0.00),
					"unit_of_measurement": "cm",

					"height": box_dimension.get("height", 0.00)
				}
			}
			parcels.append(parcel)
		# frappe.log_error(message=f"Parcels fetched: {parcels}", title="Fetched Parcels")
		return parcels
			
	except Exception as e:
		frappe.log_error(message=frappe.get_traceback(), title="Error Fetching Parcels")
		return []
	

@frappe.whitelist()
def get_pick_list_boxes(pick_list_name):
	try:
		pick_list = frappe.get_doc("Pick List", pick_list_name)
		boxes = []
		
		box_dict = {}
		for item in pick_list.locations:
			if item.custom_bo_box_type:
				if item.custom_bo_box_type not in box_dict:
					box_dict[item.custom_bo_box_type] = 0
				box_dict[item.custom_bo_box_type] += item.qty or 0
		if box_dict:
			boxes = [{"box_name": box_type, "count": count} for box_type, count in box_dict.items()]
			return boxes
		else:
			frappe.throw("No boxes found in the Pick List.")
			return []
	except Exception as e:
		frappe.log_error(message=frappe.get_traceback(), title="Error Fetching Pick List Boxes")
		return []