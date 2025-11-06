# Copyright (c) 2025, Akhilam Inc. and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
import json
import requests
from datetime import datetime
from collections import defaultdict

class PickupForms(Document):
	def on_submit(self):
		if not self.is_eshipz_order_created:
			create_eshipz_order(self)


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

		# api_endpoint = f"{url}/api/v1/create-shipments"
		api_endpoint = "https://app.eshipz.com/api/v1/create-shipments"

		if not enabled or not token or not url:
			frappe.msgprint("To create order in eshipz enable the BO Eshipz Configuration!")
			return

		currency = frappe.db.get_single_value("Global Defaults", "default_currency")
		# sender (residential)
		sender = build_address(doc.get("customer"), doc.get("customer_address"), address_type="residential")

		# receiver (business)
		receiver = build_address(doc.get("receiver_name"), doc.get("receiver_address"), address_type="business")

		parcels = get_parcels(doc.get("name"),currency)
		if not parcels:
			frappe.msgprint("No parcels found for this Pickup Form.")
			return	

		parsed_date = datetime.strptime(doc.get('actual_pickup_date'), "%Y-%m-%d")
		now = datetime.now()
		parsed_date = parsed_date.replace(hour=now.hour, minute=now.minute)
		formatted_date= parsed_date.strftime("%Y-%m-%d %H:%M")

		# Prepare the payload
		payload = {
			"billing": {
				"paid_by": "shipper"
			},
			"vendor_id": doc.get("vendor_id"),
			"description": doc.get("vendor_description"),
			"slug": doc.get("slug"),
			"purpose": "commercial",
			"order_source": "manual",
			"parcel_contents": "Pets Grooming Products",
			"is_document": "false",
			"service_type": doc.get("service_type"),
			"charged_weight": {
				"unit": "KG",
				"value": doc.get("total_weight")
			},
			"customer_reference": doc.get("name"),
			"invoice_number": doc.get("name"),
			"invoice_date": formatted_date,
			"is_cod": "false",
			"collect_on_delivery": {
				"amount": 0,
				"currency": currency
			},
			"shipment": {
				"ship_from": sender,
				"ship_to": receiver,
				"return_to": receiver,
				"is_reverse": "true",
				"is_to_pay": "true",
				"parcels": parcels
			}
		}

		headers = {'X-API-TOKEN': token, 'Content-Type': 'application/json'}
		response = requests.post(api_endpoint, headers=headers, json=payload)

		if response.status_code == 200:
			resp_json = response.json()
			if resp_json.get("meta", {}).get("code") == 200:
				tracking_numbers = resp_json.get("data", {}).get("tracking_numbers", [])
				if tracking_numbers:
					tracking_number = tracking_numbers[0]
					final_tracking_number = f"{tracking_number}-{doc.get('slug')}"
					frappe.db.set_value("Pickup Forms", doc.get('name'), "eshipz_tracking_number", final_tracking_number)
				
				# ✅ Mark order as created
				frappe.db.set_value("Pickup Forms",doc.get('name'),"is_eshipz_order_created",1)
				frappe.msgprint(f"Eshipz order created successfully. AWB: {resp_json['data']['files']['label']['label_meta']['awb']}")
			else:
				frappe.msgprint(f"Failed to create Eshipz shipment: {resp_json.get('meta', {}).get('message')}")
				frappe.log_error(
					message=f"Eshipz API Error:\n{frappe.as_json(resp_json, indent=2)}\n\nPayload Sent:\n{frappe.as_json(payload, indent=2)}",
					title=f"Eshipz Order Creation Failed for Pickup Form {doc.get('name')}"
				)
			
	except Exception as e:
		frappe.log_error(message=frappe.get_traceback(), title="BO Eshipz Order Creation Failed For Pickup Form")
		frappe.msgprint("Failed to send data to Eshipz.")


def build_address(name, address_link, address_type="residential"):
	add = frappe.db.get_value(
		"Address",
		address_link,
		["address_line1", "address_line2", "city", "state", "country", "pincode", "phone", "email_id"],
		as_dict=True
	)
	if not add:
		return {}

	country_code = frappe.db.get_value("Country", add.country, "code") if add.country else ""

	return {
		"contact_name": name,
		"company_name": name,
		"street1": add.address_line1 or "",
		"street2": add.address_line2 or "",
		"city": add.city or "",
		"state": add.state or "",
		"postal_code": add.pincode or "",
		"phone": add.phone or "9999999999",
		"email": add.email_id or "",
		"country": (country_code or "").upper(),
		"type": address_type
	}

def get_parcels(doc_name, currency):
	"""
	Fetch parcels for the given Pickup Form and return in Eshipz format.
	"""
	try:
		doc = frappe.get_doc("Pickup Forms", doc_name)
		parcels = []

		# Group parcels by box_type
		grouped = defaultdict(lambda: {"quantity": 0, "weight": 0.0})

		for item in doc.parcels:
			box_type = item.box_type
			grouped[box_type]["quantity"] += item.qty
			grouped[box_type]["weight"] += float(item.weight or 0)

		# Prepare parcels
		for box_type, values in grouped.items():
			box_dimension = frappe.db.get_value(
				"Bo Box Type",
				box_type,
				["length", "breadth", "height", "weight"],
				as_dict=True
			) or {}

			weight_val = values["weight"] or box_dimension.get("weight", 0.0)

			parcel = {
				"description": box_type,
				"box_type": "custom",
				"quantity": values["quantity"],
				"weight": {
					"value": weight_val,
					"unit": "kg"
				},
				"dimension": {
					"length": box_dimension.get("length", 0.0),
					"width": box_dimension.get("breadth", 0.0),
					"height": box_dimension.get("height", 0.0),
					"unit": "cm"
				},
				"items": [
					{
						"description": "Pets Products",
						"origin_country": "IN",
						"sku": "",
						"hs_code": "",
						"variant": "",
						"quantity": "1",
						"price": {
							"amount": float(doc.get("order_amount") or 0.0),
							"currency": currency
						},
						"weight": {
							"value": doc.get("total_weight") or 0.0,
							"unit": "kg"
						}
					}
				]
			}

			parcels.append(parcel)

		return parcels

	except Exception:
		frappe.log_error(message=frappe.get_traceback(), title="Error Fetching Parcels")
		return []
