import json
from collections import OrderedDict, defaultdict
from itertools import groupby
import re
import frappe
from frappe import _
from frappe.model.document import Document
from frappe.model.mapper import map_child_doc
from frappe.utils import ceil, cint, floor, flt, get_link_to_form

from erpnext.selling.doctype.sales_order.sales_order import (
	make_delivery_note as create_delivery_note_from_sales_order,
)

class PickList(Document):
	def validate(self):
		validate_item_locations(self)
		if self.locations:
			so = self.locations[0].sales_order
			if so:
				self.custom_reference_of_so = so

def validate_item_locations(pick_list):
	if not pick_list.locations:
		frappe.throw(_("Add items in the Item Locations table"))

@frappe.whitelist()
def create_delivery_note(source_name, target_doc=None):
	pick_list = frappe.get_doc("Pick List", source_name)
	validate_item_locations(pick_list)
	sales_dict = dict()
	sales_orders = []
	delivery_note = None
	for location in pick_list.locations:
		if location.sales_order:
			sales_orders.append(
				frappe.db.get_value(
					"Sales Order", location.sales_order, ["customer", "name as sales_order"], as_dict=True
				)
			)

	for customer, rows in groupby(sales_orders, key=lambda so: so["customer"]):
		sales_dict[customer] = {row.sales_order for row in rows}

	if sales_dict:
		delivery_note = create_dn_with_so(sales_dict, pick_list)
		
	if not all(item.sales_order for item in pick_list.locations):
		delivery_note = create_dn_wo_so(pick_list)
	
	
	frappe.msgprint(_("Delivery Note(s) created for the Pick List"))
	return delivery_note


def create_dn_wo_so(pick_list):
	delivery_note = frappe.new_doc("Delivery Note")

	item_table_mapper_without_so = {
		"doctype": "Delivery Note Item",
		"field_map": {
			"rate": "rate",
			"name": "name",
			"parent": "",
		},
	}
	consolidated_item_data(pick_list,delivery_note)
	map_pl_locations(pick_list, item_table_mapper_without_so, delivery_note)
	validate_custom_box_types(pick_list,delivery_note)
	delivery_note.insert(ignore_mandatory=True)

	return delivery_note


def create_dn_with_so(sales_dict, pick_list):
	delivery_note = None

	item_table_mapper = {
		"doctype": "Delivery Note Item",
		"field_map": {
			"rate": "rate",
			"name": "so_detail",
			"parent": "against_sales_order",
		},
		"condition": lambda doc: abs(doc.delivered_qty) < abs(doc.qty) and doc.delivered_by_supplier != 1,
	}

	for customer in sales_dict:
		for so in sales_dict[customer]:
			delivery_note = None
			kwargs = {"skip_item_mapping": True}
			delivery_note = create_delivery_note_from_sales_order(so, delivery_note,  kwargs=kwargs)
			
			break
		if delivery_note:
			# map all items of all sales orders of that customer
			for so in sales_dict[customer]:
				map_pl_locations(pick_list, item_table_mapper, delivery_note, so)
			
			consolidated_item_data(pick_list,delivery_note)
			delivery_note.flags.ignore_mandatory = True
			delivery_note.insert()
			update_packed_item_details(pick_list, delivery_note)
			validate_custom_box_types(pick_list,delivery_note)
			delivery_note.save()

	return delivery_note

def consolidated_item_data(pick_list, delivery_note):
	"""
	Consolidate items from pick list to delivery note, including product bundles from sales order.
	
	Args:
		pick_list: Source pick list with items
		delivery_note: Target delivery note to populate
	"""
	from collections import defaultdict

	def _get_product_bundles() -> dict[str, str]:
		"""
		Retrieve product bundles from pick list locations.
		
		Returns:
			Dict[so_item_row: item_code] of product bundle items
		"""
		product_bundles = {}
		for item in pick_list.locations:
			if item.product_bundle_item:
				product_bundles[item.product_bundle_item] = frappe.db.get_value(
					"Sales Order Item",
					item.product_bundle_item,
					"item_code",
				)
		return product_bundles

	# Retrieve product bundles from pick list
	product_bundles = _get_product_bundles()
	
	# Separate regular items
	regular_items = [
		item for item in pick_list.locations 
		if not item.product_bundle_item
	]
	
	# Group regular items
	grouped_data = defaultdict(lambda: {'qty': 0})
	
	for item in regular_items:
		key = (item.item_code, item.batch_no, item.warehouse)
		for k, v in vars(item).items():
			if k == 'qty':
				grouped_data[key]['qty'] += v
			elif k not in grouped_data[key]:
				grouped_data[key][k] = v
	
	# Reset delivery note items
	delivery_note.items = []
	idx = 1
	
	# Process regular grouped items
	for key, values in grouped_data.items():
		grouped_dict = {k: v for k, v in zip(['item_code', 'batch_no', 'warehouse'], key)}
		values['serial_and_batch_bundle'] = ''
		grouped_dict.update(values)
		grouped_dict['idx'] = idx
		grouped_dict['against_sales_order'] = values.get('sales_order', '')
		grouped_dict['so_detail'] = values.get('sales_order_item', '')
		
		if grouped_dict['so_detail']:
			# Fetch discount and is_free_item
			sales_order_data = frappe.db.get_value(
				"Sales Order Item",
				grouped_dict['so_detail'],
				['discount_percentage', 'discount_amount', 'is_free_item','rate','price_list_rate','pricing_rules'],
				as_dict=True
			)
			grouped_dict['discount_percentage'] = sales_order_data.discount_percentage or 0
			grouped_dict['discount_amount'] = sales_order_data.discount_amount or 0
			grouped_dict['is_free_item'] = sales_order_data.is_free_item or 0
			grouped_dict['price_list_rate'] = sales_order_data.price_list_rate or 0
			grouped_dict['rate'] = sales_order_data.rate or 0
			grouped_dict['pricing_rules'] = sales_order_data.pricing_rules or ""
			grouped_dict['against_pick_list'] = pick_list.name
		else:
			grouped_dict['discount_percentage'] = 0
			grouped_dict['discount_amount'] = 0
			grouped_dict['is_free_item'] = 0
			grouped_dict['rate'] = 0
			grouped_dict['price_list_rate'] =  0
			grouped_dict['pricing_rules'] = ""
		
		grouped_dict['custom_bo_boxes'] = ''
		idx += 1
		delivery_note.append("items", grouped_dict)
	
	# Process product bundles from sales order
	if product_bundles:
		for so_item, item_codes in product_bundles.items():
			bundle_items = frappe.db.get_all(
				"Sales Order Item", 
				filters={
					'name': so_item,
					'item_code': ['in', item_codes]
				},
				fields=['name', 'parent', 'item_code', 'qty', 'warehouse', 'discount_percentage', 'discount_amount', 'is_free_item','rate','price_list_rate','pricing_rules']
			)
			
			for bundle_item in bundle_items:
				bundle_dict = {
					'idx': idx,
					'item_code': bundle_item.item_code,
					'price_list_rate':bundle_item.price_list_rate,
					'qty': bundle_item.qty,
					'rate':bundle_item.rate,
					'pricing_rules':bundle_item.pricing_rules,
					'warehouse': bundle_item.warehouse,
					'against_sales_order': bundle_item.parent,
					'so_detail': bundle_item.name,
					'serial_and_batch_bundle': '',
					'discount_percentage': bundle_item.discount_percentage or 0,
					'discount_amount': bundle_item.discount_amount or 0,
					'is_free_item': bundle_item.is_free_item or 0
				}
				
				delivery_note.append("items", bundle_dict)
				idx += 1
	
	return delivery_note


def validate_custom_box_types(pick_list, delivery_note):
    unique_boxes = {}
    box_count = {}
    for item in pick_list.locations:
        key = (item.custom_bo_box_type,item.custom_bo_box_count)
        if key not in unique_boxes.keys():
            unique_boxes[key] = 1
            if item.custom_bo_box_type not in box_count.keys():
                box_count[item.custom_bo_box_type] = 1
            else:
                box_count[item.custom_bo_box_type] += 1
    
    delivery_note.custom_bo_boxes = []
    for custom_box_type, qty in box_count.items():
        new_row = delivery_note.append('custom_bo_boxes', {})
        new_row.box_type = custom_box_type
        new_row.item = frappe.db.get_value("Bo Box Type", custom_box_type, "item") or ""
        new_row.weight = frappe.db.get_value("Bo Box Type", custom_box_type, "weight") or 0.00
        new_row.qty = qty
    
    delivery_note.custom_bo_total_box_count = sum(item.qty for item in delivery_note.custom_bo_boxes if item.box_type != "Dummy Box")
    delivery_note.custom_bo_total_box_net_weight = sum(item.weight * item.qty for item in delivery_note.custom_bo_boxes if item.box_type != "Dummy Box")


def map_pl_locations(pick_list, item_mapper, delivery_note, sales_order=None):
	for location in pick_list.locations:
		if location.sales_order != sales_order or location.product_bundle_item:
			continue

		if location.sales_order_item:
			sales_order_item = frappe.get_doc("Sales Order Item", location.sales_order_item)
		else:
			sales_order_item = None

		source_doc = sales_order_item or location

		dn_item = map_child_doc(source_doc, delivery_note, item_mapper)
		if dn_item:
			dn_item.pick_list_item = location.name
			dn_item.warehouse = location.warehouse
			dn_item.qty = flt(location.picked_qty) / (flt(location.conversion_factor) or 1)
			dn_item.batch_no = location.batch_no
			dn_item.serial_no = location.serial_no
			dn_item.against_sales_order = location.sales_order
			dn_item.so_detail = location.sales_order_item

			update_delivery_note_item(source_doc, dn_item, delivery_note)
	
	add_product_bundles_to_delivery_note(pick_list, delivery_note, item_mapper)
	set_delivery_note_missing_values(delivery_note)

	delivery_note.pick_list = pick_list.name
	delivery_note.company = pick_list.company
	delivery_note.customer = frappe.get_value("Sales Order", sales_order, "customer")


def add_product_bundles_to_delivery_note(pick_list: "PickList", delivery_note, item_mapper) -> None:
	"""Add product bundles found in pick list to delivery note.

	When mapping pick list items, the bundle item itself isn't part of the
	locations. Dynamically fetch and add parent bundle item into DN."""
	product_bundles = pick_list._get_product_bundles()
	product_bundle_qty_map = pick_list._get_product_bundle_qty_map(product_bundles.values())

	for so_row, item_code in product_bundles.items():
		sales_order_item = frappe.get_doc("Sales Order Item", so_row)
		dn_bundle_item = map_child_doc(sales_order_item, delivery_note, item_mapper)
		dn_bundle_item.qty = pick_list._compute_picked_qty_for_bundle(
			so_row, product_bundle_qty_map[item_code]
		)
		update_delivery_note_item(sales_order_item, dn_bundle_item, delivery_note)


def update_packed_item_details(pick_list: "PickList", delivery_note) -> None:
	"""Update stock details on packed items table of delivery note."""

	def _find_so_row(packed_item):
		for item in delivery_note.items:
			if packed_item.parent_detail_docname == item.name:
				return item.so_detail

	def _find_pick_list_location(bundle_row, packed_item):
		if not bundle_row:
			return
		for loc in pick_list.locations:
			if loc.product_bundle_item == bundle_row and loc.item_code == packed_item.item_code:
				return loc

	for packed_item in delivery_note.packed_items:
		so_row = _find_so_row(packed_item)
		location = _find_pick_list_location(so_row, packed_item)
		if not location:
			continue
		packed_item.warehouse = location.warehouse
		packed_item.batch_no = location.batch_no
		packed_item.serial_no = location.serial_no



def update_delivery_note_item(source, target, delivery_note):
	cost_center = frappe.db.get_value("Project", delivery_note.project, "cost_center")
	if not cost_center:
		cost_center = get_cost_center(source.item_code, "Item", delivery_note.company)

	if not cost_center:
		cost_center = get_cost_center(source.item_group, "Item Group", delivery_note.company)

	target.cost_center = cost_center


def get_cost_center(for_item, from_doctype, company):
	"""Returns Cost Center for Item or Item Group"""
	return frappe.db.get_value(
		"Item Default",
		fieldname=["buying_cost_center"],
		filters={"parent": for_item, "parenttype": from_doctype, "company": company},
	)


def set_delivery_note_missing_values(target):
	target.run_method("set_missing_values")
	target.run_method("set_po_nos")
	target.run_method("calculate_taxes_and_totals")