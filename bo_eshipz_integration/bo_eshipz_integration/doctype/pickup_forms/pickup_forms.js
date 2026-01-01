// Copyright (c) 2025, Akhilam Inc. and contributors
// For license information, please see license.txt

frappe.ui.form.on("Pickup Forms", {
	refresh(frm) {
		// 🟢 Create Eshipz Order (ONLY when not created)
		if (frm.doc.docstatus === 1 && !frm.doc.is_eshipz_order_created) {
			frm.add_custom_button(
				__("Create Eshipz Order"),
				function () {
					frappe.call({
						method:
							"bo_eshipz_integration.bo_eshipz_integration.doctype.pickup_forms.pickup_forms.create_eshipz_order",
						args: {
							doc: frm.doc,
						},
						freeze: true,
						callback: function (r) {
							frm.reload_doc();
						},
					});
				},
				__("Eshipz Actions")
			);
		}

		if (frm.doc.docstatus === 1) {
			frm.add_custom_button(
				__("Fetch Shipment Status"),
				function () {
					frappe.call({
						method:
							"bo_eshipz_integration.bo_eshipz_integration.pickup_scheduler.get_shipping_details_status",
						args: {
							pdf_name: frm.doc.name,
						},
						freeze: true,
						callback: function (r) {
							if (
								r.message === true ||
								(Array.isArray(r.message) && r.message[0] === true)
							) {
								frappe.show_alert({
									message: __("Shipment status updated successfully"),
									indicator: "green",
								});
								frm.reload_doc();
							} else {
								frappe.show_alert({
									message: __("No shipment status change"),
									indicator: "orange",
								});
							}
						},
					});
				},
				__("Eshipz Actions")
			);
		}

		frm.set_query("customer_address", () => {
			return {
				filters: {
					link_doctype: "Customer",
					link_name: frm.doc.customer,
				},
			};
		});

		frm.set_query("receiver_address", () => {
			return {
				filters: {
					link_doctype: "Company",
					link_name: frm.doc.receiver_address,
				},
			};
		});

		frm.set_query("vendor_description", () => {
			return {
				filters: {
					is_reverse: 1,
				},
			};
		});
	},

	customer_address: function (frm) {
		if (frm.doc.customer_address) {
			return frm.call({
				method: "frappe.contacts.doctype.address.address.get_address_display",
				args: {
					address_dict: frm.doc.customer_address,
				},
				callback: function (r) {
					if (r.message) frm.set_value("customer_address_display", r.message);
				},
			});
		} else {
			frm.set_value("customer_address_display", "");
		}
	},

	receiver_address: function (frm) {
		if (frm.doc.receiver_address) {
			return frm.call({
				method: "frappe.contacts.doctype.address.address.get_address_display",
				args: {
					address_dict: frm.doc.receiver_address,
				},
				callback: function (r) {
					if (r.message) frm.set_value("receiver_address_display", r.message);
				},
			});
		} else {
			frm.set_value("receiver_address_display", "");
		}
	},
});
