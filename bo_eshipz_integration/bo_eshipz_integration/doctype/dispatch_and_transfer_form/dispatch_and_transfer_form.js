// Copyright (c) 2025, Akhilam Inc. and contributors
// For license information, please see license.txt

frappe.ui.form.on("Dispatch and Transfer Form", {
	refresh(frm) {
        if(!frm.doc.is_eshipz_order_created && frm.doc.docstatus === 1){
        //  if(frm.doc.docstatus === 1){
            frm.add_custom_button("Create Eshipz Order", function(){
                frappe.call({
                    method: 'bo_eshipz_integration.bo_eshipz_integration.doctype.dispatch_and_transfer_form.dispatch_and_transfer_form.create_eshipz_order',
                    args: {
                        doc: frm.doc,
                    },
                    freeze: true,
                    callback: (r) => {
                        if(r.message){
                            frm.reload_doc();
                        }
                        else{
                            frm.reload_doc();
                        }
                        
                    }
                })
            },'Eshipz Actions');
        }

		frm.set_query('customer_address', () => {
			return {
				filters: {
					'link_doctype':'Customer',
					'link_name': frm.doc.customer
				}
			}
		})

		frm.set_query('sender_address', () => {
			return {
				filters: {
					'link_doctype':'Company',
					'link_name': frm.doc.sender_address
				}
			}
		})
        
		frm.set_query('sales_person_address', () => {
			return {
				filters: {
					'link_doctype':'Sales Partner',
					'link_name': frm.doc.sales_person
				}
			}
		})
		frm.set_query('pick_list_ref', () => {
			return {
				filters: {
					'custom_is_eshipz_order_created':0,
					'purpose': 'Material Transfer',
					'creation': ['>=', '2025-10-01 00:00:00']
				}
			}
		})
	},
    customer_address:function(frm){
		if(frm.doc.customer_address){
			return frm.call({
			method: "frappe.contacts.doctype.address.address.get_address_display",
			args: {
			   "address_dict": frm.doc.customer_address
			},
			callback: function(r) {
			  if(r.message)
				  frm.set_value("customer_address_display", r.message);
				}
		   });
		  }
		  else{
			  frm.set_value("customer_address_display", "");
		  }
	},
	sender_address:function(frm){
		if(frm.doc.sender_address){
			return frm.call({
			method: "frappe.contacts.doctype.address.address.get_address_display",
			args: {
			   "address_dict": frm.doc.sender_address
			},
			callback: function(r) {
			  if(r.message)
				  frm.set_value("sender_address_display", r.message);
				}
		   });
		  }
		  else{
			  frm.set_value("sender_address_display", "");
		  }
	},
	sales_person_address:function(frm){
		if(frm.doc.sales_person_address){
			return frm.call({
			method: "frappe.contacts.doctype.address.address.get_address_display",
			args: {
			   "address_dict": frm.doc.sales_person_address
			},
			callback: function(r) {
			  if(r.message)
				  frm.set_value("sales_person_address_display", r.message);
				}
		   });
		  }
		  else{
			  frm.set_value("sales_person_address_display", "");
		  }
	},
	pick_list_ref:function(frm){
		if(frm.doc.pick_list_ref){
			return frappe.call({
				method: "bo_eshipz_integration.bo_eshipz_integration.doctype.dispatch_and_transfer_form.dispatch_and_transfer_form.get_pick_list_boxes",
				args: {
					"pick_list_name": frm.doc.pick_list_ref
				},
				callback: function(r) {
					console.log(r.message)
					if(r.message){
						frm.clear_table("parcels");
				
						for(let i=0; i<r.message.length; i++){
							let box = r.message[i];
							let row = frm.add_child("parcels");
							row.box_type = box.box_name;
							row.weight = box.weight;
							row.item = box.item_code;
							row.qty = box.count;
						}
						frm.refresh_field("parcels");
						frm.save()
					}
				}
					
			});
		}
	},

	order_type: function(frm) {
        let default_company = frappe.defaults.get_user_default("Company");
        if (!default_company) return;

        frappe.call({
            method: "bo_eshipz_integration.bo_eshipz_integration.doctype.dispatch_and_transfer_form.dispatch_and_transfer_form.get_company_address",
            args: { company: default_company },
            callback: function(r) {
                if (r.message) {
                    let address_name = r.message;  // e.g. "ADDR-0001"
                    console.log("Address Name:", address_name);

                    if(frm.doc.order_type === "Dispatch" || frm.doc.order_type === "Samples and Spare Parts") {
                        frm.set_value("sender_name", default_company);
                        frm.set_value("sender_address", address_name);
                    }
                }
            }
        });
    }
});
