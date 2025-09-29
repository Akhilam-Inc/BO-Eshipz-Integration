frappe.ui.form.on('Sales Invoice', {
    refresh:function(frm){
        if(frm.doc.custom_is_eshipz_order_created_bo === 1){
            frm.add_custom_button("Fetch Shipment Status", function(){
                frappe.call({
                    method: 'bo_eshipz_integration.bo_eshipz_integration.override.sales_invoice.get_shipping_details',
                    args: {
                        "sales_invoice": frm.doc.name
                    },
                    freeze: true,
                    callback: (r) => {
                        if(r.message){
                            var combine_value = r.message.tracking_number +" - "+r.message.vendor_name
    
                            frm.set_value("custom_bo_eshipz_tracking_number",combine_value)
                            frm.set_value("custom_bo_eshipz_shipment_status",r.message.tracking_status)
                            frm.refresh_field("custom_bo_eshipz_tracking_number","custom_bo_eshipz_shipment_status")
                            frm.save('Update');
                            frm.reload_doc();
                        }
                        else{
                            frm.reload_doc();
                        }
                        
                    }
                })
            },'BO Eshipz Actions');
        } 

        if(frm.doc.custom_is_eshipz_order_created_bo === 1 && frm.doc.ewaybill){
            frm.add_custom_button("Update Ewaybill Details", function(){
                frappe.call({
                    method: 'bo_eshipz_integration.bo_eshipz_integration.override.sales_invoice.update_shipment_details',
                    args: {
                        "sales_invoice": frm.doc.name,
                        "ewaybill_no":frm.doc.ewaybill,
                        "posting_date":frm.doc.posting_date,
                        "invoice_amount":frm.doc.rounded_total
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
            },'BO Eshipz Actions');
        } 


        if(!frm.doc.custom_is_eshipz_order_created_bo && frm.doc.docstatus === 1){
            frm.add_custom_button("Create Eshipz Order", function(){
                frappe.call({
                    method: 'bo_eshipz_integration.bo_eshipz_integration.override.sales_invoice.create_eshipz_order',
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
            },'BO Eshipz Actions');
        } 
    }
});