frappe.ui.form.on('Sales Invoice', {
    refresh:function(frm){
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