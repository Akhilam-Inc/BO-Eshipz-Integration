frappe.ui.form.on('Delivery Note', {
    refresh:function(frm){
        if(!frm.doc.custom_is_eshipz_order_created_bo && frm.doc.docstatus === 1){
            frm.add_custom_button("BO Create Eshipz Order", function(){
                frappe.call({
                    method: 'bo_eshipz_integration.bo_eshipz_integration.override.delivery_note.create_eshipz_order',
                    args: {
                        "self": frm.doc
                    },
                    freeze: true,
                    callback: (r) => {
                        if(r.message =="success"){
                            frm.reload_doc();
                        }   
                    }
                })
            });
        } 
    }
});