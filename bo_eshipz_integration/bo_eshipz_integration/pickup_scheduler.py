import frappe
import requests
import json
from datetime import datetime
from frappe.utils import add_days
from frappe.utils.file_manager import save_file
from frappe import _
from bo_eshipz_integration.bo_eshipz_integration.scheduler import (
    call_tracking_api,
    call_pod_api,
    schedule_next_batch,
)
import os
from urllib.parse import urlparse

BATCH_SIZE = 50

# ---------------------------------------------------Update Eshipz Shipment Actual Delivery Date-------------------------------------------------

@frappe.whitelist()
def schedule_update_delivery_date_for_pf(start=0):
    
    try:
        pickup_dispatch_forms = frappe.db.get_all(
            "Pickup Forms",
            filters={
            "docstatus": 1,
            "custom_is_eshipz_order_created": 1,
            "eshipz_shipment_status": ["not in", ["", "Shipment Not Created"]],
            "eshipz_tracking_number": ["!=", ""],
            "actual_delivery_date": ["in", [None, ""]],
            },
            fields=["name"],
            order_by="creation desc",
            limit=BATCH_SIZE,
            start=start
        )

        if not pickup_dispatch_forms:
            frappe.log_error("Batch Processing Complete", "No more pickup_dispatch_forms left to process.")
            return

        updated_pickup_dispatch_forms = []
        for pdf in pickup_dispatch_forms:
            get_delivery_date_from_shipping_details(pdf["name"])
            updated_pickup_dispatch_forms.append(pdf["name"])

        if updated_pickup_dispatch_forms:
            frappe.log_error("Updated Invoices For Delivery date", f"Successfully processed {len(updated_pickup_dispatch_forms)} invoices in this batch")

        # Schedule next batch if needed
        if len(pickup_dispatch_forms) == BATCH_SIZE:
            schedule_next_batch('bo_eshipz_integration.bo_eshipz_integration.pickup_scheduler.schedule_update_delivery_date_for_pf', start + BATCH_SIZE)

    except Exception as e:
        frappe.log_error("Error Updating Pickup Forms of Delivery Date", frappe.get_traceback())

def get_delivery_date_from_shipping_details(pdf_name):
    """
    Retrieve delivery date from the tracking API and update the Dispatch Forms.
    """
    try:
        tracking_data = call_tracking_api(pdf_name)
        has_tracking_data = isinstance(tracking_data, list) and len(tracking_data) > 0

        if has_tracking_data:
            delivery_date_str = tracking_data[0].get('delivery_date')
            if delivery_date_str:
                delivery_date = datetime.strptime(delivery_date_str, "%a, %d %b %Y %H:%M:%S %Z")
                formatted_delivery_date = delivery_date.strftime("%Y-%m-%d")
                frappe.db.set_value("Pickup Forms", pdf_name, {
                    "actual_delivery_date": formatted_delivery_date,
                    "eshipz_shipment_status": "Delivered"
                })

    except Exception as e:
        frappe.log_error(f"Error updating actual delivery date for Pickup Forms {pdf_name}", message=frappe.get_traceback())


# -------------------------------------------------Update Eshipz Shipment Status------------------------------------------------------
    
@frappe.whitelist()
def schedule_update_shipping_detail_status_for_pf(start=0):
    
    try:
        pickup_dispatch_forms = frappe.db.get_all(
            "Pickup Forms",
            filters={
            "docstatus": 1,
            "is_eshipz_order_created": 1,
            "actual_delivery_date": ["in", [None, ""]],
            "eshipz_shipment_status": ["not in", ["Delivered", "Shipment Not Created"]],
            },
            fields=["name"],
            order_by="creation desc",
            limit=BATCH_SIZE,
            start=start
        )

        if not pickup_dispatch_forms:
            frappe.log_error("Batch Processing Complete", "No more Pickup Forms left to process.")
            return

        updated_pickup_dispatch_forms= []
        for pdf in pickup_dispatch_forms:
            result = get_shipping_details_status(pdf['name'])
            if result and result[0]:
                updated_pickup_dispatch_forms.append(result[1])

        if updated_pickup_dispatch_forms:
            frappe.log_error("Updated Pickup Forms For Detail Status", f"Successfully updated {len(updated_pickup_dispatch_forms)} invoices in this batch")

        # Schedule next batch if needed
        if len(pickup_dispatch_forms) == BATCH_SIZE:
            schedule_next_batch('bo_eshipz_integration.bo_eshipz_integration.pickup_scheduler.schedule_update_shipping_detail_status_for_pf', start + BATCH_SIZE)

    except Exception as e:
        frappe.log_error("Error While Update Shipment Status From Scheduler For Pickup Forms", message=frappe.get_traceback())

def get_shipping_details_status(pdf_name):
    try:
        tracking_data = call_tracking_api(pdf_name)
        has_tracking_data = isinstance(tracking_data, list) and len(tracking_data) > 0
        if has_tracking_data:
            tracking_status = tracking_data[0].get('tag')
            existing_status = frappe.db.get_value("Pickup Forms", pdf_name, "eshipz_shipment_status")
            if existing_status != "Delivered":
                frappe.db.set_value("Pickup Forms", pdf_name, {
                    "eshipz_shipment_status": tracking_status,
                })
                return (True, pdf_name)

    except Exception as e:
        frappe.log_error(f"Error getting shipping details status for Pickup Forms {pdf_name}", message=frappe.get_traceback())

# -------------------------------------------------Get Delivered Invoices and Fetch PODs------------------------------------------------------

def get_delivered_pdf_and_fetch_pods_for_pf(start=0):

    BATCH_SIZE = 50

    try:
        # Get current fiscal year dates
        from erpnext.accounts.utils import get_fiscal_year
        fiscal_year, fiscal_start_date, fiscal_end_date = get_fiscal_year(frappe.utils.today())
        
        if not fiscal_year:
            frappe.log_error("POD Processing Error", "No active fiscal year found for current date.")
            return
        
        # Calculate date range: 30 days before fiscal year start to fiscal year end
        filter_start_date = add_days(fiscal_start_date, -31)
        filter_end_date = fiscal_end_date
        
        pickup_dispatch_forms = frappe.db.get_all(
            "Pickup Forms",
            filters={
                "docstatus": 1,
                "is_eshipz_order_created": 1,
                "eshipz_shipment_status": "Delivered",
                "eshipz_tracking_number": ["!=", ""],
                "actual_pickup_date": ["between", [filter_start_date, filter_end_date]]
            },
            fields=["name", "eshipz_tracking_number", "actual_pickup_date"],
            order_by="creation desc",
            limit=BATCH_SIZE,
            start=start
        )

        if not pickup_dispatch_forms:
            frappe.log_error("POD Processing Complete", "No more delivered pickup dispatch forms left in the specified date range.")
            return

        added, exists, failed, skipped = [], [], [], []

        for pdf in pickup_dispatch_forms:
            name = pdf.name
            tracking_no = (pdf.eshipz_tracking_number or "").split(" - ")[0]

            if not tracking_no:
                skipped.append(name)
                continue

            if get_pod_image_for_pickup_dispatch_forms(name):
                exists.append(name)
                continue

            try:
                pod = call_pod_api(tracking_no)
                url = pod.get("data", {}).get("url")

                if url:
                    doc = attach_file_from_url("Pickup Forms", name, url)
                    if doc:
                        added.append(name)
                    else:
                        failed.append(name)
                else:
                    failed.append(name)
            except Exception as e:
                failed.append(name)
                frappe.log_error(f"POD Error - {name}", str(e))

        # Summary log with Forms names instead of counts
        frappe.log_error(
            "POD Summary",
            f"Start={start} | Date Range: {filter_start_date} to {filter_end_date} | Added={added} | Exists={exists} | Failed={failed} | Skipped={skipped}"
        )

        # Enqueue next batch
        if len(pickup_dispatch_forms) == BATCH_SIZE:
            frappe.enqueue(
                "bo_eshipz_integration.bo_eshipz_integration.pickup_scheduler.get_delivered_pdf_and_fetch_pods_for_pf",
                start=start + BATCH_SIZE,
                queue="long",
                timeout=3000
            )

    except Exception as e:
        frappe.log_error("POD Batch Error", frappe.get_traceback())


def get_pod_image_for_pickup_dispatch_forms(pdf_name):
    """Fetch POD file for a Dispatch Forms."""
    try:
        safe_name = "".join(c for c in pdf_name if c.isalnum() or c in ('-', '_')).rstrip()
        return frappe.db.get_value(
            "File",
            {
                "attached_to_doctype": "Pickup Forms",
                "attached_to_name": pdf_name,
                "file_name": ["like", f"%{safe_name}_pod%"]
            },
            ["name", "file_url", "file_name"],
            order_by="creation desc"
        )
    except Exception as e:
        frappe.log_error(f"POD Fetch Error - {pdf_name}", str(e))
        return None
    
def check_pod_status(pdf_name):
    """Quick check for POD status of an Forms."""
    try:
        inv = frappe.db.get_value(
            "Pickup Forms", pdf_name,
            ["eshipz_tracking_number", "eshipz_shipment_status"], as_dict=True)

        if not inv:
            return {"status": "Pickup Forms not found"}

        if inv.eshipz_shipment_status != "Delivered":
            return {"status": "Not Delivered", "shipment_status": inv.eshipz_shipment_status}

        pod = get_pod_image_for_pickup_dispatch_forms(pdf_name)
        return {
            "status": "POD exists" if pod else "POD not found",
            "file_name": pod.file_name if pod else None,
            "file_url": pod.file_url if pod else None
        }

    except Exception as e:
        return {"status": "Error", "error": str(e)}

def attach_file_from_url(doctype, docname, file_url, filename=None):
    """
    Download a file (image/pdf/zip/etc.) from URL and attach it to a Frappe document.
    - ZIP → always {docname}.zip
    - Non-ZIP → {safe_docname}_pod.{ext}
    """
    try:
        # Fetch file data
        response = requests.get(file_url, timeout=60)
        if response.status_code != 200:
            frappe.log_error(f"Failed to download file from {file_url}. Status: {response.status_code}")
            return None

        content = response.content

        # Detect extension from URL
        parsed_url = urlparse(file_url)
        url_filename = os.path.basename(parsed_url.path)
        ext = os.path.splitext(url_filename)[1].lower().lstrip(".")

        # 🔹 Handle ZIP files → always {docname}.zip
        if ext == "zip":
            clean_docname = "".join(c for c in docname if c.isalnum() or c in ('-', '_')).replace(".", "")
            filename = f"{clean_docname}.zip"
        else:
            # 🔹 Handle non-ZIP files → {safe_docname}_pod.{ext}
            safe_docname = "".join(c for c in docname if c.isalnum() or c in ('-', '_')).rstrip()
            filename = filename or f"{safe_docname}_pod.{ext or 'bin'}"

        # Check if already exists
        existing_file = frappe.db.exists("File", {
            "attached_to_doctype": doctype,
            "attached_to_name": docname,
            "file_name": filename
        })
        if existing_file:
            frappe.log_error(f"File {filename} already exists for {doctype} {docname}")
            return frappe.get_doc("File", existing_file)

        # Save file
        file_doc = save_file(
            fname=filename,
            content=content,
            dt=doctype,
            dn=docname,
            folder="Home/Attachments",
            is_private=1 if ext == "zip" else 0
        )
        return file_doc

    except requests.exceptions.RequestException as e:
        frappe.log_error(f"Network error downloading file from {file_url}: {str(e)}")
        return None
    except Exception as e:
        frappe.log_error(f"Error attaching file from {file_url} to {doctype} {docname}: {str(e)}")
        return None