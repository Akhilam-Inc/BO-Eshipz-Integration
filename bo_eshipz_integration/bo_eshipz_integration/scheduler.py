import frappe
import requests
import json
from datetime import datetime
from frappe.utils import add_days
from frappe.utils.file_manager import save_file
from frappe import _

BATCH_SIZE = 50


def get_eshipz_config():
    """
    Retrieve Eshipz Configuration and ensure it is enabled with a valid API token.
    """
    config = frappe.get_single("BO Eshipz Configuration")
    if not config.is_enable or not config.get_password("api_token"):
        frappe.throw("Please Enable Token In Eshipz Configuration Document!")
    return config


def get_api_headers():
    """
    Prepare the API headers required for eshipz API requests.
    """
    config = get_eshipz_config()
    token = config.get_password("api_token")
    return {"Content-Type": "application/json", "X-API-TOKEN": token}


def call_tracking_api_bulk(sales_invoices):
    """
    Bulk Tracking API call using comma-separated q_num (as required by Eshipz).
    sales_invoices: List[str]
    """
    if not sales_invoices:
        return []

    headers = get_api_headers()
    tracking_url = "https://app.eshipz.com/api/v1/trackings"

    # IMPORTANT: comma-separated string
    q_num_value = ",".join(sales_invoices)

    payload = json.dumps({"q_num": q_num_value})

    response = requests.post(tracking_url, headers=headers, data=payload, timeout=30)

    if response.status_code != 200:
        frappe.log_error(title="Eshipz Bulk Tracking API Error", message=response.text)
        return []

    return response.json()


def call_shipment_api_bulk(sales_invoices):
    """
    Bulk Shipment API call.
    sales_invoices: List[str]
    """
    if not sales_invoices:
        return []

    headers = get_api_headers()

    invoice_list = json.dumps(sales_invoices)

    shipment_url = (
        "https://app.eshipz.com/api/v1/get-shipments"
        f'?db_filters={{"customer_referenc": {{"$in": {invoice_list}}}}}'
    )

    response = requests.get(shipment_url, headers=headers, timeout=30)

    if response.status_code != 200:
        frappe.log_error("Eshipz Bulk Shipment API Error", response.text)
        return []

    return response.json()


def call_pod_api(tracking_number):
    """
    Make a POST request to the Proof of Delivery API using the awb Number.
    """
    headers = get_api_headers()
    # Ensure Content-Type is set to application/json
    headers["Content-Type"] = "application/json"

    pod_url = "https://app.eshipz.com/api/v1/getPOD"

    # Send JSON data instead of form data
    payload = {"awb": tracking_number}

    response = requests.post(
        pod_url,
        data=json.dumps(payload),  # Convert to JSON string
        headers=headers,
    )

    return response.json()


def schedule_next_batch(method_path, start):
    """
    Helper function to schedule the next batch job.
    """
    frappe.enqueue(method_path, start=start, queue="long", timeout=3000)


# -------------------------------------------Update Eshipz Shipment Shipping Details -------------------------------------------


@frappe.whitelist()
def schedule_update_shipping_details_for_si(start=0):
    try:
        sales_invoices = frappe.db.get_all(
            "Sales Invoice",
            filters={
                "docstatus": 1,
                "custom_is_eshipz_order_created_bo": 1,
                "custom_bo_eshipz_tracking_number": ["in", [None, ""]],
                "custom_bo_eshipz_shipment_status": [
                    "in",
                    [None, "", "Shipment Not Created"],
                ],
            },
            fields=[
                "name",
                "custom_bo_eshipz_shipment_status",
                "custom_bo_eshipz_tracking_number",
            ],
            order_by="creation desc",
            limit=BATCH_SIZE,
            start=start,
        )

        if not sales_invoices:
            frappe.log_error(
                "Batch Processing Complete",
                "No more Sales Invoice left to process.",
            )
            return

        si_names = [si.name for si in sales_invoices]

        # ✅ SINGLE BULK API CALLS
        shipment_data = call_shipment_api_bulk(si_names)
        tracking_data = call_tracking_api_bulk(si_names)

        shipment_map = {
            s.get("customer_referenc"): s
            for s in shipment_data
            if s.get("customer_referenc")
        }

        tracking_map = {t.get("q_num"): t for t in tracking_data if t.get("q_num")}

        shipment_found = []
        shipment_not_found = []

        for si in sales_invoices:
            si_name = si.name
            existing_status = si.custom_bo_eshipz_shipment_status
            existing_tracking = si.custom_bo_eshipz_tracking_number

            shipment = shipment_map.get(si_name)
            tracking = tracking_map.get(si_name)

            if not shipment or not tracking:
                frappe.db.set_value(
                    "Sales Invoice",
                    si_name,
                    "custom_bo_eshipz_shipment_status",
                    "Shipment Not Created",
                )
                shipment_not_found.append(si_name)
                continue

            vendor_name = shipment.get("vendor_name")
            tracking_status = tracking.get("tag")
            tracking_number = tracking.get("tracking_number")

            if not existing_tracking and existing_status != "Delivered":
                frappe.db.set_value(
                    "Sales Invoice",
                    si_name,
                    {
                        "custom_bo_eshipz_shipment_status": tracking_status,
                        "custom_bo_eshipz_tracking_number": f"{tracking_number} - {vendor_name}",
                    },
                )
                shipment_found.append(si_name)

        frappe.log_error("Shipment Found FOR SI", shipment_found)
        frappe.log_error("Shipment Not Found FOR SI", shipment_not_found)

        # 🔁 Next batch
        if len(sales_invoices) == BATCH_SIZE:
            schedule_next_batch(
                "bo_eshipz_integration.bo_eshipz_integration.scheduler.schedule_update_shipping_details_for_si",
                start + BATCH_SIZE,
            )

    except Exception:
        frappe.log_error(
            "Error in BO Sales Invoice Shipping Details Scheduler",
            frappe.get_traceback(),
        )


# ---------------------------------------------------Update Eshipz Shipment Actual Delivery Date-------------------------------------------------


@frappe.whitelist()
def schedule_update_delivery_date_for_si(start=0):
    try:
        sales_invoices = frappe.db.get_all(
            "Sales Invoice",
            filters={
                "docstatus": 1,
                "custom_custom_is_eshipz_order_created_bo_bo": 1,
                "custom_bo_eshipz_shipment_status": [
                    "not in",
                    ["", "Shipment Not Created"],
                ],
                "custom_bo_eshipz_tracking_number": ["!=", ""],
                "custom_bo_actual_delivery_date": ["is", "null"],
            },
            fields=["name"],
            order_by="creation desc",
            limit=BATCH_SIZE,
            start=start,
        )

        if not sales_invoices:
            frappe.log_error(
                "Batch Processing Complete",
                "No more Sales Invoice left to process.",
            )
            return

        si_names = [si.name for si in sales_invoices]

        # ✅ SINGLE BULK TRACKING API CALL
        tracking_data = call_tracking_api_bulk(si_names)

        tracking_map = {t.get("q_num"): t for t in tracking_data if t.get("q_num")}

        updated_invoices = []

        for si in sales_invoices:
            si_name = si.name
            shipment = tracking_map.get(si_name)

            if not shipment:
                continue

            delivery_date_str = shipment.get("delivery_date")

            # ✅ Delivered
            if delivery_date_str:
                delivery_date = datetime.strptime(
                    delivery_date_str, "%a, %d %b %Y %H:%M:%S %Z"
                ).strftime("%Y-%m-%d")

                frappe.db.set_value(
                    "Sales Invoice",
                    si_name,
                    {
                        "custom_bo_actual_delivery_date": delivery_date,
                        "custom_bo_eshipz_shipment_status": "Delivered",
                    },
                )
                updated_invoices.append(si_name)

        if updated_invoices:
            frappe.log_error(
                "Updated Sales Invoice Delivery Date",
                f"Successfully processed {len(updated_invoices)} invoices",
            )

        # 🔁 Next batch
        if len(sales_invoices) == BATCH_SIZE:
            schedule_next_batch(
                "bo_eshipz_integration.bo_eshipz_integration.scheduler.schedule_update_delivery_date_for_si",
                start + BATCH_SIZE,
            )

    except Exception:
        frappe.log_error(
            "Error Updating Sales Invoice Delivery Date",
            frappe.get_traceback(),
        )


# -------------------------------------------------Update Eshipz Shipment Status------------------------------------------------------


@frappe.whitelist()
def schedule_update_shipping_detail_status_for_si(start=0):
    try:
        sales_invoices = frappe.db.get_all(
            "Sales Invoice",
            filters={
                "docstatus": 1,
                "custom_is_eshipz_order_created_bo": 1,
                "custom_bo_actual_delivery_date": ["is", "null"],
                "custom_bo_eshipz_shipment_status": [
                    "not in",
                    ["Delivered", "Shipment Not Created"],
                ],
            },
            fields=["name", "custom_bo_eshipz_shipment_status"],
            order_by="creation desc",
            limit=BATCH_SIZE,
            start=start,
        )

        if not sales_invoices:
            frappe.log_error(
                "Batch Processing Complete",
                "No more Sales Invoice left to process.",
            )
            return

        si_names = [si.name for si in sales_invoices]

        # ✅ SINGLE BULK TRACKING API CALL
        tracking_data = call_tracking_api_bulk(si_names)

        tracking_map = {t.get("q_num"): t for t in tracking_data if t.get("q_num")}

        updated_invoices = []

        for si in sales_invoices:
            si_name = si.name
            existing_status = si.custom_bo_eshipz_shipment_status

            shipment = tracking_map.get(si_name)
            if not shipment:
                continue

            tag_status = shipment.get("tag")

            if existing_status != "Delivered" and tag_status:
                frappe.db.set_value(
                    "Sales Invoice",
                    si_name,
                    "custom_bo_eshipz_shipment_status",
                    tag_status,
                )
                updated_invoices.append(si_name)

        if updated_invoices:
            frappe.log_error(
                "Updated Sales Invoice For Detail Status",
                f"Successfully updated {len(updated_invoices)} invoices in this batch",
            )

        # 🔁 Next batch
        if len(sales_invoices) == BATCH_SIZE:
            schedule_next_batch(
                "bo_eshipz_integration.bo_eshipz_integration.scheduler.schedule_update_shipping_detail_status_for_si",
                start + BATCH_SIZE,
            )

    except Exception:
        frappe.log_error(
            "Error While Updating Shipment Status For Sales Invoice",
            frappe.get_traceback(),
        )


# -------------------------------------------------Get Delivered Invoices and Fetch PODs------------------------------------------------------


def get_delivered_pdf_and_fetch_pods_for_si(start=0):
    BATCH_SIZE = 50

    try:
        # Get current fiscal year dates
        from erpnext.accounts.utils import get_fiscal_year

        fiscal_year, fiscal_start_date, fiscal_end_date = get_fiscal_year(
            frappe.utils.today()
        )

        if not fiscal_year:
            frappe.log_error(
                "POD Processing Error", "No active fiscal year found for current date."
            )
            return

        # Calculate date range: 30 days before fiscal year start to fiscal year end
        filter_start_date = add_days(fiscal_start_date, -31)
        filter_end_date = fiscal_end_date

        pickup_dispatch_forms = frappe.db.get_all(
            "Sales Invoice",
            filters={
                "docstatus": 1,
                "custom_is_eshipz_order_created_bo": 1,
                "custom_bo_eshipz_shipment_status": "Delivered",
                "custom_bo_eshipz_tracking_number": ["!=", ""],
                "order_date": ["between", [filter_start_date, filter_end_date]],
            },
            fields=["name", "custom_bo_eshipz_tracking_number", "order_date"],
            order_by="creation desc",
            limit=BATCH_SIZE,
            start=start,
        )

        if not pickup_dispatch_forms:
            frappe.log_error(
                "POD Processing Complete",
                "No more delivered pickup dispatch forms left in the specified date range.",
            )
            return

        added, exists, failed, skipped = [], [], [], []

        for pdf in pickup_dispatch_forms:
            name = pdf.name
            tracking_no = (pdf.custom_bo_eshipz_tracking_number or "").split(" - ")[0]

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
                    doc = attach_image_from_url("Sales Invoice", name, url)
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
            "POD Summary For Sales Invoice",
            f"Start={start} | Date Range: {filter_start_date} to {filter_end_date} | Added={added} | Exists={exists} | Failed={failed} | Skipped={skipped}",
        )

        # Enqueue next batch
        if len(pickup_dispatch_forms) == BATCH_SIZE:
            frappe.enqueue(
                "bo_eshipz_integration.bo_eshipz_integration.scheduler.get_delivered_pdf_and_fetch_pods_for_si",
                start=start + BATCH_SIZE,
                queue="long",
                timeout=3000,
            )

    except Exception as e:
        frappe.log_error("POD Batch Error", frappe.get_traceback())


def get_pod_image_for_pickup_dispatch_forms(pdf_name):
    """Fetch POD file for a Dispatch Forms."""
    try:
        safe_name = "".join(
            c for c in pdf_name if c.isalnum() or c in ("-", "_")
        ).rstrip()
        return frappe.db.get_value(
            "File",
            {
                "attached_to_doctype": "Sales Invoice",
                "attached_to_name": pdf_name,
                "file_name": ["like", f"%{safe_name}_pod%"],
            },
            ["name", "file_url", "file_name"],
            order_by="creation desc",
        )
    except Exception as e:
        frappe.log_error(f"POD Fetch Error - {pdf_name}", str(e))
        return None


def check_pod_status(pdf_name):
    """Quick check for POD status of an Forms."""
    try:
        inv = frappe.db.get_value(
            "Sales Invoice",
            pdf_name,
            ["custom_bo_eshipz_tracking_number", "custom_bo_eshipz_shipment_status"],
            as_dict=True,
        )

        if not inv:
            return {"status": "Sales Invoice not found"}

        if inv.custom_bo_eshipz_shipment_status != "Delivered":
            return {
                "status": "Not Delivered",
                "shipment_status": inv.custom_bo_eshipz_shipment_status,
            }

        pod = get_pod_image_for_pickup_dispatch_forms(pdf_name)
        return {
            "status": "POD exists" if pod else "POD not found",
            "file_name": pod.file_name if pod else None,
            "file_url": pod.file_url if pod else None,
        }

    except Exception as e:
        return {"status": "Error", "error": str(e)}


def attach_image_from_url(doctype, docname, image_url, filename=None):
    """
    Download image from URL and attach it to a Frappe document.
    """
    import os
    import tempfile
    from urllib.parse import urlparse

    try:
        # Fetch image data
        response = requests.get(image_url, timeout=30)
        if response.status_code != 200:
            frappe.log_error(
                f"Failed to download image from {image_url}. Status: {response.status_code}"
            )
            return None

        # Extract content
        content = response.content
        content_type = response.headers.get("Content-Type", "")

        # Determine file extension
        if content_type and "/" in content_type:
            ext = content_type.split("/")[-1]
            # Handle common edge cases
            if ext == "jpeg":
                ext = "jpg"
            elif ext not in ["jpg", "jpeg", "png", "gif", "pdf", "webp"]:
                ext = "jpg"  # Default fallback
        else:
            # Try to get extension from URL
            parsed_url = urlparse(image_url)
            url_ext = os.path.splitext(parsed_url.path)[1].lower().lstrip(".")
            ext = (
                url_ext
                if url_ext in ["jpg", "jpeg", "png", "gif", "pdf", "webp"]
                else "jpg"
            )

        # Create a safe filename (remove special characters that cause path issues)
        safe_docname = "".join(
            c for c in docname if c.isalnum() or c in ("-", "_")
        ).rstrip()
        filename = filename or f"{safe_docname}_pod.{ext}"

        # Check if file already exists
        existing_file = frappe.db.exists(
            "File",
            {
                "attached_to_doctype": doctype,
                "attached_to_name": docname,
                "file_name": filename,
            },
        )

        if existing_file:
            frappe.log_error(f"File {filename} already exists for {doctype} {docname}")
            return frappe.get_doc("File", existing_file)

        # Create temporary file first
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}") as temp_file:
            temp_file.write(content)
            temp_file_path = temp_file.name

        try:
            # Save the file using the temporary file path
            file_doc = save_file(
                fname=filename,
                content=content,
                dt=doctype,
                dn=docname,
                folder="Home/Attachments",  # Specify a proper folder
                is_private=0,
            )

            return file_doc

        finally:
            # Clean up temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)

    except requests.exceptions.RequestException as e:
        frappe.log_error(f"Network error downloading image from {image_url}: {str(e)}")
        return None
    except Exception as e:
        frappe.log_error(
            f"Error attaching image from {image_url} to {doctype} {docname}: {str(e)}"
        )
        return None
