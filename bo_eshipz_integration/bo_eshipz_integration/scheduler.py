import frappe
import requests
import json
from datetime import datetime
import time
from frappe.utils.file_manager import save_file
from frappe.utils import add_days, today
import os
from urllib.parse import urlparse

# ============================================================================
# CONFIGURATION CONSTANTS
# ============================================================================

BATCH_SIZE = 50
API_CALL_LIMIT = 350  # API calls allowed
RECORDS_PER_API_CALL = 50  # How many records sent in one API call
RATE_LIMIT_BUFFER = 10  # Safety buffers


# ============================================================================
# CONFIGURATION & AUTHENTICATION
# ============================================================================

def get_eshipz_config():
    """
    Retrieve Eshipz Configuration and ensure it is enabled with a valid API token.
    """
    config = frappe.get_single("BO Eshipz Configuration")
    if not config.is_enable or not config.get_password("api_token"):
        frappe.throw("Please Enable Token In BO Eshipz Configuration Document!")
    return config


def get_api_headers():
    """
    Prepare the API headers required for eshipz API requests.
    """
    config = get_eshipz_config()
    token = config.get_password("api_token")
    return {"Content-Type": "application/json", "X-API-TOKEN": token}


# ============================================================================
# API CALL FUNCTIONS
# ============================================================================

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


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def map_tracking_by_reference(tracking_response):
    """
    Map tracking data using customer reference.
    """
    result = {}

    for row in tracking_response:
        reference = row.get("order_id") or row.get("customer_reference")

        if reference:
            result[reference] = row

    return result


def update_delivery_dates_from_tracking(sales_invoice, shipment):
    """
    Update delivery dates on Sales Invoice from tracking payload.
    Returns True if updated, else False.
    """
    try:
        update_values = {}

        delivery_date_str = shipment.get("delivery_date")

        if delivery_date_str:
            delivery_date = datetime.strptime(
                delivery_date_str, "%a, %d %b %Y %H:%M:%S %Z"
            )
            update_values.update(
                {
                    "custom_bo_actual_delivery_date": delivery_date.strftime("%Y-%m-%d"),
                    "custom_bo_eshipz_shipment_status": "Delivered",
                }
            )

        if update_values:
            frappe.db.set_value(
                "Sales Invoice",
                sales_invoice,
                update_values,
                update_modified=False,
            )
            return True

        return False

    except Exception:
        frappe.log_error(
            title=f"Delivery Date Update Failed: {sales_invoice}",
            message=frappe.get_traceback(),
        )
        return False


def update_shipping_status_from_tracking(sales_invoice, shipment):
    """
    Update shipment status on Sales Invoice.
    Returns True if updated, else False.
    """
    try:
        tag_status = shipment.get("tag")

        existing_status = frappe.db.get_value(
            "Sales Invoice",
            sales_invoice,
            "custom_bo_eshipz_shipment_status",
        )

        # ✅ Normal update
        if existing_status != "Delivered" and tag_status:
            frappe.db.set_value(
                "Sales Invoice",
                sales_invoice,
                "custom_bo_eshipz_shipment_status",
                tag_status,
                update_modified=False,
            )
            return True

        return False

    except Exception:
        frappe.log_error(
            f"Shipment Status Update Failed: {sales_invoice}",
            frappe.get_traceback(),
        )
        return False


def attach_file_from_url(doctype, docname, file_url, safe_name=None):
    """
    Download a file (image/pdf/zip/etc.) from URL and attach it to a Frappe document.
    - ZIP → always {docname}.zip
    - Non-ZIP → {safe_docname}_pod.{ext}
    """
    try:
        response = requests.get(file_url, timeout=60)
        if response.status_code != 200:
            frappe.log_error(
                f"Failed to download file from {file_url}. Status: {response.status_code}"
            )
            return None

        content = response.content
        parsed_url = urlparse(file_url)
        url_filename = os.path.basename(parsed_url.path)
        ext = os.path.splitext(url_filename)[1].lower().lstrip(".")

        # 🔹 Handle ZIP files → always {docname}.zip
        if ext == "zip":
            clean_docname = "".join(
                c for c in docname if c.isalnum() or c in ("-", "_")
            ).replace(".", "")
            filename = f"{clean_docname}.zip"
            is_private = 1
        else:
            # 🔹 Handle non-ZIP files → {safe_docname}_pod.{ext}
            safe_docname = (
                safe_name
                or "".join(c for c in docname if c.isalnum() or c in ("-", "_")).rstrip()
            )
            filename = f"{safe_docname}_pod.{ext or 'bin'}"
            is_private = 0

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
            return frappe.get_doc("File", existing_file)

        # Save file
        file_doc = save_file(
            fname=filename,
            content=content,
            dt=doctype,
            dn=docname,
            folder="Home/Attachments",
            is_private=is_private,
        )
        return file_doc

    except requests.exceptions.RequestException as e:
        frappe.log_error(f"Network error downloading file from {file_url}: {str(e)}")
        return None
    except Exception as e:
        frappe.log_error(
            f"Error attaching file from {file_url} to {doctype} {docname}: {str(e)}"
        )
        return None


# ============================================================================
# MAIN SCHEDULER FUNCTIONS
# ============================================================================

# -------------------------------------------Update Eshipz Shipment Shipping Details -------------------------------------------


@frappe.whitelist()
def schedule_update_shipping_details_for_si():
    """
    Updates shipping details for Sales Invoices respecting API rate limits.
    Makes 2 API calls per batch (shipment + tracking).
    """

    try:
        total_processed = 0
        shipment_found = []
        shipment_not_found = []
        api_calls_made = 0
        error_count = 0
        last_processed_id = None
        iteration = 0

        frappe.logger().info(
            f"Starting Sales Invoice shipping details sync at {datetime.now()}"
        )

        # Reserve space for 2 API calls per batch
        while api_calls_made < API_CALL_LIMIT - 1:
            iteration += 1

            filters = {
                "docstatus": 1,
                "custom_is_eshipz_order_created_bo": 1,
                "is_return": 0,
                "custom_bo_eshipz_tracking_number": ["in", [None, ""]],
                "custom_bo_eshipz_shipment_status": [
                    "in",
                    [None, "", "Shipment Not Created"],
                ],
            }

            if last_processed_id:
                filters["name"] = [">", last_processed_id]

            sales_invoices = frappe.db.get_all(
                "Sales Invoice",
                filters=filters,
                fields=["name"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not sales_invoices:
                frappe.logger().info("No more Sales Invoices to process")
                break

            invoice_names = [si.name for si in sales_invoices]
            last_processed_id = invoice_names[-1]

            try:
                # --- API Call 1: Shipment ---
                shipment_data = call_shipment_api_bulk(invoice_names)
                api_calls_made += 1

                # --- API Call 2: Tracking ---
                tracking_data = call_tracking_api_bulk(invoice_names)
                api_calls_made += 1

                frappe.logger().info(
                    f"API Calls {api_calls_made}/{API_CALL_LIMIT} | "
                    f"Batch {iteration} | Records {len(invoice_names)}"
                )

                shipment_map = {
                    d.get("customer_referenc"): d
                    for d in shipment_data or []
                    if d.get("customer_referenc")
                }

                tracking_map = {
                    d.get("order_id"): d
                    for d in tracking_data or []
                    if d.get("order_id")
                }

                for invoice_name in invoice_names:
                    try:
                        shipment = shipment_map.get(invoice_name)
                        tracking = tracking_map.get(invoice_name)

                        if shipment and tracking:
                            vendor_name = shipment.get("vendor_name")
                            tracking_status = tracking.get("tag")
                            tracking_number = tracking.get("tracking_number")

                            combined_value = f"{tracking_number} - {vendor_name}"

                            existing_status, existing_tracking = frappe.db.get_value(
                                "Sales Invoice",
                                invoice_name,
                                [
                                    "custom_bo_eshipz_shipment_status",
                                    "custom_bo_eshipz_tracking_number",
                                ],
                            )

                            if not existing_tracking and existing_status != "Delivered":
                                frappe.db.set_value(
                                    "Sales Invoice",
                                    invoice_name,
                                    {
                                        "custom_bo_eshipz_shipment_status": tracking_status,
                                        "custom_bo_eshipz_tracking_number": combined_value,
                                    },
                                    update_modified=False,
                                )

                            shipment_found.append(invoice_name)

                        else:
                            frappe.db.set_value(
                                "Sales Invoice",
                                invoice_name,
                                "custom_bo_eshipz_shipment_status",
                                "Shipment Not Created",
                                update_modified=False,
                            )
                            shipment_not_found.append(invoice_name)

                        total_processed += 1

                    except Exception:
                        error_count += 1
                        frappe.log_error(
                            f"Error processing Sales Invoice {invoice_name}",
                            frappe.get_traceback(),
                        )
                        continue

                frappe.db.commit()
                time.sleep(0.1)

            except Exception as api_error:
                api_calls_made += 2
                error_count += 1
                frappe.log_error(
                    f"API Error - Sales Invoice Batch {iteration}",
                    f"Batch starting at {invoice_names[0]}\nError: {str(api_error)}",
                )
                continue

            if api_calls_made >= API_CALL_LIMIT - 1:
                frappe.logger().warning(
                    f"Reached API call limit ({api_calls_made}/{API_CALL_LIMIT}). "
                    f"Stopping gracefully."
                )
                break

        # ---- Final Summary ----
        has_more_records = api_calls_made >= API_CALL_LIMIT - 1
        status = "PAUSED - More records pending" if has_more_records else "COMPLETED"

        summary = f"""
            Sales Invoice Shipping Sync Summary:
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Status: {status}
            Total Processed: {total_processed}
            Shipment Found: {len(shipment_found)}
            Shipment Not Found: {len(shipment_not_found)}
            API Calls Made: {api_calls_made}/{API_CALL_LIMIT}
            Errors: {error_count}
            Last Processed ID: {last_processed_id or "N/A"}
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """

        if has_more_records:
            frappe.log_error("SI Shipping Sync Paused - API Limit Reached", summary)
        elif error_count > 0:
            frappe.log_error("SI Shipping Sync Completed with Errors", summary)
        else:
            frappe.log_error("SI Shipping Sync Completed Successfully", summary)

        return {
            "status": "success",
            "processed": total_processed,
            "found": len(shipment_found),
            "not_found": len(shipment_not_found),
            "api_calls": api_calls_made,
            "errors": error_count,
            "has_more": has_more_records,
        }

    except Exception:
        frappe.log_error(
            "Error in Sales Invoice Shipping Batch Processing",
            frappe.get_traceback(),
        )
        raise


# ---------------------------------------------------Update Eshipz Shipment Actual Delivery Date-------------------------------------------------


@frappe.whitelist()
def schedule_update_delivery_date_for_si():
    """
    Updates actual / expected delivery dates for Sales Invoices
    respecting API rate limits.
    Uses cursor-based batching to avoid skipping records.
    """

    try:
        total_processed = 0
        updated_invoices = []
        skipped_invoices = []
        api_calls_made = 0
        error_count = 0
        last_processed_id = None
        iteration = 0

        frappe.logger().info(f"Starting SI delivery date sync at {datetime.now()}")

        while api_calls_made < API_CALL_LIMIT:
            iteration += 1

            filters = {
                "docstatus": 1,
                "custom_custom_is_eshipz_order_created_bo_bo": 1,
                "is_return": 0,
                "custom_bo_eshipz_tracking_number": ["!=", ""],
                "custom_bo_actual_delivery_date": ["in", ["", None]],
                "custom_bo_eshipz_shipment_status": [
                    "not in",
                    ["", "Shipment Not Created"],
                ],
            }

            if last_processed_id:
                filters["name"] = [">", last_processed_id]

            sales_invoices = frappe.db.get_all(
                "Sales Invoice",
                filters=filters,
                fields=["name"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not sales_invoices:
                frappe.logger().info("No more Sales Invoices to process")
                break

            invoice_names = [row.name for row in sales_invoices]
            last_processed_id = invoice_names[-1]

            try:
                # ---- API Call (Tracking only) ----
                tracking_response = call_tracking_api_bulk(invoice_names)
                api_calls_made += 1

                frappe.logger().info(
                    f"API Calls {api_calls_made}/{API_CALL_LIMIT} | "
                    f"Batch {iteration} | Records {len(invoice_names)}"
                )

                if not tracking_response:
                    skipped_invoices.extend(invoice_names)
                    total_processed += len(invoice_names)
                    continue

                tracking_map = map_tracking_by_reference(tracking_response)

                for invoice_name in invoice_names:
                    try:
                        shipment = tracking_map.get(invoice_name)

                        if shipment:
                            updated = update_delivery_dates_from_tracking(
                                invoice_name, shipment
                            )
                            if updated:
                                updated_invoices.append(invoice_name)
                            else:
                                skipped_invoices.append(invoice_name)
                        else:
                            skipped_invoices.append(invoice_name)

                        total_processed += 1

                    except Exception:
                        error_count += 1
                        frappe.log_error(
                            f"Delivery Date Record Error: {invoice_name}",
                            frappe.get_traceback(),
                        )
                        continue

                frappe.db.commit()
                time.sleep(0.1)

            except Exception as api_error:
                api_calls_made += 1
                error_count += 1
                frappe.log_error(
                    f"Delivery Date API Error - Batch {iteration}",
                    f"Batch starting at {invoice_names[0]}\n{str(api_error)}",
                )
                continue

            if api_calls_made >= API_CALL_LIMIT:
                frappe.logger().warning(
                    f"Reached API call limit ({api_calls_made}/{API_CALL_LIMIT}). "
                    f"Stopping gracefully."
                )
                break

        # ---- Final Summary ----
        has_more_records = api_calls_made >= API_CALL_LIMIT
        status = "PAUSED - More records pending" if has_more_records else "COMPLETED"

        summary = f"""
            Sales Invoice Delivery Date Sync Summary:
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Status: {status}
            Total Processed: {total_processed}
            Updated: {len(updated_invoices)}
            Skipped: {len(skipped_invoices)}
            API Calls Made: {api_calls_made}/{API_CALL_LIMIT}
            Errors: {error_count}
            Last Processed ID: {last_processed_id or "N/A"}
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """

        if has_more_records:
            frappe.log_error("SI Delivery Date Sync Paused", summary)
        elif error_count > 0:
            frappe.log_error("SI Delivery Date Sync Completed with Errors", summary)
        else:
            frappe.log_error("SI Delivery Date Sync Completed Successfully", summary)

        return {
            "status": "success",
            "processed": total_processed,
            "updated": len(updated_invoices),
            "skipped": len(skipped_invoices),
            "api_calls": api_calls_made,
            "errors": error_count,
            "has_more": has_more_records,
        }

    except Exception:
        frappe.log_error(
            "Error in Sales Invoice Delivery Date Batch Processing",
            frappe.get_traceback(),
        )
        raise


# -------------------------------------------------Update Eshipz Shipment Status------------------------------------------------------


@frappe.whitelist()
def schedule_update_shipping_detail_status_for_si():
    """
    Updates shipping status for Sales Invoices respecting API rate limits.
    Uses cursor-based batching to avoid skipping records.
    """

    try:
        total_processed = 0
        updated_invoices = []
        skipped_invoices = []
        api_calls_made = 0
        error_count = 0
        last_processed_id = None
        iteration = 0

        frappe.logger().info(f"Starting SI shipping status sync at {datetime.now()}")

        while api_calls_made < API_CALL_LIMIT:
            iteration += 1

            filters = {
                "docstatus": 1,
                "is_return": 0,
                "custom_is_eshipz_order_created_bo": 1,
                "custom_bo_actual_delivery_date": ["in", ["", None]],
                "custom_bo_eshipz_shipment_status": [
                    "not in",
                    ["Delivered", "Shipment Not Created"],
                ],
            }

            if last_processed_id:
                filters["name"] = [">", last_processed_id]

            sales_invoices = frappe.db.get_all(
                "Sales Invoice",
                filters=filters,
                fields=["name"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not sales_invoices:
                frappe.logger().info("No more Sales Invoices to process")
                break

            invoice_names = [row.name for row in sales_invoices]
            last_processed_id = invoice_names[-1]

            try:
                # ---- API Call (Tracking only) ----
                tracking_response = call_tracking_api_bulk(invoice_names)
                api_calls_made += 1

                frappe.logger().info(
                    f"API Calls {api_calls_made}/{API_CALL_LIMIT} | "
                    f"Batch {iteration} | Records {len(invoice_names)}"
                )

                if not tracking_response:
                    skipped_invoices.extend(invoice_names)
                    total_processed += len(invoice_names)
                    continue

                tracking_map = map_tracking_by_reference(tracking_response)

                for invoice_name in invoice_names:
                    try:
                        shipment = tracking_map.get(invoice_name)

                        if shipment:
                            updated = update_shipping_status_from_tracking(
                                invoice_name, shipment
                            )
                            if updated:
                                updated_invoices.append(invoice_name)
                            else:
                                skipped_invoices.append(invoice_name)
                        else:
                            skipped_invoices.append(invoice_name)

                        total_processed += 1

                    except Exception:
                        error_count += 1
                        frappe.log_error(
                            f"Shipping Status Record Error: {invoice_name}",
                            frappe.get_traceback(),
                        )
                        continue

                frappe.db.commit()
                time.sleep(0.1)

            except Exception as api_error:
                api_calls_made += 1
                error_count += 1
                frappe.log_error(
                    f"Shipping Status API Error - Batch {iteration}",
                    f"Batch starting at {invoice_names[0]}\n{str(api_error)}",
                )
                continue

            if api_calls_made >= API_CALL_LIMIT:
                frappe.logger().warning(
                    f"Reached API call limit ({api_calls_made}/{API_CALL_LIMIT}). "
                    f"Stopping gracefully."
                )
                break

        # ---- Final Summary ----
        has_more_records = api_calls_made >= API_CALL_LIMIT
        status = "PAUSED - More records pending" if has_more_records else "COMPLETED"

        summary = f"""
            Sales Invoice Shipping Status Sync Summary:
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Status: {status}
            Total Processed: {total_processed}
            Updated: {len(updated_invoices)}
            Skipped: {len(skipped_invoices)}
            API Calls Made: {api_calls_made}/{API_CALL_LIMIT}
            Errors: {error_count}
            Last Processed ID: {last_processed_id or "N/A"}
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """

        if has_more_records:
            frappe.log_error("SI Shipping Status Sync Paused", summary)
        elif error_count > 0:
            frappe.log_error("SI Shipping Status Sync Completed with Errors", summary)
        else:
            frappe.log_error("SI Shipping Status Sync Completed Successfully", summary)

        return {
            "status": "success",
            "processed": total_processed,
            "updated": len(updated_invoices),
            "skipped": len(skipped_invoices),
            "api_calls": api_calls_made,
            "errors": error_count,
            "has_more": has_more_records,
        }

    except Exception:
        frappe.log_error(
            "Error in Sales Invoice Shipping Status Batch Processing",
            frappe.get_traceback(),
        )
        raise


# -------------------------------------------------Get Delivered Invoices and Fetch PODs------------------------------------------------------

@frappe.whitelist()
def get_delivered_invoices_and_fetch_pods():
    """
    Fetch PODs for delivered Sales Invoices using BULK shipment API.
    Cursor-based batching + API limit safe.
    """

    try:
        from erpnext.accounts.utils import get_fiscal_year

        total_processed = 0
        added, failed, skipped = [], [], []
        api_calls_made = 0
        error_count = 0
        last_processed_id = None
        iteration = 0

        # ---- Fiscal Year Range ----
        fiscal_year, fiscal_start_date, fiscal_end_date = get_fiscal_year(today())
        if not fiscal_year:
            frappe.log_error("POD Processing Error", "No active fiscal year found.")
            return

        filter_start_date = add_days(fiscal_start_date, -31)
        filter_end_date = fiscal_end_date

        frappe.logger().info(
            f"POD Sync started at {datetime.now()} | "
            f"Range: {filter_start_date} → {filter_end_date}"
        )

        def safe_docname(name):
            return "".join(c for c in name if c.isalnum() or c in ("-", "_")).rstrip()

        while api_calls_made < API_CALL_LIMIT:
            iteration += 1

            # ---- Invoice Filters ----
            filters = {
                "docstatus": 1,
                "custom_is_eshipz_order_created_bo": 1,
                "is_return": 0,
                "custom_bo_eshipz_shipment_status": "Delivered",
                "custom_bo_eshipz_tracking_number": ["!=", ""],
                "posting_date": ["between", [filter_start_date, filter_end_date]],
            }

            if last_processed_id:
                filters["name"] = [">", last_processed_id]

            invoices = frappe.db.get_all(
                "Sales Invoice",
                filters=filters,
                fields=["name"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not invoices:
                frappe.logger().info("No more delivered invoices to process")
                break

            invoice_names = [inv.name for inv in invoices]
            last_processed_id = invoice_names[-1]

            # ---- Existing POD check (bulk) ----
            existing_files = frappe.db.get_all(
                "File",
                filters={
                    "attached_to_doctype": "Sales Invoice",
                    "attached_to_name": ["in", invoice_names],
                    "file_name": ["like", "%pod%"],
                },
                fields=["attached_to_name"],
            )
            existing_pod_map = {f.attached_to_name for f in existing_files}

            frappe.logger().info(
                f"Batch {iteration} | Records {len(invoice_names)} | "
                f"API Calls {api_calls_made}/{API_CALL_LIMIT}"
            )

            # ---- BULK Shipment API Call ----
            shipment_data = call_shipment_api_bulk(invoice_names)
            api_calls_made += 1

            if not shipment_data:
                failed.extend(invoice_names)
                total_processed += len(invoice_names)
                continue

            # ---- Build POD Map: Invoice → POD URL ----
            pod_map = {
                s.get("customer_referenc"): s.get("pod_link")
                for s in shipment_data
                if s.get("customer_referenc") and s.get("pod_link")
            }

            # ---- Attach PODs ----
            for name in invoice_names:

                if name in existing_pod_map:
                    skipped.append(name)
                    total_processed += 1
                    continue

                pod_url = pod_map.get(name)
                if not pod_url:
                    failed.append(name)
                    total_processed += 1
                    continue

                try:
                    file_doc = attach_file_from_url(
                        "Sales Invoice",
                        name,
                        pod_url,
                        safe_name=safe_docname(name),
                    )

                    if file_doc:
                        added.append(name)
                    else:
                        failed.append(name)

                except Exception:
                    failed.append(name)
                    error_count += 1
                    frappe.log_error(
                        f"POD Attach Error - {name}",
                        frappe.get_traceback(),
                    )

                total_processed += 1

            frappe.db.commit()
            time.sleep(0.1)

        # ---- Final Summary ----
        has_more_records = api_calls_made >= API_CALL_LIMIT
        status = "PAUSED - More records pending" if has_more_records else "COMPLETED"

        summary = f"""
        POD Fetch Summary:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        Status: {status}
        Date Range: {filter_start_date} → {filter_end_date}
        Total Processed: {total_processed}
        Added: {len(added)}
        Failed: {len(failed)}
        Skipped: {len(skipped)}
        API Calls Made: {api_calls_made}/{API_CALL_LIMIT}
        Errors: {error_count}
        Last Processed ID: {last_processed_id or "N/A"}
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """

        if has_more_records:
            frappe.log_error("POD Sync Paused - API Limit Reached", summary)
        elif error_count > 0:
            frappe.log_error("POD Sync Completed with Errors", summary)
        else:
            frappe.log_error("POD Sync Completed Successfully", summary)

        return {
            "status": "success",
            "processed": total_processed,
            "added": len(added),
            "failed": len(failed),
            "skipped": len(skipped),
            "api_calls": api_calls_made,
            "errors": error_count,
            "has_more": has_more_records,
        }

    except Exception:
        frappe.log_error("POD Batch Error", frappe.get_traceback())
        raise
