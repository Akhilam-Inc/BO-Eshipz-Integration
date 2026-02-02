import frappe
from datetime import datetime
import time
from bo_eshipz_integration.bo_eshipz_integration.scheduler import (
    call_tracking_api_bulk,
    call_shipment_api_bulk,
    attach_file_from_url
)

BATCH_SIZE = 50  # Records per batch
API_CALL_LIMIT = 350  # API calls allowed
RECORDS_PER_API_CALL = 50  # How many records sent in one API call
RATE_LIMIT_BUFFER = 10  # Safety buffer

# ---------------------------------------------------Update Eshipz Shipment Actual Delivery Date-------------------------------------------------


@frappe.whitelist()
def schedule_update_delivery_date_for_pf():
    """
    Updates delivery dates for pickup forms respecting API rate limits.
    Stops at 340 API calls to avoid hitting the 350 limit.
    """
    try:
        total_processed = 0
        total_updated = 0
        api_calls_made = 0
        error_count = 0
        max_api_calls = API_CALL_LIMIT - RATE_LIMIT_BUFFER
        last_processed_id = None
        iteration = 0

        frappe.logger().info(f"Starting delivery date sync at {datetime.now()}")

        while api_calls_made < max_api_calls:
            iteration += 1

            filters = {
                "docstatus": 1,
                "is_eshipz_order_created": 1,
                "eshipz_shipment_status": ["not in", ["", "Shipment Not Created"]],
                "actual_delivery_date": ["in", ["", None]],
            }

            # Cursor-based pagination
            if last_processed_id:
                filters["name"] = [">", last_processed_id]

            pickup_forms = frappe.db.get_all(
                "Pickup Forms",
                filters=filters,
                fields=["name"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not pickup_forms:
                frappe.logger().info("No more records to process")
                break

            names = [d.name for d in pickup_forms]
            last_processed_id = names[-1]
            total_processed += len(names)

            # Make API call with error handling
            try:
                tracking_data = call_tracking_api_bulk(names)
                api_calls_made += 1

                frappe.logger().info(
                    f"API Call {api_calls_made}/{max_api_calls} - "
                    f"Batch {iteration} - {len(names)} records"
                )

                tracking_map = {
                    d.get("order_id"): d
                    for d in tracking_data or []
                    if d.get("order_id")
                }

                # Process each record individually with error handling
                for pdf_name in names:
                    try:
                        shipment = tracking_map.get(pdf_name)
                        if not shipment:
                            continue

                        update_values = {}

                        # Handle actual delivery date
                        if shipment.get("delivery_date"):
                            try:
                                dt = datetime.strptime(
                                    shipment["delivery_date"],
                                    "%a, %d %b %Y %H:%M:%S %Z",
                                )
                                update_values.update(
                                    {
                                        "actual_delivery_date": dt.strftime("%Y-%m-%d"),
                                        "eshipz_shipment_status": "Delivered",
                                    }
                                )
                            except ValueError as date_error:
                                frappe.log_error(
                                    f"Invalid delivery_date format for {pdf_name}",
                                    f"Date: {shipment.get('delivery_date')}\nError: {str(date_error)}",
                                )
                                error_count += 1

                        # Handle expected delivery date
                        elif shipment.get("expected_delivery_date"):
                            try:
                                dt = datetime.strptime(
                                    shipment["expected_delivery_date"],
                                    "%a, %d %b %Y %H:%M:%S %Z",
                                )
                                update_values["expected_delivery_date"] = dt.strftime(
                                    "%Y-%m-%d"
                                )
                            except ValueError as date_error:
                                frappe.log_error(
                                    f"Invalid expected_delivery_date format for {pdf_name}",
                                    f"Date: {shipment.get('expected_delivery_date')}\nError: {str(date_error)}",
                                )
                                error_count += 1

                        # Update database if we have values
                        if update_values:
                            frappe.db.set_value(
                                "Pickup Forms",
                                pdf_name,
                                update_values,
                                update_modified=False,
                            )
                            total_updated += 1

                    except Exception as record_error:
                        error_count += 1
                        frappe.log_error(
                            f"Error processing record {pdf_name}",
                            f"Shipment data: {shipment}\nError: {str(record_error)}",
                        )
                        continue

                # Commit after each batch
                frappe.db.commit()

                # Small delay to avoid rate limiting
                time.sleep(0.1)

            except Exception as api_error:
                api_calls_made += 1  # Count failed calls too
                error_count += 1
                frappe.log_error(
                    f"API Error - Batch {iteration}",
                    f"Batch starting at {names[0]}\nError: {str(api_error)}",
                )
                continue  # Skip failed batch, continue with next

            # Check if approaching API limit
            if api_calls_made >= max_api_calls:
                frappe.logger().warning(
                    f"Reached API call limit ({api_calls_made}/{max_api_calls}). "
                    f"Stopping gracefully. Last processed: {last_processed_id}"
                )
                break

        # Final summary
        has_more_records = api_calls_made >= max_api_calls
        status = "PAUSED - More records pending" if has_more_records else "COMPLETED"

        summary = f"""
            Delivery Date Sync Summary:
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Status: {status}
            Total Records Processed: {total_processed}
            Total Records Updated: {total_updated}
            API Calls Made: {api_calls_made}/{API_CALL_LIMIT}
            Batches Processed: {iteration}
            Errors Encountered: {error_count}
            Last Processed ID: {last_processed_id or "N/A"}
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """

        if has_more_records:
            frappe.log_error(
                "Delivery Date Sync Paused - API Limit Reached",
                summary
                + f"\n\nℹ️ Run the job again to continue from: {last_processed_id}",
            )
        elif error_count > 0:
            frappe.log_error("Delivery Date Sync Completed with Errors", summary)
        else:
            frappe.log_error("Delivery Date Sync Completed Successfully", summary)

        return {
            "status": "success",
            "processed": total_processed,
            "updated": total_updated,
            "api_calls": api_calls_made,
            "errors": error_count,
            "has_more": has_more_records,
            "last_id": last_processed_id,
        }

    except Exception:
        frappe.log_error(
            "Delivery Date Sync Critical Failure",
            f"Processed: {total_processed}\n"
            f"Updated: {total_updated}\n"
            f"API Calls: {api_calls_made}\n"
            f"Errors: {error_count}\n\n"
            f"{frappe.get_traceback()}",
        )
        raise


# -------------------------------------------------Update Eshipz Shipment Status------------------------------------------------------


@frappe.whitelist()
def schedule_update_shipping_detail_status_for_pf():
    """
    Processes pickup forms respecting API rate limits.
    Stops at 340 API calls (350 - 10 buffer) to avoid hitting limit.
    """
    try:
        total_updated = 0
        total_processed = 0
        api_calls_made = 0
        max_api_calls = API_CALL_LIMIT - RATE_LIMIT_BUFFER  # 340 calls
        last_processed_id = None
        iteration = 0

        frappe.logger().info(f"Starting shipment sync at {datetime.now()}")

        while api_calls_made < max_api_calls:
            iteration += 1

            filters = {
                "docstatus": 1,
                "is_eshipz_order_created": 1,
                "actual_delivery_date": ["in", ["", None]],
                "eshipz_shipment_status": [
                    "not in",
                    ["Delivered", "Shipment Not Created", "Cancelled"],
                ],
            }

            if last_processed_id:
                filters["name"] = [">", last_processed_id]

            pickup_forms = frappe.db.get_all(
                "Pickup Forms",
                filters=filters,
                fields=["name"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not pickup_forms:
                frappe.logger().info("No more records to process")
                break

            names = [d.name for d in pickup_forms]
            last_processed_id = names[-1]
            total_processed += len(names)

            # Make API call
            try:
                tracking_data = call_tracking_api_bulk(names)
                api_calls_made += 1  # Increment API call counter

                frappe.logger().info(
                    f"API Call {api_calls_made}/{max_api_calls} - Batch {iteration} - {len(names)} records"
                )

                tracking_map = {
                    d.get("order_id"): d for d in tracking_data or [] if d.get("order_id")
                }

                # Process each record
                for pdf_name in names:
                    try:
                        shipment = tracking_map.get(pdf_name)
                        if not shipment:
                            continue

                        new_status = None
                        if shipment.get("shipment_status") == "cancelled":
                            new_status = "Cancelled"
                        elif shipment.get("tag"):
                            new_status = shipment.get("tag")

                        if new_status:
                            frappe.db.set_value(
                                "Pickup Forms",
                                pdf_name,
                                "eshipz_shipment_status",
                                new_status,
                                update_modified=False,
                            )
                            total_updated += 1

                    except Exception as e:
                        frappe.log_error(f"Error processing record {pdf_name}", str(e))
                        continue

                frappe.db.commit()

                # Small delay to avoid rate limiting
                time.sleep(0.1)

            except Exception as e:
                api_calls_made += 1  # Count failed calls too
                frappe.log_error(
                    f"API Error - Batch {iteration}",
                    f"Batch starting at {names[0]}\nError: {str(e)}",
                )
                continue

            # Check if approaching limit
            if api_calls_made >= max_api_calls:
                frappe.logger().warning(
                    f"Reached API call limit ({api_calls_made}/{max_api_calls}). "
                    f"Stopping gracefully. Last processed: {last_processed_id}"
                )
                break

        # Final summary
        summary = f"""
            Shipment Status Sync Summary:
            - Total Records Processed: {total_processed}
            - Total Records Updated: {total_updated}
            - API Calls Made: {api_calls_made}/{API_CALL_LIMIT}
            - Batches Processed: {iteration}
            - Last Processed ID: {last_processed_id or "N/A"}
            - Status: {"COMPLETED" if not pickup_forms else "PAUSED - More records pending"}
        """

        if api_calls_made >= max_api_calls:
            frappe.log_error(
                "Shipment Sync Paused - API Limit Reached",
                summary
                + f"\n\nRun the job again to continue from: {last_processed_id}",
            )
        else:
            frappe.log_error("Shipment Sync Completed", summary)

        return {
            "status": "success",
            "processed": total_processed,
            "updated": total_updated,
            "api_calls": api_calls_made,
            "has_more": api_calls_made >= max_api_calls,
        }

    except Exception:
        frappe.log_error(
            "Shipment Sync Critical Failure",
            f"Processed: {total_processed}\nAPI Calls: {api_calls_made}\n\n{frappe.get_traceback()}",
        )
        raise


# -------------------------------------------------Get Delivered Invoices and Fetch PODs------------------------------------------------------


@frappe.whitelist()
def get_delivered_pdf_and_fetch_pods_for_pf():
    """
    Fetch PODs for delivered Pickup Forms using BULK shipment API.
    Cursor-based batching + API limit safe.
    """

    try:
        total_processed = 0
        added, exists, failed, skipped = [], [], [], []

        api_calls_made = 0
        error_count = 0
        last_processed_id = None
        iteration = 0

        frappe.logger().info(
            f"Pickup Forms POD Sync started at {datetime.now()}"
        )

        def safe_docname(name):
            return "".join(c for c in name if c.isalnum() or c in ("-", "_")).rstrip()

        while api_calls_made < API_CALL_LIMIT:
            iteration += 1

            # ---- Pickup Forms Filters ----
            filters = {
                "docstatus": 1,
                "is_eshipz_order_created": 1,
                "eshipz_shipment_status": "Delivered",
                "eshipz_tracking_number": ["!=", ""],
            }

            if last_processed_id:
                filters["name"] = [">", last_processed_id]

            pickup_forms = frappe.db.get_all(
                "Pickup Forms",
                filters=filters,
                fields=["name"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not pickup_forms:
                frappe.logger().info("No more delivered Pickup Forms to process")
                break

            form_names = [f.name for f in pickup_forms]
            last_processed_id = form_names[-1]

            # ---- Existing POD check (bulk) ----
            existing_files = frappe.db.get_all(
                "File",
                filters={
                    "attached_to_doctype": "Pickup Forms",
                    "attached_to_name": ["in", form_names],
                    "file_name": ["like", "%pod%"],
                },
                fields=["attached_to_name"],
            )
            existing_pod_map = {f.attached_to_name for f in existing_files}

            frappe.logger().info(
                f"PF Batch {iteration} | Records {len(form_names)} | "
                f"API Calls {api_calls_made}/{API_CALL_LIMIT}"
            )

            # ---- BULK Shipment API Call ----
            shipment_data = call_shipment_api_bulk(form_names)
            api_calls_made += 1

            if not shipment_data:
                failed.extend(form_names)
                total_processed += len(form_names)
                continue

            # ---- Build POD Map ----
            pod_map = {
                s.get("customer_referenc"): s.get("pod_link")
                for s in shipment_data
                if s.get("customer_referenc") and s.get("pod_link")
            }

            # ---- Attach PODs ----
            for name in form_names:

                if name in existing_pod_map:
                    exists.append(name)
                    total_processed += 1
                    continue

                pod_url = pod_map.get(name)
                if not pod_url:
                    failed.append(name)
                    total_processed += 1
                    continue

                try:
                    file_doc = attach_file_from_url(
                        "Pickup Forms",
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
                        f"Pickup Forms POD Attach Error - {name}",
                        frappe.get_traceback(),
                    )

                total_processed += 1

            frappe.db.commit()
            time.sleep(0.1)

        # ---- Final Summary ----
        has_more_records = api_calls_made >= API_CALL_LIMIT
        status = "PAUSED - More records pending" if has_more_records else "COMPLETED"

        summary = f"""
        Pickup Forms POD Fetch Summary:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        Status: {status}
        Total Processed: {total_processed}
        Added: {len(added)}
        Exists: {len(exists)}
        Failed: {len(failed)}
        Skipped: {len(skipped)}
        API Calls Made: {api_calls_made}/{API_CALL_LIMIT}
        Errors: {error_count}
        Last Processed ID: {last_processed_id or "N/A"}
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """

        if has_more_records:
            frappe.log_error("Pickup Forms POD Sync Paused", summary)
        elif error_count > 0:
            frappe.log_error("Pickup Forms POD Completed with Errors", summary)
        else:
            frappe.log_error("Pickup Forms POD Completed Successfully", summary)

        return {
            "status": "success",
            "processed": total_processed,
            "added": len(added),
            "exists": len(exists),
            "failed": len(failed),
            "api_calls": api_calls_made,
            "errors": error_count,
            "has_more": has_more_records,
        }

    except Exception:
        frappe.log_error("Pickup Forms POD Batch Error", frappe.get_traceback())
        raise