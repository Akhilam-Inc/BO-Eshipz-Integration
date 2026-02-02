import frappe
from datetime import datetime
import time
from bo_eshipz_integration.bo_eshipz_integration.scheduler import (
    call_shipment_api_bulk,
    call_tracking_api_bulk,
    attach_file_from_url
)

BATCH_SIZE = 50
API_CALL_LIMIT = 350
RATE_LIMIT_BUFFER = 10
MAX_API_CALLS = API_CALL_LIMIT - RATE_LIMIT_BUFFER  # 340

# -------------------------------------------Update Eshipz Shipment Shipping Details -------------------------------------------


@frappe.whitelist()
def schedule_update_shipping_details_for_dtf():
    """
    Updates shipping details for DTF respecting API rate limits.
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

        frappe.logger().info(f"Starting DTF shipping details sync at {datetime.now()}")

        while api_calls_made < MAX_API_CALLS - 1:  # Reserve space for 2 calls
            iteration += 1

            filters = {
                "docstatus": 1,
                "is_eshipz_order_created": 1,
                "eshipz_tracking_number": ["=", ""],
                "eshipz_shipment_status": ["in", [None, "", "Shipment Not Created"]],
            }

            if last_processed_id:
                filters["name"] = [">", last_processed_id]

            dtf_list = frappe.db.get_all(
                "Dispatch and Transfer Form",
                filters=filters,
                fields=["name"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not dtf_list:
                frappe.logger().info("No more DTF records to process")
                break

            dtf_names = [d.name for d in dtf_list]
            last_processed_id = dtf_names[-1]

            try:
                # API Call 1: Shipment data
                shipment_data = call_shipment_api_bulk(dtf_names)
                api_calls_made += 1

                # API Call 2: Tracking data
                tracking_data = call_tracking_api_bulk(dtf_names)
                api_calls_made += 1

                frappe.logger().info(
                    f"API Calls {api_calls_made}/{MAX_API_CALLS} - "
                    f"Batch {iteration} - {len(dtf_names)} records"
                )

                shipment_map = {
                    d.get("customer_referenc"): d
                    for d in shipment_data or []
                    if d.get("customer_referenc")
                }

                tracking_map = {
                    d.get("order_id"): d for d in tracking_data or [] if d.get("order_id")
                }

                for pdf_name in dtf_names:
                    try:
                        shipment = shipment_map.get(pdf_name)
                        tracking = tracking_map.get(pdf_name)

                        if shipment and tracking:
                            vendor_name = shipment.get("vendor_name")
                            tracking_status = tracking.get("tag")
                            tracking_number = tracking.get("tracking_number")

                            combined_value = " - ".join(
                                filter(None, [tracking_number, vendor_name])
                            )

                            existing_status, existing_tracking = frappe.db.get_value(
                                "Dispatch and Transfer Form",
                                pdf_name,
                                ["eshipz_shipment_status", "eshipz_tracking_number"],
                            )

                            if not existing_tracking and existing_status != "Delivered":
                                frappe.db.set_value(
                                    "Dispatch and Transfer Form",
                                    pdf_name,
                                    {
                                        "eshipz_shipment_status": tracking_status,
                                        "eshipz_tracking_number": combined_value,
                                    },
                                    update_modified=False,
                                )
                            shipment_found.append(pdf_name)
                        else:
                            frappe.db.set_value(
                                "Dispatch and Transfer Form",
                                pdf_name,
                                "eshipz_shipment_status",
                                "Shipment Not Created",
                                update_modified=False,
                            )
                            shipment_not_found.append(pdf_name)

                        total_processed += 1

                    except Exception as record_error:
                        error_count += 1
                        frappe.log_error(
                            f"Error processing DTF record {pdf_name}", str(record_error)
                        )
                        continue

                frappe.db.commit()
                time.sleep(0.1)

            except Exception as api_error:
                api_calls_made += 2  # Count failed calls
                error_count += 1
                frappe.log_error(
                    f"API Error - DTF Batch {iteration}",
                    f"Batch starting at {dtf_names[0]}\nError: {str(api_error)}",
                )
                continue

            if api_calls_made >= MAX_API_CALLS - 1:
                frappe.logger().warning(
                    f"Reached API call limit ({api_calls_made}/{API_CALL_LIMIT}). "
                    f"Stopping gracefully."
                )
                break

        # Final summary
        has_more_records = api_calls_made >= MAX_API_CALLS - 1
        status = "PAUSED - More records pending" if has_more_records else "COMPLETED"

        summary = f"""
            DTF Shipping Details Sync Summary:
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
            frappe.log_error("DTF Shipping Sync Paused - API Limit Reached", summary)
        elif error_count > 0:
            frappe.log_error("DTF Shipping Sync Completed with Errors", summary)
        else:
            frappe.log_error("DTF Shipping Sync Completed Successfully", summary)

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
            "Error in DTF Shipping Batch Processing",
            frappe.get_traceback(),
        )
        raise


# ---------------------------------------------------Update Eshipz Shipment Actual Delivery Date-------------------------------------------------


@frappe.whitelist()
def schedule_update_delivery_date_for_dtf():
    """
    Updates delivery dates for DTF respecting API rate limits.
    """
    try:
        total_updated = 0
        total_processed = 0
        api_calls_made = 0
        error_count = 0
        last_processed_id = None
        iteration = 0

        frappe.logger().info(f"Starting DTF delivery date sync at {datetime.now()}")

        while api_calls_made < MAX_API_CALLS:
            iteration += 1

            filters = {
                "docstatus": 1,
                "is_eshipz_order_created": 1,
                "eshipz_shipment_status": ["not in", ["", "Shipment Not Created"]],
                "eshipz_tracking_number": ["!=", ""],
                "actual_delivery_date": ["in", ["", None]],
            }

            if last_processed_id:
                filters["name"] = [">", last_processed_id]

            dtf_list = frappe.db.get_all(
                "Dispatch and Transfer Form",
                filters=filters,
                fields=["name"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not dtf_list:
                frappe.logger().info("No more DTF records to process")
                break

            dtf_names = [d.name for d in dtf_list]
            last_processed_id = dtf_names[-1]
            total_processed += len(dtf_names)

            try:
                tracking_data = call_tracking_api_bulk(dtf_names)
                api_calls_made += 1

                frappe.logger().info(
                    f"API Call {api_calls_made}/{MAX_API_CALLS} - "
                    f"Batch {iteration} - {len(dtf_names)} records"
                )

                tracking_map = {
                    d.get("order_id"): d for d in tracking_data or [] if d.get("order_id")
                }

                for pdf_name in dtf_names:
                    try:
                        tracking = tracking_map.get(pdf_name)
                        if not tracking:
                            continue

                        delivery_date_str = tracking.get("delivery_date")
                        if not delivery_date_str:
                            continue

                        try:
                            delivery_date = datetime.strptime(
                                delivery_date_str,
                                "%a, %d %b %Y %H:%M:%S %Z",
                            ).strftime("%Y-%m-%d")

                            frappe.db.set_value(
                                "Dispatch and Transfer Form",
                                pdf_name,
                                {
                                    "actual_delivery_date": delivery_date,
                                    "eshipz_shipment_status": "Delivered",
                                },
                                update_modified=False,
                            )
                            total_updated += 1

                        except ValueError as date_error:
                            error_count += 1
                            frappe.log_error(
                                f"Invalid date format for DTF {pdf_name}",
                                f"Date: {delivery_date_str}\nError: {str(date_error)}",
                            )

                    except Exception as record_error:
                        error_count += 1
                        frappe.log_error(
                            f"Error processing DTF record {pdf_name}", str(record_error)
                        )
                        continue

                frappe.db.commit()
                time.sleep(0.1)

            except Exception as api_error:
                api_calls_made += 1
                error_count += 1
                frappe.log_error(
                    f"API Error - DTF Delivery Date Batch {iteration}",
                    f"Batch starting at {dtf_names[0]}\nError: {str(api_error)}",
                )
                continue

            if api_calls_made >= MAX_API_CALLS:
                frappe.logger().warning(
                    f"Reached API call limit ({api_calls_made}/{API_CALL_LIMIT}). "
                    f"Stopping gracefully."
                )
                break

        # Final summary
        has_more_records = api_calls_made >= MAX_API_CALLS
        status = "PAUSED - More records pending" if has_more_records else "COMPLETED"

        summary = f"""
            DTF Delivery Date Sync Summary:
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Status: {status}
            Total Processed: {total_processed}
            Total Updated: {total_updated}
            API Calls Made: {api_calls_made}/{API_CALL_LIMIT}
            Errors: {error_count}
            Last Processed ID: {last_processed_id or "N/A"}
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """

        if has_more_records:
            frappe.log_error(
                "DTF Delivery Date Sync Paused - API Limit Reached", summary
            )
        elif error_count > 0:
            frappe.log_error("DTF Delivery Date Sync Completed with Errors", summary)
        else:
            frappe.log_error("DTF Delivery Date Sync Completed Successfully", summary)

        return {
            "status": "success",
            "processed": total_processed,
            "updated": total_updated,
            "api_calls": api_calls_made,
            "errors": error_count,
            "has_more": has_more_records,
        }

    except Exception:
        frappe.log_error(
            "Error Updating Dispatch and Transfer Form Delivery Date",
            frappe.get_traceback(),
        )
        raise


# -------------------------------------------------Update Eshipz Shipment Status------------------------------------------------------


@frappe.whitelist()
def schedule_update_shipping_detail_status_for_dtf():
    """
    Updates shipment status for DTF respecting API rate limits.
    """
    try:
        total_updated = 0
        total_processed = 0
        api_calls_made = 0
        error_count = 0
        last_processed_id = None
        iteration = 0

        frappe.logger().info(f"Starting DTF shipment status sync at {datetime.now()}")

        while api_calls_made < MAX_API_CALLS:
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

            dtf_list = frappe.db.get_all(
                "Dispatch and Transfer Form",
                filters=filters,
                fields=["name", "eshipz_shipment_status"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not dtf_list:
                frappe.logger().info("No more DTF records to process")
                break

            dtf_names = [d.name for d in dtf_list]
            last_processed_id = dtf_names[-1]
            total_processed += len(dtf_names)

            try:
                tracking_data = call_tracking_api_bulk(dtf_names)
                api_calls_made += 1

                frappe.logger().info(
                    f"API Call {api_calls_made}/{MAX_API_CALLS} - "
                    f"Batch {iteration} - {len(dtf_names)} records"
                )

                tracking_map = {
                    t.get("order_id"): t for t in tracking_data or [] if t.get("order_id")
                }

                for dtf in dtf_list:
                    try:
                        pdf_name = dtf.name
                        existing_status = dtf.eshipz_shipment_status

                        shipment = tracking_map.get(pdf_name)
                        if not shipment:
                            continue

                        shipment_status = shipment.get("shipment_status")
                        tag_status = shipment.get("tag")

                        new_status = None

                        # Cancelled has highest priority
                        if shipment_status == "cancelled":
                            new_status = "Cancelled"
                        # Normal progression
                        elif existing_status != "Delivered" and tag_status:
                            new_status = tag_status

                        if new_status:
                            frappe.db.set_value(
                                "Dispatch and Transfer Form",
                                pdf_name,
                                "eshipz_shipment_status",
                                new_status,
                                update_modified=False,
                            )
                            total_updated += 1

                    except Exception as record_error:
                        error_count += 1
                        frappe.log_error(
                            f"Error processing DTF status record {pdf_name}",
                            str(record_error),
                        )
                        continue

                frappe.db.commit()
                time.sleep(0.1)

            except Exception as api_error:
                api_calls_made += 1
                error_count += 1
                frappe.log_error(
                    f"API Error - DTF Status Batch {iteration}",
                    f"Batch starting at {dtf_names[0]}\nError: {str(api_error)}",
                )
                continue

            if api_calls_made >= MAX_API_CALLS:
                frappe.logger().warning(
                    f"Reached API call limit ({api_calls_made}/{API_CALL_LIMIT}). "
                    f"Stopping gracefully."
                )
                break

        # Final summary
        has_more_records = api_calls_made >= MAX_API_CALLS
        status = "PAUSED - More records pending" if has_more_records else "COMPLETED"

        summary = f"""
            DTF Shipment Status Sync Summary:
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            Status: {status}
            Total Processed: {total_processed}
            Total Updated: {total_updated}
            API Calls Made: {api_calls_made}/{API_CALL_LIMIT}
            Errors: {error_count}
            Last Processed ID: {last_processed_id or "N/A"}
            ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """

        if has_more_records:
            frappe.log_error("DTF Status Sync Paused - API Limit Reached", summary)
        elif error_count > 0:
            frappe.log_error("DTF Status Sync Completed with Errors", summary)
        else:
            frappe.log_error("DTF Status Sync Completed Successfully", summary)

        return {
            "status": "success",
            "processed": total_processed,
            "updated": total_updated,
            "api_calls": api_calls_made,
            "errors": error_count,
            "has_more": has_more_records,
        }

    except Exception:
        frappe.log_error(
            "Error While Updating Shipment Status For Dispatch and Transfer Form",
            frappe.get_traceback(),
        )
        raise


# -------------------------------------------------Get Delivered Invoices and Fetch PODs------------------------------------------------------


@frappe.whitelist()
def get_delivered_pdf_and_fetch_pods_for_dtf():
    """
    Fetch PODs for delivered Dispatch and Transfer Forms (DTF)
    using BULK shipment API.
    Cursor-based batching + API rate limit safe.
    """

    try:
        total_processed = 0
        added, exists, failed, skipped = [], [], [], []
        api_calls_made = 0
        error_count = 0
        last_processed_id = None
        iteration = 0

        frappe.logger().info(
            f"DTF POD Sync started at {datetime.now()} | API Limit: {MAX_API_CALLS}"
        )

        def safe_docname(name):
            return "".join(c for c in name if c.isalnum() or c in ("-", "_")).rstrip()

        while api_calls_made < MAX_API_CALLS:
            iteration += 1

            # ---- DTF Filters ----
            filters = {
                "docstatus": 1,
                "is_eshipz_order_created": 1,
                "eshipz_shipment_status": "Delivered",
                "eshipz_tracking_number": ["!=", ""],
            }

            if last_processed_id:
                filters["name"] = [">", last_processed_id]

            dtf_docs = frappe.db.get_all(
                "Dispatch and Transfer Form",
                filters=filters,
                fields=["name"],
                order_by="name asc",
                limit=BATCH_SIZE,
            )

            if not dtf_docs:
                frappe.logger().info("No more DTF records to process")
                break

            dtf_names = [d.name for d in dtf_docs]
            last_processed_id = dtf_names[-1]

            # ---- Existing PODs (bulk check) ----
            existing_files = frappe.db.get_all(
                "File",
                filters={
                    "attached_to_doctype": "Dispatch and Transfer Form",
                    "attached_to_name": ["in", dtf_names],
                    "file_name": ["like", "%pod%"],
                },
                fields=["attached_to_name"],
            )
            existing_pod_map = {f.attached_to_name for f in existing_files}

            frappe.logger().info(
                f"Batch {iteration} | Records {len(dtf_names)} | "
                f"API Calls {api_calls_made}/{MAX_API_CALLS}"
            )

            # ---- BULK Shipment API Call ----
            shipment_data = call_shipment_api_bulk(dtf_names)
            api_calls_made += 1

            if not shipment_data:
                failed.extend(dtf_names)
                total_processed += len(dtf_names)
                continue

            # ---- Build POD Map: DTF → POD URL ----
            pod_map = {
                s.get("customer_referenc"): s.get("pod_link")
                for s in shipment_data
                if s.get("customer_referenc") and s.get("pod_link")
            }

            # ---- Attach PODs ----
            for name in dtf_names:

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
                        "Dispatch and Transfer Form",
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
                        f"DTF POD Attach Error - {name}",
                        frappe.get_traceback(),
                    )

                total_processed += 1

            frappe.db.commit()
            time.sleep(0.1)

        # ---- Final Summary ----
        has_more_records = api_calls_made >= MAX_API_CALLS
        status = "PAUSED - More records pending" if has_more_records else "COMPLETED"

        summary = f"""
        DTF POD Fetch Summary:
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        Status: {status}
        Total Processed: {total_processed}
        Added: {len(added)}
        Already Exists: {len(exists)}
        Failed: {len(failed)}
        Skipped: {len(skipped)}
        API Calls Made: {api_calls_made}/{MAX_API_CALLS}
        Errors: {error_count}
        Last Processed ID: {last_processed_id or "N/A"}
        ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        """

        if has_more_records:
            frappe.log_error("DTF POD Sync Paused - API Limit Reached", summary)
        elif error_count > 0:
            frappe.log_error("DTF POD Sync Completed with Errors", summary)
        else:
            frappe.log_error("DTF POD Sync Completed Successfully", summary)

        return {
            "status": "success",
            "processed": total_processed,
            "added": len(added),
            "exists": len(exists),
            "failed": len(failed),
            "skipped": len(skipped),
            "api_calls": api_calls_made,
            "errors": error_count,
            "has_more": has_more_records,
        }

    except Exception:
        frappe.log_error("DTF POD Batch Fatal Error", frappe.get_traceback())
        raise