app_name = "bo_eshipz_integration"
app_title = "Bo Eshipz Integration"
app_publisher = "Akhilam Inc."
app_description = "shipping aggregator for blue occean(abk)"
app_email = "contact@akhilaminc.com"
app_license = "mit"

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "bo_eshipz_integration",
# 		"logo": "/assets/bo_eshipz_integration/logo.png",
# 		"title": "Bo Eshipz Integration",
# 		"route": "/bo_eshipz_integration",
# 		"has_permission": "bo_eshipz_integration.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/bo_eshipz_integration/css/bo_eshipz_integration.css"
# app_include_js = "/assets/bo_eshipz_integration/js/bo_eshipz_integration.js"

# include js, css files in header of web template
# web_include_css = "/assets/bo_eshipz_integration/css/bo_eshipz_integration.css"
# web_include_js = "/assets/bo_eshipz_integration/js/bo_eshipz_integration.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "bo_eshipz_integration/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
doctype_js = {
    "Pick List" : "custom_scripts/pick_list.js",
    "Sales Invoice" : "custom_scripts/sales_invoice.js",
    "Delivery Note" : "custom_scripts/delivery_note.js"
}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "bo_eshipz_integration/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "bo_eshipz_integration.utils.jinja_methods",
# 	"filters": "bo_eshipz_integration.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "bo_eshipz_integration.install.before_install"
# after_install = "bo_eshipz_integration.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "bo_eshipz_integration.uninstall.before_uninstall"
# after_uninstall = "bo_eshipz_integration.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "bo_eshipz_integration.utils.before_app_install"
# after_app_install = "bo_eshipz_integration.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "bo_eshipz_integration.utils.before_app_uninstall"
# after_app_uninstall = "bo_eshipz_integration.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "bo_eshipz_integration.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

doc_events = {
    "Delivery Note": {
        "before_submit":"bo_eshipz_integration.bo_eshipz_integration.override.delivery_note.before_submit",
        "validate":"bo_eshipz_integration.bo_eshipz_integration.override.delivery_note.validate"
    },
    "Sales Invoice":{
        "on_submit":"bo_eshipz_integration.bo_eshipz_integration.override.sales_invoice.on_submit"
    }
}

# Scheduled Tasks
# ---------------

scheduler_events = {

    "cron":{
        # Sales Invoice
        "0 18 * * *":[
            "bo_eshipz_integration.bo_eshipz_integration.scheduler.schedule_update_shipping_details_for_si",
            "bo_eshipz_integration.bo_eshipz_integration.scheduler.schedule_update_shipping_detail_status_for_si"
        ],
        "45 1 * * *":[
            "bo_eshipz_integration.bo_eshipz_integration.scheduler.get_delivered_pdf_and_fetch_pods_for_si"
        ],
        "45 2 * * *":[
            "bo_eshipz_integration.bo_eshipz_integration.scheduler.schedule_update_delivery_date_for_si"
        ],

        #Dispatch Forms
        "0 7 * * *":[
            "bo_eshipz_integration.bo_eshipz_integration.dispatch_scheduler.schedule_update_shipping_details_for_dtf",
            "bo_eshipz_integration.bo_eshipz_integration.dispatch_scheduler.schedule_update_shipping_detail_status_for_dtf"
        ],
        "30 3 * * *":[
            "bo_eshipz_integration.bo_eshipz_integration.dispatch_scheduler.get_delivered_pdf_and_fetch_pods_for_dtf"
        ],
        "15 4 * * *":[
            "bo_eshipz_integration.bo_eshipz_integration.dispatch_scheduler.schedule_update_delivery_date_for_dtf"
        ],

        #Pickup Forms
        "0 4 * * *":[
            "bo_eshipz_integration.bo_eshipz_integration.pickup_scheduler.schedule_update_shipping_detail_status_for_pf"
        ],
        "45 4 * * *":[
            "bo_eshipz_integration.bo_eshipz_integration.pickup_scheduler.get_delivered_pdf_and_fetch_pods_for_pf"
        ],
        "30 5 * * *":[
            "bo_eshipz_integration.bo_eshipz_integration.pickup_scheduler.schedule_update_delivery_date_for_pf"
        ]
    }
}

# Testing
# -------

# before_tests = "bo_eshipz_integration.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "bo_eshipz_integration.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "bo_eshipz_integration.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["bo_eshipz_integration.utils.before_request"]
# after_request = ["bo_eshipz_integration.utils.after_request"]

# Job Events
# ----------
# before_job = ["bo_eshipz_integration.utils.before_job"]
# after_job = ["bo_eshipz_integration.utils.after_job"]

fixtures = [
    {"dt": "Custom Field", "filters": [
        [
            "module", "in", ["Bo Eshipz Integration"]
        ]
    ]},
    
]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"bo_eshipz_integration.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }

