"""ZohoInventory target sink class, which handles writing streams."""
from target_zoho_inventory.client import ZohoInventorySink
from datetime import datetime
import json
import ast


class PurchaseOrderSink(ZohoInventorySink):
    """ZohoInventory target sink class."""

    name = "Bills"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def process_record(self, record: dict, context: dict) -> None:
        if self.stream_name == "Bills":
            self.process_bills(record, context)

    def process_bills(self, record, context):
        """Process the record."""
        path = "/purchaseorders"
        vendor_id = record.get("vendorId")

        if type(record['lineItems']) == str:
            record['lineItems'] = json.loads(record['lineItems'])

        if "vendorName" in record and not vendor_id:
            matches = self.search_vendors(record["vendorName"])
            if matches:
                vendor_id = matches[0]["contact_id"]
            else:
                raise Exception(f"No matches found for vendor {record['vendorName']}")

        elif "vendorNum" in record:
            vendor_id = record.get("vendorNum")

        is_taxable = record.get("taxCode")
        line_items = [
            self.parse_line(line) for line in record['lineItems']
        ]

        request_data = {
            "vendor_id": vendor_id,
            "reference_number": record.get("id"),
            "purchaseorder_number": record.get("billNum"),
            "date": datetime.fromisoformat(record.get('createdAt')).strftime("%Y-%m-%d"),
            "currency_code": record.get("currency"),
            "line_items": line_items
        }

        headers = self.http_headers

        params = {}
        if self.config.get('organization_id'):
            params['organization_id'] = self.config.get('organization_id')
        resp = self.request_api(
            "POST", path, request_data=request_data,
            params=params, headers=headers
        )

    def parse_line(self, line):
        if not line.get("productId"):
            items = self.paginated_search(
                "/items", line.get("productName"), "name.contains"
            )
            names = [v["name"] for v in items]
            matches = list(
                self.get_close_matches(
                    line.get("productName"), names, n=1, cutoff=0.8
                ).keys()
            )
            result = list(filter(lambda x: x["name"] == matches[0], items))

            line["productId"] = result[0]["item_id"]

        return {
            "name": line.get("productName"),
            "item_id": line.get("productId"),
            "quantity": line.get("quantity"),
            "unit_price": line.get("unitPrice"),
            "discount": line.get("discountAmount"),
            "tax_name": line.get("taxCode"),
            "description": line.get("description"),
        }

class BuyOrderSink(ZohoInventorySink):
    """ZohoInventory target sink class."""

    name = "BuyOrders"
    endpoint = "/purchaseorders"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        #process transaction_date
        transaction_date = record.get("transaction_date")
        if isinstance(transaction_date, datetime):
            transaction_date = transaction_date.strftime("%Y-%m-%d")
        
        #get payload
        payload = {
            "date": transaction_date,
        }

        if record.get("id"):
            payload.update({"reference_number":record.get("id")})

        #get supplier_name
        if record.get("supplier_name"):
            matches = self.search_vendors(record["supplier_name"])
            if matches:
                vendor_id = matches[0]["contact_id"]
                payload["vendor_id"] = vendor_id
            else:
                self.logger.info(f"Skipping order because no matches found for vendor {record['supplier_name']}")
                return None

        #process line_items
        line_items = record.get("line_items", [])
        if isinstance(line_items, str):
            line_items = self.parse_objs(line_items)
        if line_items:
            line_items = [
                {"quantity": item.get("quantity"), "item_id": item.get("product_remoteId")}
                for item in line_items
            ]
            payload["line_items"] = line_items
        else:
            self.logger.info("Skipping order with no line items")
            return None
        return payload

    def upsert_record(self, record: dict, context: dict) -> None:
        state_updates = dict()
        if record:
            params = {}
            if self.config.get('organization_id'):
                params['organization_id'] = self.config.get('organization_id')
            response = self.request_api(
                "POST", endpoint=self.endpoint,
                request_data=record,
                params=params
            )
            res_json_id = response.json()["purchaseorder"]["purchaseorder_id"]
            self.logger.info(f"{self.name} created with id: {res_json_id}")
            return res_json_id, True, state_updates

class AssemblyOrderSink(ZohoInventorySink):
    """ZohoInventory target sink class for assembly orders."""

    name = "AssemblyOrders"
    endpoint = "/bundles"

    def preprocess_record(self, record: dict, context: dict) -> dict:
        # Process date
        transaction_date = record.get("transaction_date")
        if isinstance(transaction_date, datetime):
            transaction_date = transaction_date.strftime("%Y-%m-%d")
        
        # Get payload
        payload = {
            "date": transaction_date,
            "composite_item_id": record.get("product_remoteId"),
            "composite_item_name": record.get("product_name"),
            "quantity_to_bundle": record.get("quantity"),
            "is_completed": record.get("is_completed", False),
        }
        
        if record.get("id"):
            payload.update({"reference_number":record.get("id")})

        # Process line items
        line_items = record.get("line_items", [])
        if isinstance(line_items, str):
            line_items = self.parse_objs(line_items)
        if line_items:
            processed_line_items = []
            for item in line_items:
                processed_item = {
                    "item_id": item.get("part_product_remoteId"),
                    "name": item.get("part_product_name"),
                    "quantity_consumed": item.get("part_quantity"),
                }
                if self.config.get('export_warehouse_id'):
                    processed_item["warehouse_id"] = self.config.get('export_warehouse_id')
                               
                if item.get("account_id"):
                    processed_item["account_id"] = item.get("account_id")
                
                processed_line_items.append(processed_item)
            payload["line_items"] = processed_line_items
        else:
            self.logger.info("Skipping assembly order with no line items")
            return None

        return payload

    def upsert_record(self, record: dict, context: dict) -> None:
        state_updates = dict()
        if record:
            params = {
                "ignore_auto_number_generation" : True
            }
            if self.config.get('organization_id'):
                params['organization_id'] = self.config.get('organization_id')
            

            response = self.request_api(
                "POST", endpoint=self.endpoint,
                request_data=record,
                params=params
            )

            
            try:
                res_json_id = response.json()["bundle"]["bundle_id"]
                self.logger.info(f"{self.name} created with id: {res_json_id}")
                return res_json_id, True, state_updates
            except Exception as e:
                self.logger.error(f"Failed to extract bundle ID from response: {e}")
                self.logger.error(f"Response content: {response.text}")
                return None, False, state_updates

