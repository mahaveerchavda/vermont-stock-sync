#!/usr/bin/env python3
"""
Vermont Sales -> Shopify (chavda.com) nightly stock sync.

Auth: Shopify custom-app "client credentials" grant, exactly as proven by
this repo's shopify-test.yml (grant_type=client_credentials against
https://<shop>/admin/oauth/access_token using SHOPIFY_CLIENT_ID /
SHOPIFY_CLIENT_SECRET). No other auth method is used.

Rules enforced (do not relax any of these without explicit sign-off):
  - Match Vermont's `model` column to Shopify variant SKUs, exact string match.
  - Write `available` inventory ONLY at the Warehouse Stock location.
    Shop location is never read or written.
  - Skip rows with negative availability.
  - Skip rows whose availability is not a finite whole number (rejects
    blank/garbage text, +-inf, NaN, and fractional values like "5.7" -
    none of these are silently rounded or truncated).
  - Skip SKUs that match more than one Shopify variant (ambiguous).
  - Skip SKUs (models) that appear more than once in the Vermont feed
    itself - a duplicate row means we can't be sure which value is
    current, so neither is written.
  - Never touch price. Never touch product status (no archive/unpublish/publish).
  - Only products that actually received a successful inventory write this
    run get the dated tags. Stale `vermont<8 digits>` tags are removed
    BEFORE today's tags are added (so a product already near Shopify's
    per-product tag limit always has room), then `vermont` and
    `vermont<DDMMYYYY>` (today, South Africa time) are added. Every other
    tag on the product is left exactly as-is. A product's tag update only
    counts as successful if both its remove and add operations succeeded.
  - Never creates products. Unmatched Vermont SKUs are only ever reported,
    never turned into new products.

Safety:
  - Defaults to --dry-run. A live run requires --live on the command line.
    In dry run, nothing is written to Shopify: `skus_written` and
    `products_tagged` in the summary are always 0, and the separate
    `skus_would_write` / `products_would_tag` fields show what a live run
    would do instead.
  - inventorySetQuantities sets an *absolute* value, so re-running this
    script (or overlapping with a leftover manual batch) is idempotent -
    it can only waste an API call, never corrupt a quantity.
  - Uses only the Python standard library - no pip install step required.
"""

import argparse
import csv
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Fixed constants - do not change without explicit confirmation from the
# business owner. Getting either location ID wrong risks writing stock to
# the wrong place or touching the Shop location, which is off-limits.
# ---------------------------------------------------------------------------
WAREHOUSE_STOCK_LOCATION_ID = "gid://shopify/Location/83949387993"
SHOP_LOCATION_ID = "gid://shopify/Location/83837911257"  # NEVER written to.

SHOPIFY_API_VERSION = os.environ.get("SHOPIFY_API_VERSION", "2025-01")

INVENTORY_BATCH_SIZE = int(os.environ.get("INVENTORY_BATCH_SIZE", "250"))  # API hard cap
TAG_BATCH_SIZE = int(os.environ.get("TAG_BATCH_SIZE", "40"))  # empirically safe under the 1000 cost cap

SAST = timezone(timedelta(hours=2))  # South Africa Standard Time, no DST.

DATED_TAG_RE = re.compile(r"^vermont(\d{8})$")

WHOLE_NUMBER_RE = re.compile(r"^[+-]?\d+$")


def log(*args):
    print(*args, flush=True)


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_access_token(shop, client_id, client_secret):
    """Client-credentials grant, identical to shopify-test.yml."""
    import urllib.parse

    payload = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
    ).encode()

    req = urllib.request.Request(
        f"https://{shop}/admin/oauth/access_token",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.load(resp)
    except urllib.error.HTTPError as e:
        raise SystemExit(f"Shopify auth failed: HTTP {e.code} - {e.read().decode(errors='replace')}")

    token = result.get("access_token")
    if not token:
        raise SystemExit("Shopify auth failed: no access_token in response.")
    scopes = {s.strip() for s in result.get("scope", "").split(",") if s.strip()}
    required = {"write_inventory", "write_products", "read_locations"}
    missing = required - scopes
    if missing:
        raise SystemExit(f"Shopify token missing required scopes: {sorted(missing)}")
    return token


# ---------------------------------------------------------------------------
# GraphQL helper with throttle-aware retry
# ---------------------------------------------------------------------------

def graphql(shop, token, query, variables=None, max_retries=6):
    url = f"https://{shop}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    body = json.dumps({"query": query, "variables": variables or {}}).encode()

    delay = 2
    for attempt in range(1, max_retries + 1):
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": token,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.load(resp)
        except urllib.error.HTTPError as e:
            raw = e.read().decode(errors="replace")
            if e.code == 429 and attempt < max_retries:
                log(f"  HTTP 429, retrying in {delay}s (attempt {attempt}/{max_retries})")
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            raise SystemExit(f"GraphQL HTTP error {e.code}: {raw}")
        except urllib.error.URLError as e:
            if attempt < max_retries:
                log(f"  network error ({e}), retrying in {delay}s")
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            raise

        errors = data.get("errors")
        if errors:
            codes = {err.get("extensions", {}).get("code") for err in errors}
            if "THROTTLED" in codes and attempt < max_retries:
                log(f"  throttled, retrying in {delay}s (attempt {attempt}/{max_retries})")
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            raise SystemExit(f"GraphQL errors: {json.dumps(errors)}")

        return data["data"]

    raise SystemExit("GraphQL request failed after retries.")


# ---------------------------------------------------------------------------
# Bulk export of every product variant's SKU / inventory item / product tags
# ---------------------------------------------------------------------------

def run_bulk_variant_export(shop, token, poll_interval=5, timeout_seconds=1800):
    query = """
    mutation {
      bulkOperationRunQuery(
        query: \"\"\"
        {
          productVariants {
            edges {
              node {
                id
                sku
                inventoryItem { id }
                product { id tags }
              }
            }
          }
        }
        \"\"\"
      ) {
        bulkOperation { id status }
        userErrors { field message }
      }
    }
    """
    data = graphql(shop, token, query)
    errs = data["bulkOperationRunQuery"]["userErrors"]
    if errs:
        raise SystemExit(f"bulkOperationRunQuery failed: {errs}")

    log("Bulk variant export started, polling for completion...")
    poll_query = """
    query {
      currentBulkOperation {
        id
        status
        errorCode
        objectCount
        url
      }
    }
    """
    waited = 0
    while waited < timeout_seconds:
        data = graphql(shop, token, poll_query)
        op = data["currentBulkOperation"]
        status = op["status"]
        if status == "COMPLETED":
            log(f"Bulk export complete: {op['objectCount']} objects.")
            return op["url"]
        if status in ("FAILED", "CANCELED", "EXPIRED"):
            raise SystemExit(f"Bulk export {status}: {op.get('errorCode')}")
        time.sleep(poll_interval)
        waited += poll_interval

    raise SystemExit("Bulk export timed out.")


def download_jsonl(url, dest_path):
    if not url:
        # Empty result set - Shopify returns no url when there's no data.
        open(dest_path, "w").close()
        return dest_path
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=120) as resp, open(dest_path, "wb") as out:
        out.write(resp.read())
    return dest_path


# ---------------------------------------------------------------------------
# Matching logic - identical rules to the manual run this replaces, plus
# stricter quantity parsing and duplicate-feed-row protection.
# ---------------------------------------------------------------------------

def build_sku_map(variants_jsonl_path):
    sku_map = defaultdict(list)
    total = 0
    with open(variants_jsonl_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            total += 1
            sku = (obj.get("sku") or "").strip()
            if not sku:
                continue
            sku_map[sku].append(obj)
    log(f"Loaded {total} Shopify variants, {len(sku_map)} unique non-empty SKUs.")
    return sku_map


def parse_quantity(raw):
    """
    Parse a feed availability value as a whole number.
    Returns (ok, int_value_or_None, rejection_reason_or_None).
    Rejects: empty/garbage text, +-infinity, NaN, and any value with a
    fractional part (e.g. "5.7") - none of these are rounded/truncated.
    """
    s = (raw or "").strip()
    if WHOLE_NUMBER_RE.match(s):
        return True, int(s), None

    try:
        f = float(s)
    except (ValueError, OverflowError):
        return False, None, "unparseable_availability"

    if not math.isfinite(f):
        return False, None, "non_finite_availability"

    if f != int(f):
        return False, None, "fractional_availability"

    return True, int(f), None


def build_matches(csv_path, sku_map):
    matched, negative_skipped, ambiguous_skipped, unmatched, duplicate_skipped = [], [], [], [], []

    with open(csv_path, newline="") as f:
        rows = list(csv.DictReader(f))
    log(f"Loaded {len(rows)} rows from Vermont feed: {csv_path}")

    model_counts = Counter((r.get("model") or "").strip() for r in rows if (r.get("model") or "").strip())

    for r in rows:
        model = (r.get("model") or "").strip()
        avail_raw = (r.get("availability") or "").strip()
        if not model:
            continue

        # Duplicate SKU within the feed itself - can't trust either value,
        # so neither is written. Checked before anything else.
        if model_counts[model] > 1:
            duplicate_skipped.append({"model": model, "availability": avail_raw, "occurrences": model_counts[model]})
            continue

        ok, avail, reason = parse_quantity(avail_raw)
        if not ok:
            unmatched.append({"model": model, "availability": avail_raw, "reason": reason})
            continue

        variants = sku_map.get(model)
        if not variants:
            unmatched.append({"model": model, "availability": avail_raw, "reason": "no_sku_match"})
            continue

        if len(variants) > 1:
            ambiguous_skipped.append({"model": model, "availability": avail_raw, "variant_ids": [v["id"] for v in variants]})
            continue

        v = variants[0]
        if avail < 0:
            negative_skipped.append({"model": model, "availability": avail_raw})
            continue

        matched.append(
            {
                "model": model,
                "availability": avail,
                "variant_id": v["id"],
                "inventory_item_id": v["inventoryItem"]["id"],
                "product_id": v["product"]["id"],
                "product_tags": v["product"]["tags"],
            }
        )

    log(
        f"Matched: {len(matched)}  Negative-skipped: {len(negative_skipped)}  "
        f"Ambiguous-skipped: {len(ambiguous_skipped)}  Duplicate-feed-skipped: {len(duplicate_skipped)}  "
        f"Unmatched: {len(unmatched)}"
    )
    return matched, negative_skipped, ambiguous_skipped, unmatched, duplicate_skipped


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# ---------------------------------------------------------------------------
# Inventory writes
# ---------------------------------------------------------------------------

SET_QTY_MUTATION = """
mutation SetQty($input: InventorySetQuantitiesInput!) {
  inventorySetQuantities(input: $input) {
    inventoryAdjustmentGroup { id }
    userErrors { field message code }
  }
}
"""


def write_inventory(shop, token, matched, dry_run):
    """
    Returns:
      successful_product_ids - products with an actually-confirmed write this run
                                (always empty in dry run - nothing was actually written)
      attempted_product_ids  - every product a write was attempted/planned for,
                                used to preview tagging in dry run
      skus_would_write       - how many SKUs this run planned to write (both modes)
      skus_written           - how many SKUs were actually confirmed written (0 in dry run)
      errors                 - per-batch userErrors, live mode only
    """
    successful_product_ids = set()
    attempted_product_ids = {m["product_id"] for m in matched}
    skus_would_write = len(matched)
    skus_written = 0
    errors = []

    batches = list(chunked(matched, INVENTORY_BATCH_SIZE))
    log(f"Inventory: {len(matched)} SKUs in {len(batches)} batch(es) of up to {INVENTORY_BATCH_SIZE}.")

    if dry_run:
        for i, batch in enumerate(batches):
            log(f"  [dry-run] batch {i+1}/{len(batches)}: would write {len(batch)} SKUs (no write performed)")
        return successful_product_ids, attempted_product_ids, skus_would_write, skus_written, errors

    for i, batch in enumerate(batches):
        quantities = [
            {
                "inventoryItemId": m["inventory_item_id"],
                "locationId": WAREHOUSE_STOCK_LOCATION_ID,
                "quantity": m["availability"],
            }
            for m in batch
        ]
        variables = {
            "input": {
                "name": "available",
                "reason": "correction",
                "ignoreCompareQuantity": True,
                "quantities": quantities,
            }
        }

        data = graphql(shop, token, SET_QTY_MUTATION, variables)
        result = data["inventorySetQuantities"]
        if result["userErrors"]:
            errors.append({"batch": i, "errors": result["userErrors"]})
            log(f"  batch {i+1}/{len(batches)}: userErrors: {result['userErrors']}")
            # Whole batch failed together - Shopify applies InventorySetQuantities
            # as one adjustment group, so treat none of this batch as successful.
            continue

        for m in batch:
            successful_product_ids.add(m["product_id"])
        skus_written += len(batch)
        log(f"  batch {i+1}/{len(batches)}: OK ({len(batch)} SKUs)")

    return successful_product_ids, attempted_product_ids, skus_would_write, skus_written, errors


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------

def compute_tag_plan(matched, basis_product_ids, today_tag):
    """
    One entry per product in basis_product_ids that still needs a tag
    change: which stale dated tags to remove and which tags to add.
    basis_product_ids is the successfully-updated set in live mode, or the
    attempted/would-write set in dry run (for an accurate preview).
    """
    by_product = {}
    seen = set()
    for m in matched:
        pid = m["product_id"]
        if pid not in basis_product_ids or pid in seen:
            continue
        seen.add(pid)
        current_tags = m["product_tags"] or []
        stale_dated = [t for t in current_tags if DATED_TAG_RE.match(t) and t != today_tag]
        to_add = [t for t in ("vermont", today_tag) if t not in current_tags]
        to_remove = stale_dated
        if not to_add and not to_remove:
            continue  # already fully tagged for today, nothing to do
        by_product[pid] = {"add": to_add, "remove": to_remove}
    return by_product


def write_tags(shop, token, tag_plan, dry_run):
    """
    Returns (products_tagged, errors, products_would_tag).
    products_tagged only counts a product once BOTH its remove and its add
    operations (whichever were planned) came back with no userErrors.
    Always 0 in dry run - nothing is actually written.
    Each product's mutations are ordered remove-before-add, so stale tags
    are cleared before new ones are added (keeps headroom under Shopify's
    per-product tag limit instead of briefly exceeding it).
    """
    product_ids = list(tag_plan.keys())
    products_would_tag = len(product_ids)

    if dry_run:
        for pid in product_ids:
            plan = tag_plan[pid]
            log(f"  [dry-run] would update product {pid}: -remove {plan['remove']}  +add {plan['add']}")
        return 0, [], products_would_tag

    batches = list(chunked(product_ids, TAG_BATCH_SIZE))
    log(f"Tagging: {len(product_ids)} products in {len(batches)} batch(es) of up to {TAG_BATCH_SIZE}.")

    tagged = 0
    errors = []

    for i, batch_ids in enumerate(batches):
        parts = []
        idx_to_pid = {}
        for j, pid in enumerate(batch_ids):
            idx_to_pid[j] = pid
            plan = tag_plan[pid]
            # Remove stale dated tags BEFORE adding new ones (frees headroom
            # under the per-product tag cap; GraphQL executes root mutation
            # fields serially in the order listed, so this order is honored).
            if plan["remove"]:
                tags_json = json.dumps(plan["remove"])
                parts.append(f'r{j}: tagsRemove(id: "{pid}", tags: {tags_json}) {{ userErrors {{ field message }} }}')
            if plan["add"]:
                tags_json = json.dumps(plan["add"])
                parts.append(f'a{j}: tagsAdd(id: "{pid}", tags: {tags_json}) {{ userErrors {{ field message }} }}')

        if not parts:
            continue

        query = "mutation {\n  " + "\n  ".join(parts) + "\n}"
        data = graphql(shop, token, query)

        failed_indices = set()
        for key, val in data.items():
            if val and val.get("userErrors"):
                j = int(key[1:])
                failed_indices.add(j)

        batch_failed_pids = sorted({idx_to_pid[j] for j in failed_indices})
        succeeded = len(batch_ids) - len(batch_failed_pids)
        tagged += succeeded

        if batch_failed_pids:
            errors.append({"batch": i, "failed_products": batch_failed_pids})
            log(f"  tag batch {i+1}/{len(batches)}: {succeeded}/{len(batch_ids)} OK, failed: {batch_failed_pids}")
        else:
            log(f"  tag batch {i+1}/{len(batches)}: OK ({succeeded} products)")

    return tagged, errors, products_would_tag


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Vermont -> Shopify nightly stock sync")
    parser.add_argument("--live", action="store_true", help="Actually write to Shopify. Without this flag, runs as a dry run.")
    parser.add_argument("--csv-path", default="pricing_availability.csv", help="Path to the freshly-downloaded Vermont pricing_availability.csv.")
    parser.add_argument("--summary-out", default="vermont_sync_summary.json", help="Where to write the run summary JSON.")
    args = parser.parse_args()

    dry_run = not args.live

    shop = os.environ.get("SHOPIFY_SHOP", "").strip()
    client_id = os.environ.get("SHOPIFY_CLIENT_ID", "").strip()
    client_secret = os.environ.get("SHOPIFY_CLIENT_SECRET", "").strip()
    if not all([shop, client_id, client_secret]):
        raise SystemExit("Missing one or more of SHOPIFY_SHOP / SHOPIFY_CLIENT_ID / SHOPIFY_CLIENT_SECRET.")

    log(f"=== Vermont stock sync - {'DRY RUN' if dry_run else 'LIVE'} ===")
    log(f"Shop: {shop}  API version: {SHOPIFY_API_VERSION}")
    log(f"Warehouse Stock location: {WAREHOUSE_STOCK_LOCATION_ID} (write target)")
    log(f"Shop location: {SHOP_LOCATION_ID} (never touched)")

    if not os.path.exists(args.csv_path):
        raise SystemExit(
            f"Vermont CSV not found at '{args.csv_path}'. This script expects a freshly downloaded "
            f"pricing_availability.csv to already be in place at that path before it runs - it does "
            f"not fetch or cache the feed itself."
        )

    token = get_access_token(shop, client_id, client_secret)
    log("Authenticated OK, required scopes confirmed.")

    bulk_url = run_bulk_variant_export(shop, token)
    variants_path = "shopify_variants_export.jsonl"
    download_jsonl(bulk_url, variants_path)

    sku_map = build_sku_map(variants_path)
    matched, negative_skipped, ambiguous_skipped, unmatched, duplicate_skipped = build_matches(args.csv_path, sku_map)

    today_tag = "vermont" + datetime.now(SAST).strftime("%d%m%Y")
    log(f"Today's dated tag: {today_tag}")

    successful_product_ids, attempted_product_ids, skus_would_write, skus_written, inv_errors = write_inventory(
        shop, token, matched, dry_run
    )

    # Live mode only tags products that actually got a confirmed write.
    # Dry run previews against every product a write was planned for.
    tag_basis = attempted_product_ids if dry_run else successful_product_ids
    tag_plan = compute_tag_plan(matched, tag_basis, today_tag)
    products_tagged, tag_errors, products_would_tag = write_tags(shop, token, tag_plan, dry_run)

    summary = {
        "mode": "dry_run" if dry_run else "live",
        "timestamp_sast": datetime.now(SAST).isoformat(),
        "today_tag": today_tag,
        "vermont_rows_total": len(matched) + len(negative_skipped) + len(ambiguous_skipped) + len(unmatched) + len(duplicate_skipped),
        "matched_candidates": len(matched),
        "skus_written": skus_written,
        "skus_would_write": skus_would_write,
        "products_tagged": products_tagged,
        "products_would_tag": products_would_tag,
        "negative_skipped": len(negative_skipped),
        "ambiguous_skipped": len(ambiguous_skipped),
        "duplicate_feed_skipped": len(duplicate_skipped),
        "unmatched": len(unmatched),
        "inventory_errors": inv_errors,
        "tag_errors": tag_errors,
        "sample_unmatched": unmatched[:25],
        "sample_ambiguous": ambiguous_skipped[:25],
        "sample_duplicate_feed_skipped": duplicate_skipped[:25],
    }
    with open(args.summary_out, "w") as f:
        json.dump(summary, f, indent=2)

    log("\n=== SUMMARY ===")
    log(json.dumps({k: v for k, v in summary.items() if not k.startswith("sample_")}, indent=2))

    if inv_errors or tag_errors:
        log("\nCompleted with errors - see summary JSON for details.")
        sys.exit(1)

    log(f"\nDone. Summary written to {args.summary_out}")


if __name__ == "__main__":
    main()
