#!/usr/bin/env python3
"""
generate_master_reference.py

Mirror of create_master_reference.R — builds master_company_reference.csv
by merging the three company reference files using only the Python stdlib.
"""

import csv
import sys

print("=== Building Master Company Reference File ===\n")

# ---------- 1. Read data -----------------------------------------------------
def read_csv(path):
    # Try utf-8 first, fall back to latin-1 for files with special characters
    for enc in ("utf-8-sig", "latin-1"):
        try:
            with open(path, newline="", encoding=enc) as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            return rows, reader.fieldnames
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Could not decode {path}")

print("Reading input files...")
sirca, sirca_cols = read_csv("si_au_ref_names.csv")
secref, secref_cols = read_csv("Reference_SecurityReference-2025-12-merged.csv")
master, master_cols = read_csv("MasterCompany.csv")

print(f"  si_au_ref_names:    {len(sirca)} rows x {len(sirca_cols)} cols")
print(f"  SecurityReference:  {len(secref)} rows x {len(secref_cols)} cols")
print(f"  MasterCompany:      {len(master)} rows x {len(master_cols)} cols")
print()

# ---------- 2. Standardise join keys -----------------------------------------
def clean(val):
    if val is None:
        return ""
    return val.strip()

for row in sirca:
    row["MS_CompanyID"] = clean(row.get("MS_CompanyID", ""))
    row["MS_SecurityID"] = clean(row.get("MS_SecurityID", ""))

for row in secref:
    row["CompanyId"] = clean(row.get("CompanyId", ""))
    row["ShareClassId"] = clean(row.get("ShareClassId", ""))
    row["ISIN"] = clean(row.get("ISIN", ""))

for row in master:
    row["ISIN"] = clean(row.get("ISIN", ""))
    row["Symbol"] = clean(row.get("Symbol", ""))

# ---------- 3. Step 1: Join SIRCA to SecurityReference -----------------------
print("Step 1: Joining si_au_ref_names to SecurityReference")
print("        on MS_CompanyID = CompanyId AND MS_SecurityID = ShareClassId...")

# Columns from SecurityReference to include
sr_cols_keep = [
    "CompanyId", "ShareClassId",
    "ISIN", "CUSIP", "Valoren",
    "ExchangeId", "CurrencyId", "MIC",
    "IPODate", "IPOOfferPrice", "IPOOfferPriceRange",
    "IsDepositaryReceipt", "DepositaryReceiptRatio",
    "SecurityType",
    "ShareClassDescription", "ShareClassStatus",
    "IsPrimaryShare", "IsDividendReinvest", "IsDirectInvest",
    "InvestmentId",
    "CommonShareSubType", "ExchangeSubMarketGlobalId",
    "ConversionRatio", "ActiveOrDelisted",
]
sr_cols_keep = [c for c in sr_cols_keep if c in secref_cols]

# Build lookup: (CompanyId, ShareClassId) -> first matching row
sr_lookup = {}
for row in secref:
    key = (row["CompanyId"], row["ShareClassId"])
    if key not in sr_lookup and key != ("", ""):
        sr_lookup[key] = row

n_step1 = 0
merged = []
for row in sirca:
    new_row = dict(row)
    key = (row["MS_CompanyID"], row["MS_SecurityID"])
    sr_row = sr_lookup.get(key) if key != ("", "") else None
    if sr_row:
        n_step1 += 1
        for col in sr_cols_keep:
            new_row[f"SR_{col}"] = sr_row.get(col, "")
    else:
        for col in sr_cols_keep:
            new_row[f"SR_{col}"] = ""
    merged.append(new_row)

print(f"  Matched: {n_step1} of {len(sirca)} SIRCA rows")
print(f"  Unmatched: {len(sirca) - n_step1} rows\n")

# ---------- 4. Step 2: Join to MasterCompany on ISIN ------------------------
print("Step 2: Joining to MasterCompany on ISIN...")

mc_cols_keep = [
    "ISIN",
    "Identifier",
    "ACN", "ABN",
    "HomeMarket",
    "GICSSector", "GICSIndGrp",
    "ASXSector", "ASXSubSector",
    "FormerNames",
    "TradingStatus",
    "ListDate", "DelistDate", "DelistReason",
    "Address", "City", "State", "Postcode",
    "PhoneNumber", "FaxNumber",
    "WebAddress", "EmailAddress",
    "RegistryId", "Template", "ASX300",
]
mc_cols_keep = [c for c in mc_cols_keep if c in master_cols]

# Build lookup: ISIN -> first matching row
mc_isin_lookup = {}
for row in master:
    isin = row["ISIN"]
    if isin and isin not in mc_isin_lookup:
        mc_isin_lookup[isin] = row

n_step2 = 0
for mrow in merged:
    sr_isin = mrow.get("SR_ISIN", "")
    mc_row = mc_isin_lookup.get(sr_isin) if sr_isin else None
    if mc_row:
        n_step2 += 1
        for col in mc_cols_keep:
            mrow[f"MC_{col}"] = mc_row.get(col, "")
    else:
        for col in mc_cols_keep:
            mrow[f"MC_{col}"] = ""

print(f"  Matched by ISIN: {n_step2} rows")

# ---------- 5. Step 3: Fallback match on Symbol for remaining rows -----------
print("Step 3: Attempting Symbol-based match for rows without ISIN match...")

# Build lookup: uppercase Symbol -> first matching row
mc_symbol_lookup = {}
for row in master:
    sym = row["Symbol"].upper()
    if sym and sym not in mc_symbol_lookup:
        mc_symbol_lookup[sym] = row

n_step3 = 0
for mrow in merged:
    # Only try symbol match if ISIN match didn't work
    if mrow.get("MC_ABN", "") or mrow.get("MC_ACN", "") or mrow.get("MC_TradingStatus", ""):
        continue
    ticker = clean(mrow.get("CompanyTicker", "")).upper()
    mc_row = mc_symbol_lookup.get(ticker) if ticker else None
    if mc_row:
        n_step3 += 1
        for col in mc_cols_keep:
            mrow[f"MC_{col}"] = mc_row.get(col, "")

print(f"  Matched by Symbol: {n_step3} rows")
total_mc = n_step2 + n_step3
print(f"  Total MasterCompany matches: {total_mc} of {len(merged)} rows\n")

# ---------- 6. Define output columns -----------------------------------------
print("Organising output columns...")

output_cols = [
    # Core Identifiers
    "Gcode",
    "SR_CompanyId", "SR_ShareClassId",
    "SR_ISIN", "SR_CUSIP", "SR_Valoren",
    "CompanyTicker", "SecurityTicker",
    "MC_Identifier",
    "MA_Identifier",

    # Company Info (SIRCA)
    "FullCompanyName", "AbbrevCompanyName",
    "GICSIndustry",
    "SIRCAIndustryClassCode", "SIRCASectorCode",
    "EarliestListDate", "LatestDelistDate",
    "CompanyDelistReasonCode", "CompanyRelatedGCode",
    "CompanyDelistReasonComment",

    # Security Info (SIRCA)
    "SeniorSecurity", "SecurityType",
    "AbreviatedSecurityDescription",
    "ListDate_YMD", "DelistDate_YMD",
    "ListDate", "DelistDate",
    "RecordCount",
    "AlteredLink",

    # Security Info (SecurityReference)
    "SR_ExchangeId", "SR_CurrencyId", "SR_MIC",
    "SR_SecurityType",
    "SR_ShareClassDescription", "SR_ShareClassStatus",
    "SR_IsPrimaryShare",
    "SR_IsDepositaryReceipt", "SR_DepositaryReceiptRatio",
    "SR_IPODate", "SR_IPOOfferPrice", "SR_IPOOfferPriceRange",
    "SR_IsDividendReinvest", "SR_IsDirectInvest",
    "SR_InvestmentId",
    "SR_CommonShareSubType", "SR_ExchangeSubMarketGlobalId",
    "SR_ConversionRatio", "SR_ActiveOrDelisted",

    # Company Info (MasterCompany)
    "MC_ACN", "MC_ABN",
    "MC_HomeMarket",
    "MC_GICSSector", "MC_GICSIndGrp",
    "MC_ASXSector", "MC_ASXSubSector",
    "MC_FormerNames",
    "MC_TradingStatus",
    "MC_ListDate", "MC_DelistDate", "MC_DelistReason",
    "MC_Address", "MC_City", "MC_State", "MC_Postcode",
    "MC_PhoneNumber", "MC_FaxNumber",
    "MC_WebAddress", "MC_EmailAddress",
    "MC_RegistryId", "MC_Template", "MC_ASX300",

    # Traceability IDs
    "MS_CompanyID", "MS_SecurityID",
    "MS_CompanyID2", "MS_SecurityID2",
]

# Filter to columns that actually exist in the data
available = set(merged[0].keys()) if merged else set()
output_cols = [c for c in output_cols if c in available]

# Check for any SIRCA columns we might have missed
all_output_set = set(output_cols)
skip = {"join_key", "ListDate_DaysSince", "DelistDate_DaysSince"}
for col in sirca_cols:
    if col not in all_output_set and col not in skip:
        print(f"  Note: Including additional SIRCA column: {col}")
        output_cols.append(col)

# ---------- 7. Write output --------------------------------------------------
output_file = "master_company_reference.csv"
with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=output_cols, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(merged)

# ---------- 8. Summary statistics --------------------------------------------
print(f"\n=== Summary ===")
print(f"Total rows in master reference: {len(merged)}")
print(f"Total columns: {len(output_cols)}")

gcodes = set(r["Gcode"] for r in merged)
print(f"\nUnique Gcodes (companies through history): {len(gcodes)}")

n_sr = sum(1 for r in merged if r.get("SR_CompanyId", ""))
n_mc = sum(1 for r in merged if r.get("MC_ABN", "") or r.get("MC_ACN", "") or r.get("MC_TradingStatus", ""))
n_isin = sum(1 for r in merged if r.get("SR_ISIN", ""))
print(f"Rows with SecurityReference data: {n_sr} ({100*n_sr/len(merged):.1f}%)")
print(f"Rows with MasterCompany data:     {n_mc} ({100*n_mc/len(merged):.1f}%)")
print(f"Rows with ISIN:                   {n_isin} ({100*n_isin/len(merged):.1f}%)")

# Trading status breakdown
from collections import Counter
status_counts = Counter(r.get("MC_TradingStatus", "") for r in merged)
print("\nTrading Status (from MasterCompany):")
for status, count in sorted(status_counts.items()):
    label = status if status else "(unmatched/empty)"
    print(f"  {label}: {count}")

active_counts = Counter(r.get("SR_ActiveOrDelisted", "") for r in merged)
print("\nActive/Delisted (from SecurityReference):")
for status, count in sorted(active_counts.items()):
    label = status if status else "(unmatched/empty)"
    print(f"  {label}: {count}")

print(f"\nOutput written to: {output_file}")
print("Done.")
