###############################################################################
# create_master_reference.R
#
# Build a master company reference file by merging three data sources:
#   1. si_au_ref_names.csv       – SIRCA reference with Gcode (unique company
#                                   identifier through history) and MS IDs
#   2. Reference_SecurityReference-2025-12-merged.csv
#                                 – Morningstar security reference with
#                                   CompanyId, ShareClassId, ISIN, etc.
#   3. MasterCompany.csv         – ASX company master with ISIN, ABN, ACN,
#                                   GICS sector, address, trading status, etc.
#
# Merge strategy:
#   Step 1: si_au_ref_names LEFT JOIN SecurityReference
#           on MS_CompanyID = CompanyId AND MS_SecurityID = ShareClassId
#   Step 2: Result LEFT JOIN MasterCompany on ISIN
#   Step 3: For rows still unmatched on ISIN, attempt match on Symbol/Ticker
#   Step 4: Select non-redundant columns from each source, prefix where needed
#
# Output: master_company_reference.csv
#
# Uses only base R (no external packages required).
###############################################################################

cat("=== Building Master Company Reference File ===\n\n")

# ---------- 1. Read data -----------------------------------------------------
cat("Reading input files...\n")

sirca <- read.csv("si_au_ref_names.csv", stringsAsFactors = FALSE)
secref <- read.csv("Reference_SecurityReference-2025-12-merged.csv",
                    stringsAsFactors = FALSE)
master <- read.csv("MasterCompany.csv", stringsAsFactors = FALSE)

cat("  si_au_ref_names:    ", nrow(sirca), "rows x", ncol(sirca), "cols\n")
cat("  SecurityReference:  ", nrow(secref), "rows x", ncol(secref), "cols\n")
cat("  MasterCompany:      ", nrow(master), "rows x", ncol(master), "cols\n\n")

# ---------- 2. Standardise join keys -----------------------------------------

# Trim whitespace from ID columns to avoid join mismatches
sirca$MS_CompanyID  <- trimws(sirca$MS_CompanyID)
sirca$MS_SecurityID <- trimws(sirca$MS_SecurityID)
secref$CompanyId    <- trimws(secref$CompanyId)
secref$ShareClassId <- trimws(secref$ShareClassId)
secref$ISIN         <- trimws(secref$ISIN)
master$ISIN         <- trimws(master$ISIN)

# Set empty strings to NA for clean joins
sirca$MS_CompanyID[sirca$MS_CompanyID == ""]   <- NA
sirca$MS_SecurityID[sirca$MS_SecurityID == ""] <- NA
secref$ISIN[secref$ISIN == ""]                 <- NA
master$ISIN[master$ISIN == ""]                 <- NA

# ---------- 3. Step 1: Join SIRCA to SecurityReference -----------------------
cat("Step 1: Joining si_au_ref_names to SecurityReference\n")
cat("        on MS_CompanyID = CompanyId AND MS_SecurityID = ShareClassId...\n")

# Select columns from SecurityReference that add new information
# (exclude columns that are redundant with si_au_ref_names)
sr_cols <- c(
  "CompanyId", "ShareClassId",               # IDs (keep for reference)
  "ISIN", "CUSIP", "Valoren",                # Security identifiers
  "ExchangeId", "CurrencyId", "MIC",         # Exchange/currency info
  "IPODate", "IPOOfferPrice", "IPOOfferPriceRange",  # IPO details
  "IsDepositaryReceipt", "DepositaryReceiptRatio",   # DR info
  "SecurityType",                             # MS security type code
  "ShareClassDescription", "ShareClassStatus",
  "IsPrimaryShare", "IsDividendReinvest", "IsDirectInvest",
  "InvestmentId",
  "CommonShareSubType", "ExchangeSubMarketGlobalId",
  "ConversionRatio", "ActiveOrDelisted"
)
# Keep only columns that actually exist in secref
sr_cols <- sr_cols[sr_cols %in% names(secref)]

# Prefix SecurityReference columns with "SR_" to distinguish from SIRCA
sr_select <- secref[, sr_cols, drop = FALSE]
sr_prefixed_names <- paste0("SR_", sr_cols)
names(sr_select) <- sr_prefixed_names

# Add join keys (unprefixed) for the merge
sr_select$join_CompanyId    <- secref$CompanyId
sr_select$join_ShareClassId <- secref$ShareClassId

# Perform left join: keep all SIRCA rows
# Create composite join keys
sirca$join_key <- paste0(sirca$MS_CompanyID, "|", sirca$MS_SecurityID)
sr_select$join_key <- paste0(sr_select$join_CompanyId, "|",
                              sr_select$join_ShareClassId)

# Deduplicate SecurityReference by join_key (keep first occurrence)
sr_dedup <- sr_select[!duplicated(sr_select$join_key), ]

# Use match for efficient left join
match_idx <- match(sirca$join_key, sr_dedup$join_key)

# Bind SecurityReference columns to SIRCA
merged <- sirca
for (col in sr_prefixed_names) {
  merged[[col]] <- sr_dedup[[col]][match_idx]
}

# Count matches
n_step1 <- sum(!is.na(match_idx))
cat("  Matched:", n_step1, "of", nrow(sirca), "SIRCA rows\n")
cat("  Unmatched:", sum(is.na(match_idx)), "rows (no MS_CompanyID/SecurityID)\n\n")

# Clean up temp columns
merged$join_key <- NULL

# ---------- 4. Step 2: Join to MasterCompany on ISIN ------------------------
cat("Step 2: Joining to MasterCompany on ISIN...\n")

# Select unique/useful columns from MasterCompany
# (exclude columns that are redundant with SIRCA or SecurityReference)
mc_cols <- c(
  "ISIN",                                     # Join key
  "Identifier",                               # ASX identifier code
  "ACN", "ABN",                               # Business numbers
  "HomeMarket",
  "GICSSector", "GICSIndGrp",                 # GICS at sector/ind group level
  "ASXSector", "ASXSubSector",                # ASX sector classification
  "FormerNames",                              # Historical name changes
  "TradingStatus",                            # Current trading status
  "ListDate", "DelistDate", "DelistReason",   # MC list/delist info
  "Address", "City", "State", "Postcode",     # Location
  "PhoneNumber", "FaxNumber",
  "WebAddress", "EmailAddress",
  "RegistryId", "Template", "ASX300"          # Other useful flags
)
mc_cols <- mc_cols[mc_cols %in% names(master)]

mc_select <- master[, mc_cols, drop = FALSE]

# Prefix MasterCompany columns with "MC_"
mc_prefixed_names <- paste0("MC_", mc_cols)
names(mc_select) <- mc_prefixed_names

# Deduplicate MasterCompany by ISIN (keep first occurrence)
mc_select$join_ISIN <- master$ISIN
mc_dedup <- mc_select[!is.na(mc_select$join_ISIN) &
                        !duplicated(mc_select$join_ISIN), ]

# Match on ISIN from the SecurityReference join (SR_ISIN column)
match_isin <- match(merged$SR_ISIN, mc_dedup$join_ISIN)

# Bind MasterCompany columns
for (col in mc_prefixed_names) {
  merged[[col]] <- mc_dedup[[col]][match_isin]
}

n_step2 <- sum(!is.na(match_isin))
cat("  Matched by ISIN:", n_step2, "rows\n")

# ---------- 5. Step 3: Fallback match on Symbol for remaining rows -----------
cat("Step 3: Attempting Symbol-based match for rows without ISIN match...\n")

unmatched_isin <- which(is.na(match_isin))

if (length(unmatched_isin) > 0) {
  # Try matching CompanyTicker (from SIRCA) to Symbol (in MasterCompany)
  master$Symbol_upper <- toupper(trimws(master$Symbol))

  # Build a lookup from MasterCompany by Symbol (deduplicated)
  mc_by_symbol <- master[!duplicated(master$Symbol_upper) &
                           master$Symbol_upper != "", ]

  # Create prefixed version for symbol-matched rows
  mc_sym_select <- mc_by_symbol[, mc_cols[mc_cols %in% names(master)],
                                 drop = FALSE]
  mc_sym_prefixed <- paste0("MC_", names(mc_sym_select))
  names(mc_sym_select) <- mc_sym_prefixed
  mc_sym_select$join_Symbol <- mc_by_symbol$Symbol_upper

  # Match SIRCA CompanyTicker (uppercase) to MasterCompany Symbol
  merged$ticker_upper <- toupper(trimws(merged$CompanyTicker))
  sym_match <- match(merged$ticker_upper[unmatched_isin],
                      mc_sym_select$join_Symbol)

  n_sym_matched <- sum(!is.na(sym_match))

  if (n_sym_matched > 0) {
    matched_rows <- unmatched_isin[!is.na(sym_match)]
    mc_matched <- mc_sym_select[sym_match[!is.na(sym_match)], , drop = FALSE]

    for (col in mc_sym_prefixed) {
      if (col %in% names(merged)) {
        merged[[col]][matched_rows] <- mc_matched[[col]]
      }
    }
  }

  cat("  Matched by Symbol:", n_sym_matched, "rows\n")
  merged$ticker_upper <- NULL
}

total_mc_matched <- sum(!is.na(merged$MC_ISIN))
cat("  Total MasterCompany matches:", total_mc_matched, "of", nrow(merged),
    "rows\n\n")

# ---------- 6. Organise final output columns ---------------------------------
cat("Organising output columns...\n")

# Column order:
# A) Core identifiers (Gcode, CompanyId, ShareClassId, ISIN, tickers)
# B) Company info from SIRCA (name, history, GICS industry)
# C) Security info from SIRCA (security type, dates)
# D) Security info from SecurityReference (share class, DR, IPO)
# E) Company info from MasterCompany (ABN, ACN, address, sectors, status)

output_cols <- c(
  # --- A: Core Identifiers ---
  "Gcode",
  "SR_CompanyId", "SR_ShareClassId",
  "SR_ISIN", "SR_CUSIP", "SR_Valoren",
  "CompanyTicker", "SecurityTicker",
  "MC_Identifier",
  "MA_Identifier",

  # --- B: Company Info (SIRCA) ---
  "FullCompanyName", "AbbrevCompanyName",
  "GICSIndustry",
  "SIRCAIndustryClassCode", "SIRCASectorCode",
  "EarliestListDate", "LatestDelistDate",
  "CompanyDelistReasonCode", "CompanyRelatedGCode",
  "CompanyDelistReasonComment",

  # --- C: Security Info (SIRCA) ---
  "SeniorSecurity", "SecurityType",
  "AbreviatedSecurityDescription",
  "ListDate_YMD", "DelistDate_YMD",
  "ListDate", "DelistDate",
  "RecordCount",
  "AlteredLink",

  # --- D: Security Info (SecurityReference) ---
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

  # --- E: Company Info (MasterCompany) ---
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

  # --- F: Internal IDs (kept for traceability) ---
  "MS_CompanyID", "MS_SecurityID",
  "MS_CompanyID2", "MS_SecurityID2"
)

# Keep only columns that exist in the merged data
output_cols <- output_cols[output_cols %in% names(merged)]

# Also include any SIRCA columns we may have missed
sirca_remaining <- setdiff(names(sirca), c(output_cols, "join_key",
                                            "ListDate_DaysSince",
                                            "DelistDate_DaysSince"))
if (length(sirca_remaining) > 0) {
  cat("  Note: Including additional SIRCA columns:",
      paste(sirca_remaining, collapse = ", "), "\n")
  output_cols <- c(output_cols, sirca_remaining)
  output_cols <- output_cols[output_cols %in% names(merged)]
}

output <- merged[, output_cols, drop = FALSE]

# ---------- 7. Summary statistics --------------------------------------------
cat("\n=== Summary ===\n")
cat("Total rows in master reference:", nrow(output), "\n")
cat("Total columns:", ncol(output), "\n\n")

# Gcode coverage
n_gcodes <- length(unique(output$Gcode))
cat("Unique Gcodes (companies through history):", n_gcodes, "\n")

# Match rates
cat("Rows with SecurityReference data:", sum(!is.na(output$SR_CompanyId)),
    sprintf("(%.1f%%)\n", 100 * mean(!is.na(output$SR_CompanyId))))
cat("Rows with MasterCompany data:    ", sum(!is.na(output$MC_ACN) |
                                               !is.na(output$MC_ABN) |
                                               !is.na(output$MC_TradingStatus)),
    sprintf("(%.1f%%)\n", 100 * mean(!is.na(output$MC_ACN) |
                                       !is.na(output$MC_ABN) |
                                       !is.na(output$MC_TradingStatus))))
cat("Rows with ISIN:                  ", sum(!is.na(output$SR_ISIN) &
                                               output$SR_ISIN != ""),
    sprintf("(%.1f%%)\n", 100 * mean(!is.na(output$SR_ISIN) &
                                       output$SR_ISIN != "")))

# GICS coverage
cat("Rows with GICS Industry (8-digit):", sum(!is.na(output$GICSIndustry) &
                                                output$GICSIndustry != 0),
    "\n")
cat("Rows with MC GICS Sector:         ", sum(!is.na(output$MC_GICSSector) &
                                                output$MC_GICSSector != ""),
    "\n")

# Trading status breakdown
if ("MC_TradingStatus" %in% names(output)) {
  cat("\nTrading Status (from MasterCompany):\n")
  status_tbl <- table(output$MC_TradingStatus, useNA = "ifany")
  print(status_tbl)
}

# Active/Delisted breakdown from SecurityReference
if ("SR_ActiveOrDelisted" %in% names(output)) {
  cat("\nActive/Delisted (from SecurityReference):\n")
  print(table(output$SR_ActiveOrDelisted, useNA = "ifany"))
}

# ---------- 8. Write output --------------------------------------------------
output_file <- "master_company_reference.csv"
write.csv(output, output_file, row.names = FALSE)
cat("\nOutput written to:", output_file, "\n")
cat("Done.\n")
