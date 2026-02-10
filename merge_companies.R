###############################################################################
# merge_companies.R
#
# Merge WgeaCompList (base list) with MasterComp (public listed firms).
# Strategy:
#   1. Exact match on company name (case-insensitive, trimmed)
#   2. Exact match on ABN
#   3. For unmatched rows, compute fuzzy name-matching score for manual review
#
# Output columns added to WgeaCompList:
#   - merged_by_name   : 1 if matched by exact company name, 0 otherwise
#   - merged_by_ABN    : 1 if matched by exact ABN, 0 otherwise
#   - matching_score   : best fuzzy string similarity score (0-1, 1 = perfect)
#                        computed for rows not matched by name or ABN
#   - MC_best_match_name : the best-matching MasterComp name (for manual review)
#   - All MasterComp columns are brought in with prefix "MC_"
#
# Uses only base R (no external packages required).
###############################################################################

# ---------- Helper: normalised Levenshtein similarity ------------------------
# adist() returns edit distance; we normalise by the max string length
# to get a similarity score between 0 and 1.
levenshtein_sim <- function(a, b_vec) {
  dists <- adist(a, b_vec)[1, ]
  max_lens <- pmax(nchar(a), nchar(b_vec))
  max_lens[max_lens == 0] <- 1  # avoid division by zero
  1 - dists / max_lens
}

# ---------- 1. Read data -----------------------------------------------------
wgea <- read.csv("WgeaCompList.csv", stringsAsFactors = FALSE)
master <- read.csv("MasterComp.csv", stringsAsFactors = FALSE)

cat("WGEA rows:", nrow(wgea), "\n")
cat("Master rows:", nrow(master), "\n")

# ---------- 2. Standardise keys ----------------------------------------------

# Company names: trim whitespace, lowercase for matching
wgea$name_key <- trimws(tolower(wgea$Company_Name))
master$name_key <- trimws(tolower(master$CompanyName))

# ABN: convert to character, set NA for missing
wgea$abn_key <- as.character(wgea$ABN)
master$abn_key <- as.character(master$ABN)
wgea$abn_key[is.na(wgea$abn_key) | wgea$abn_key == ""] <- NA
master$abn_key[is.na(master$abn_key) | master$abn_key == ""] <- NA

# ---------- 3. Prepare MasterComp columns for merge --------------------------
# Prefix master columns (except join keys) with "MC_"
mc_cols <- setdiff(names(master), c("name_key", "abn_key"))
mc_renamed <- paste0("MC_", mc_cols)
master_for_merge <- master[, mc_cols, drop = FALSE]
names(master_for_merge) <- mc_renamed
master_for_merge$name_key <- master$name_key
master_for_merge$abn_key <- master$abn_key

# ---------- 4. Step 1 – Exact name match ------------------------------------
cat("Step 1: Matching by exact company name (case-insensitive)...\n")

# Deduplicate master by name_key (keep first occurrence)
master_name <- master_for_merge[!duplicated(master_for_merge$name_key), ]

name_match <- merge(
  wgea, master_name,
  by = "name_key",
  all.x = TRUE,
  suffixes = c("", ".mc_name")
)

# Flag rows that matched by name
name_match$merged_by_name <- ifelse(!is.na(name_match$MC_CompanyName), 1L, 0L)

cat("  Matched by name:", sum(name_match$merged_by_name == 1), "rows\n")

# ---------- 5. Step 2 – ABN match for remaining rows ------------------------
cat("Step 2: Matching remaining rows by ABN...\n")

# Rows not yet matched
unmatched_idx <- which(name_match$merged_by_name == 0)

# Deduplicate master by abn_key (keep first, ignore NAs)
master_abn <- master_for_merge[!is.na(master_for_merge$abn_key) &
                                 !duplicated(master_for_merge$abn_key), ]

if (length(unmatched_idx) > 0) {
  unmatched <- name_match[unmatched_idx, ]

  abn_lookup <- match(unmatched$abn_key, master_abn$abn_key)
  matched_abn_idx <- which(!is.na(abn_lookup))

  if (length(matched_abn_idx) > 0) {
    master_rows <- master_abn[abn_lookup[matched_abn_idx], mc_renamed, drop = FALSE]

    # Fill in master columns for ABN-matched rows
    for (col in mc_renamed) {
      name_match[unmatched_idx[matched_abn_idx], col] <- master_rows[[col]]
    }
  }

  cat("  Matched by ABN:", length(matched_abn_idx), "rows\n")
} else {
  cat("  No unmatched rows remain after name matching.\n")
}

name_match$merged_by_ABN <- ifelse(
  name_match$merged_by_name == 0 & !is.na(name_match$MC_CompanyName), 1L, 0L
)

# ---------- 6. Step 3 – Fuzzy matching score for remaining unmatched rows ----
cat("Step 3: Computing fuzzy matching scores for unmatched rows...\n")

still_unmatched_idx <- which(name_match$merged_by_name == 0 &
                               name_match$merged_by_ABN == 0)

cat("  Unmatched rows to score:", length(still_unmatched_idx), "\n")

# Initialise matching_score and best-match columns
name_match$matching_score <- NA_real_
name_match$MC_best_match_name <- NA_character_

if (length(still_unmatched_idx) > 0) {
  # Get unique unmatched WGEA names to avoid redundant computation
  unmatched_names <- unique(name_match$name_key[still_unmatched_idx])
  master_names <- unique(master$name_key)

  cat("  Unique unmatched WGEA names:", length(unmatched_names), "\n")
  cat("  Master names to compare against:", length(master_names), "\n")

  # Store results in a named vector for fast lookup
  best_scores <- numeric(length(unmatched_names))
  best_matches <- character(length(unmatched_names))
  names(best_scores) <- unmatched_names
  names(best_matches) <- unmatched_names

  # Process one name at a time (adist is vectorised over the second argument)
  for (i in seq_along(unmatched_names)) {
    if (i %% 1000 == 1) {
      cat("    Processing name", i, "of", length(unmatched_names), "...\n")
    }

    sims <- levenshtein_sim(unmatched_names[i], master_names)
    best_j <- which.max(sims)
    best_scores[unmatched_names[i]] <- sims[best_j]
    best_matches[unmatched_names[i]] <- master_names[best_j]
  }

  # Map back to all unmatched rows
  for (idx in still_unmatched_idx) {
    nm <- name_match$name_key[idx]
    name_match$matching_score[idx] <- best_scores[nm]
    name_match$MC_best_match_name[idx] <- best_matches[nm]
  }
}

# ---------- 7. Clean up and write output -------------------------------------
cat("\nSummary:\n")
cat("  Total WGEA rows:", nrow(name_match), "\n")
cat("  Matched by name:", sum(name_match$merged_by_name == 1), "\n")
cat("  Matched by ABN:", sum(name_match$merged_by_ABN == 1), "\n")
cat("  Unmatched (with fuzzy score):", sum(!is.na(name_match$matching_score)), "\n")

# Drop internal join keys
name_match$name_key <- NULL
name_match$abn_key <- NULL
if ("abn_key.mc_name" %in% names(name_match)) {
  name_match$abn_key.mc_name <- NULL
}

# Reorder: original WGEA cols first, then merge flags, then master cols
wgea_orig_cols <- c("Year", "Company_Name", "ABN", "ANZSIC_Code")
flag_cols <- c("merged_by_name", "merged_by_ABN", "matching_score",
               "MC_best_match_name")
master_info_cols <- mc_renamed
all_cols <- c(wgea_orig_cols, flag_cols, master_info_cols)
all_cols <- all_cols[all_cols %in% names(name_match)]
name_match <- name_match[, all_cols]

# Write output
output_file <- "WgeaCompList_merged.csv"
write.csv(name_match, output_file, row.names = FALSE)
cat("\nOutput written to:", output_file, "\n")
