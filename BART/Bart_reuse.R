

# CLEAR ENVIRONMENT FIRST
rm(list = ls())


# 1. LOAD REQUIRED PACKAGES
cat("1. LOADING PACKAGES...\n")
library(dplyr)
library(bartCause)


# 2. IMPORT DATA
cat("2. IMPORTING DATA...\n")

filename <- "/Users/kekdiaz/Documents/GitHub/ML_Ops-Project2/Datagrip_Docker_Folder/scripts/ummc_exports/tableau_ummc_complete_data_20251005.csv"

head(filename)

tryCatch({
  df <- read.csv(filename, 
                 stringsAsFactors = FALSE,
                 na.strings = c("", "NA", "N/A", "NULL"),
                 strip.white = TRUE)
  cat(" Data loaded successfully from:", filename, "\n")
  cat(" Dimensions:", dim(df), "\n")
}, error = function(e) {
  cat(" Failed to load", filename, "\n")
  cat("   Error:", e$message, "\n")
  stop("Please fix the filename and run again")
})

# 3. INITIAL DATA CHECK
cat("\n3. INITIAL DATA CHECK:\n")
cat("   First 3 rows:\n")
print(head(df, 3))
cat("   Column names:", paste(names(df), collapse = ", "), "\n")

# 4. CLEAN DATA
cat("\n4. CLEANING DATA...\n")
original_rows <- nrow(df)

required_cols <- c("patient_race", "door_to_doctor_min", "patient_age", 
                   "patient_gender", "risk_score", "visit_date")

if(!"visit_date" %in% names(df)) {
  cat("'visit_date' column not found in data\n")
  cat("   Available columns:", paste(names(df), collapse = ", "), "\n")
  stop("Please check your column names and run again")
}

df_clean <- df %>% select(all_of(required_cols))

df_clean <- na.omit(df_clean)
cat(" Removed", original_rows - nrow(df_clean), "rows with NA values\n")

cat("  Extracting year from visit_date...\n")
df_clean <- df_clean %>%
  mutate(
    patient_race = as.character(patient_race),
    patient_gender = as.character(patient_gender),
    door_to_doctor_min = as.numeric(door_to_doctor_min),
    patient_age = as.numeric(patient_age),
    risk_score = as.numeric(risk_score),
    visit_date = as.character(visit_date)
  )

df_clean$year <- substr(df_clean$visit_date, 1, 4)
cat(" Extracted year from visit_date\n")

df_clean <- df_clean %>%
  filter(
    door_to_doctor_min >= 0 & door_to_doctor_min <= 240,
    patient_age >= 0 & patient_age <= 120,
    risk_score >= 0,
    year >= "2010" & year <= "2025"
  )
cat(" Removed impossible values\n")

cat(" Clean data dimensions:", dim(df_clean), "\n")

# 5. CREATE BINS
cat("\n5. CREATING BINS...\n")

df_clean$age_bin <- cut(df_clean$patient_age,
                        breaks = c(0, 18, 35, 50, 65, 120),
                        labels = c("0-18", "19-35", "36-50", "51-65", "65+"),
                        include.lowest = TRUE)

df_clean$is_female <- as.numeric(df_clean$patient_gender == "F")

risk_quantiles <- quantile(df_clean$risk_score, 
                           probs = c(0, 0.25, 0.5, 0.75, 1),
                           na.rm = TRUE)
df_clean$risk_bin <- cut(df_clean$risk_score,
                         breaks = risk_quantiles,
                         labels = c("Low", "Medium-Low", "Medium-High", "High"),
                         include.lowest = TRUE)

cat(" Created bins\n")

# 6. VERIFY CLEAN DATA
cat("\n6. VERIFYING CLEAN DATA...\n")

cat("   Data types:\n")
for(col in names(df_clean)) {
  cat("      ", col, ":", class(df_clean[[col]]), "\n")
}

cat("\n   Value ranges:\n")
cat("      door_to_doctor_min:", min(df_clean$door_to_doctor_min), "to", max(df_clean$door_to_doctor_min), "minutes\n")
cat("      patient_age:", min(df_clean$patient_age), "to", max(df_clean$patient_age), "years\n")
cat("      risk_score:", min(df_clean$risk_score), "to", max(df_clean$risk_score), "\n")

cat("\n   Sample sizes by race:\n")
race_counts <- table(df_clean$patient_race)
for(race in names(race_counts)) {
  cat("      ", race, ":", race_counts[race], "patients\n")
}

cat("\n   Sample sizes by year:\n")
year_counts <- table(df_clean$year)
for(yr in names(year_counts)) {
  cat("      ", yr, ":", year_counts[yr], "patients\n")
}

# 7. FINAL DATA QUALITY CHECK
cat("\n7. FINAL QUALITY CHECK...\n")

issues <- c()
if(any(is.na(df_clean))) issues <- c(issues, "NA values present")
if(nrow(df_clean) < 100) issues <- c(issues, "Sample too small")
if(min(race_counts) < 10) issues <- c(issues, "Some racial groups too small")

if(length(issues) == 0) {
  cat(" DATA READY FOR BART ANALYSIS!\n")
} else {
  cat("    Issues found:\n")
  for(issue in issues) cat("      -", issue, "\n")
  stop("Fix data issues before proceeding")
}

# 8. BART ANALYSIS FUNCTIONS
# 8. BART ANALYSIS FUNCTIONS
cat("\n8. RUNNING COMPLETE BART ANALYSES...\n")

run_bart_analysis <- function(treatment, outcome, confounders, data, sample_size = 2000, group_name = "Group") {
  set.seed(123)
  sample_data <- data %>% sample_n(min(sample_size, nrow(data)))
  
  complete_cases <- complete.cases(confounders) & !is.na(outcome) & !is.na(treatment)
  y <- outcome[complete_cases]
  z <- treatment[complete_cases]
  x <- confounders[complete_cases, ]
  
  cat("group_name", "analysis:\n")
  cat("      Sample:", length(y), "(", sum(z), "treatment,", sum(1-z), "control)\n")
  
  tryCatch({
    bart_fit <- bartc(response = y,
                      treatment = z,
                      confounders = x,
                      method.rsp = "bart",
                      method.trt = "bart",
                      n.samples = 1000,
                      n.burn = 200,
                      n.chains = 2,
                      n.trees = 200,
                      verbose = FALSE)
    
    # Get the summary - it returns a data frame with named columns
    summ <- summary(bart_fit)
    
    # PROPERLY EXTRACT THE VALUES FROM THE DATA FRAME
    ate_estimate <- as.numeric(summ$estimate)
    ci_lower_estimate <- as.numeric(summ$ci.lower)
    ci_upper_estimate <- as.numeric(summ$ci.upper)
    
    # For p-value, calculate from ATE samples
    ate_samples <- bart_fit$ATE
    p_value_estimate <- 2 * (1 - pnorm(abs(ate_estimate) / sd(ate_samples, na.rm = TRUE)))
    
    # Print results
    cat("ATE:", sprintf("%+0.2f", ate_estimate), "minutes\n")
    cat("      95% CI: [", sprintf("%+0.2f", ci_lower_estimate), ",", sprintf("%+0.2f", ci_upper_estimate), "]\n")
    cat("      p-value:", sprintf("%0.3f", p_value_estimate), "\n")
    
    return(list(
      ate = ate_estimate,           # Single numeric value
      ci_lower = ci_lower_estimate, # Single numeric value
      ci_upper = ci_upper_estimate, # Single numeric value
      p_value = p_value_estimate,   # Single numeric value
      n_treatment = sum(z),
      n_control = sum(1-z)
    ))
    
  }, error = function(e) {
    cat("Failed:", e$message, "\n")
    return(NA)
  })
}

# NEW FUNCTION: Run analysis for each race by year
run_analysis_by_race_and_year <- function(data) {
  results_list <- list()
  years <- unique(data$year)
  races <- unique(data$patient_race)
  
  for (current_year in years) {
    for (current_race in races) {
      cat("Analyzing", current_race, "in", current_year, "\n")
      
      year_race_data <- data[data$year == current_year & data$patient_race == current_race, ]
      
      if (nrow(year_race_data) < 50) {  # Lower threshold for year-race combinations
        cat("Sample too small (", nrow(year_race_data), "rows), skipping\n")
        next
      }
      
      # Compare this race vs all others in the same year
      treatment <- as.numeric(year_race_data$patient_race == current_race)
      outcome <- year_race_data$door_to_doctor_min
      
      confounders <- data.frame(
        age = year_race_data$patient_age,
        female = as.numeric(year_race_data$patient_gender == "F"),
        risk_score = year_race_data$risk_score
      )
      
      result <- run_bart_analysis(treatment, outcome, confounders, year_race_data, 
                                  group_name = paste(current_race, "in", current_year))
      
      if (!any(is.na(result))) {
        results_list[[paste(current_year, current_race, sep = "_")]] <- list(
          year = current_year,
          race = current_race,
          subgroup_type = "Year_Race",
          ate = result$ate,
          ci_lower = result$ci_lower,
          ci_upper = result$ci_upper,
          p_value = result$p_value,
          n_treatment = result$n_treatment,
          n_control = result$n_control,
          total_n = nrow(year_race_data)
        )
      }
    }
  }
  
  return(results_list)
}

# NEW FUNCTION: Run gender analysis by year
run_gender_analysis_by_year <- function(data) {
  results_list <- list()
  years <- unique(data$year)
  
  for (current_year in years) {
    cat(" Analyzing gender in", current_year, "\n")
    
    year_data <- data[data$year == current_year, ]
    
    if (nrow(year_data) < 100) {
      cat("Sample too small (", nrow(year_data), "rows), skipping\n")
      next
    }
    
    treatment_gender <- as.numeric(year_data$patient_gender == "F")
    outcome <- year_data$door_to_doctor_min
    
    confounders <- data.frame(
      age = year_data$patient_age,
      risk_score = year_data$risk_score,
      race_black = as.numeric(year_data$patient_race == "Black")
    )
    
    result <- run_bart_analysis(treatment_gender, outcome, confounders, year_data,
                                group_name = paste("Gender in", current_year))
    
    if (!any(is.na(result))) {
      results_list[[current_year]] <- list(
        year = current_year,
        subgroup_type = "Year_Gender",
        ate = result$ate,
        ci_lower = result$ci_lower,
        ci_upper = result$ci_upper,
        p_value = result$p_value,
        n_treatment = result$n_treatment,
        n_control = result$n_control,
        total_n = nrow(year_data)
      )
    }
  }
  
  return(results_list)
}

# RUN ALL ANALYSES


all_results <- list()

# A. RACE DISPARITIES (OVERALL)
cat("\nA. OVERALL RACIAL DISPARITIES:\n")
races <- unique(df_clean$patient_race)
race_results <- list()

for (race in races) {
  treatment <- as.numeric(df_clean$patient_race == race)
  outcome <- df_clean$door_to_doctor_min
  confounders <- data.frame(
    age = df_clean$patient_age,
    female = as.numeric(df_clean$patient_gender == "F"),
    risk_score = df_clean$risk_score
  )
  
  result <- run_bart_analysis(treatment, outcome, confounders, df_clean, 
                              group_name = paste("Race:", race))
  
  if (!any(is.na(result))) {
    race_results[[race]] <- list(
      analysis_type = "race_disparity",
      group = race,
      ate = result$ate,
      ci_lower = result$ci_lower,
      ci_upper = result$ci_upper,
      p_value = result$p_value,
      n_treatment = result$n_treatment,
      n_control = result$n_control
    )
  }
}

all_results$race <- race_results

# B. GENDER DISPARITIES (OVERALL)
cat("\nB. GENDER DISPARITIES:\n")
treatment_gender <- as.numeric(df_clean$patient_gender == "F")
outcome <- df_clean$door_to_doctor_min
confounders <- data.frame(
  age = df_clean$patient_age,
  risk_score = df_clean$risk_score,
  race_black = as.numeric(df_clean$patient_race == "Black")
)

gender_result <- run_bart_analysis(treatment_gender, outcome, confounders, df_clean,
                                   group_name = "Gender: Female vs Male")

if (!any(is.na(gender_result))) {
  all_results$gender <- list(
    analysis_type = "gender_disparity", 
    group = "Female_vs_Male",
    ate = gender_result$ate,
    ci_lower = gender_result$ci_lower,
    ci_upper = gender_result$ci_upper,
    p_value = gender_result$p_value,
    n_treatment = gender_result$n_treatment,
    n_control = gender_result$n_control
  )
}

# C. RACE DISPARITIES BY YEAR (FOR EACH RACE)
cat("\nC. RACE DISPARITIES BY YEAR (FOR EACH RACE):\n")
yearly_race_results <- run_analysis_by_race_and_year(df_clean)
all_results$by_year_race <- yearly_race_results

# D. GENDER DISPARITIES BY YEAR
cat("\nD. GENDER DISPARITIES BY YEAR:\n")
yearly_gender_results <- run_gender_analysis_by_year(df_clean)
all_results$by_year_gender <- yearly_gender_results

# 10 CREATE TABLEAU-READY CSV

tableau_data <- data.frame()

# SIMPLE VALUE EXTRACTION - NO MORE COMPLEX SAFE EXTRACTION NEEDED
extract_value <- function(value, default = NA) {
  if (is.null(value) || length(value) == 0) return(default)
  return(as.numeric(value))
}

# Add race disparity results (OVERALL)
cat("   📊 Compiling overall race disparities...\n")
if (!is.null(all_results$race)) {
  for (race_name in names(all_results$race)) {
    res <- all_results$race[[race_name]]
    
    new_row <- data.frame(
      analysis_type = "Race Disparity",
      subgroup_type = "Overall",
      subgroup = race_name,
      year = "All Years",  # Add year column
      group_comparison = paste(race_name, "vs Others"),
      ate = extract_value(res$ate),
      ci_lower = extract_value(res$ci_lower),
      ci_upper = extract_value(res$ci_upper),
      p_value = extract_value(res$p_value, 1.0),
      n_treatment = extract_value(res$n_treatment, 0),
      n_control = extract_value(res$n_control, 0),
      total_sample = extract_value(res$n_treatment, 0) + extract_value(res$n_control, 0),
      stringsAsFactors = FALSE
    )
    tableau_data <- rbind(tableau_data, new_row)
  }
}

# Add gender disparity results (OVERALL)
cat(" Compiling overall gender disparities...\n")
if (!is.null(all_results$gender)) {
  res <- all_results$gender
  
  new_row <- data.frame(
    analysis_type = "Gender Disparity",
    subgroup_type = "Overall",
    subgroup = "Overall",
    year = "All Years",  # Add year column
    group_comparison = "Female vs Male",
    ate = extract_value(res$ate),
    ci_lower = extract_value(res$ci_lower),
    ci_upper = extract_value(res$ci_upper),
    p_value = extract_value(res$p_value, 1.0),
    n_treatment = extract_value(res$n_treatment, 0),
    n_control = extract_value(res$n_control, 0),
    total_sample = extract_value(res$n_treatment, 0) + extract_value(res$n_control, 0),
    stringsAsFactors = FALSE
  )
  tableau_data <- rbind(tableau_data, new_row)
}

# Add race disparities by year (FOR EACH RACE)
cat("   📊 Compiling race disparities by year...\n")
if (!is.null(all_results$by_year_race)) {
  for (key in names(all_results$by_year_race)) {
    res <- all_results$by_year_race[[key]]
    
    new_row <- data.frame(
      analysis_type = "Race Disparity",
      subgroup_type = "Year_Race",
      subgroup = res$race,
      year = res$year,  # Actual year value
      group_comparison = paste(res$race, "vs Others in", res$year),
      ate = extract_value(res$ate),
      ci_lower = extract_value(res$ci_lower),
      ci_upper = extract_value(res$ci_upper),
      p_value = extract_value(res$p_value, 1.0),
      n_treatment = extract_value(res$n_treatment, 0),
      n_control = extract_value(res$n_control, 0),
      total_sample = extract_value(res$total_n, 0),
      stringsAsFactors = FALSE
    )
    tableau_data <- rbind(tableau_data, new_row)
  }
}

# Add gender disparities by year
cat("   Compiling gender disparities by year...\n")
if (!is.null(all_results$by_year_gender)) {
  for (year_name in names(all_results$by_year_gender)) {
    res <- all_results$by_year_gender[[year_name]]
    
    new_row <- data.frame(
      analysis_type = "Gender Disparity",
      subgroup_type = "Year_Gender",
      subgroup = "Overall",
      year = res$year,  # Actual year value
      group_comparison = paste("Female vs Male in", res$year),
      ate = extract_value(res$ate),
      ci_lower = extract_value(res$ci_lower),
      ci_upper = extract_value(res$ci_upper),
      p_value = extract_value(res$p_value, 1.0),
      n_treatment = extract_value(res$n_treatment, 0),
      n_control = extract_value(res$n_control, 0),
      total_sample = extract_value(res$total_n, 0),
      stringsAsFactors = FALSE
    )
    tableau_data <- rbind(tableau_data, new_row)
  }
}

# Add significance flags and visualization helpers
tableau_data$is_significant <- ifelse(tableau_data$p_value < 0.05, "Significant", "Not Significant")
tableau_data$ate_direction <- ifelse(tableau_data$ate > 0, "Positive", "Negative")
tableau_data$effect_size <- abs(tableau_data$ate)

# Save the main results
write.csv(tableau_data, "bart_analysis_results_for_tableau2.csv", row.names = FALSE)
cat(" Total rows exported:", nrow(tableau_data), "\n")

# Also save the cleaned dataset with year column for additional analysis
write.csv(df_clean, "cleaned_data_with_bart_results2.csv", row.names = FALSE)
cat(" Saved cleaned dataset to: cleaned_data_with_bart_results.csv\n")

# 11. FINAL STATUS
cat("\n==============================================================\n")
cat(" PIPELINE COMPLETE - ALL ANALYSES FINISHED!\n")
cat("   Files saved to:", getwd(), "\n")
cat("   Generated files:\n")
cat("   1. bart_analysis_results_for_tableau.csv - Main ATE results\n")
cat("   2. cleaned_data_with_bart_results.csv - Cleaned dataset\n")
cat("\n   Analysis types included:\n")
cat("   - Race disparities (overall)\n")
cat("   - Gender disparities (overall)\n")
cat("   - Race disparities by year (for each race)\n")
cat("   - Gender disparities by year\n")
cat("   - Year column added for filtering in Tableau\n")
cat("   - CI values properly included\n")
cat("==============================================================\n")
