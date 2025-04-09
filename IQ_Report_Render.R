## This file executes IQ_Report.Rmd to render PDF reports detailing data that has
## been flagged for falling outside of IQ range, as determined dynamically based on the data

library(tidyverse)
library(data.table)
library(doFuture)
library(lubridate)
library(stringr)
library(openxlsx)
library(tictoc)
# Libraries for Rmd
library(knitr)
library(data.table)
library(dplyr)
library(lubridate)
library(ggplot2)
library(ggpubr)
library(scales)
library(EnvStats)
library(tidyr)
library(kableExtra)
library(tidyverse)
library(stringr)
library(glue)
library(plyr)
library(dplyr)
library(git2r)

# Set to TRUE to render IQ Reports for each habitat
render_reports <- TRUE

# import "seacar_data_location" variable which points to data directory
source("seacar_data_location.R")

# Create output path if it doesn't already exist
output_path <- c("output","output/data")
for (path in output_path) {
  if (!dir.exists(path)){dir.create(path, recursive = TRUE)}
}

#List data files
seacardat <- list.files(seacar_data_location, full.names = TRUE, pattern=".txt")

# Water Column habitat contains Nekton, discrete & continuous Water Clarity & Nutrient data
# Create list of WC NUT files
wq_files <- seacardat[str_detect(seacardat, "Combined_WQ_WC_NUT_")]
wq_disc_files <- str_subset(wq_files, "_cont_", negate=TRUE)
wq_cont_files <- str_subset(wq_files, "_cont_")
nekton_file <- str_subset(seacardat, "All_NEKTON")

#Set quantile thresholds for flagging "questionable" values
quant_low <- 0.001
quant_high <- 0.999
num_sds <- 3

# any parameters to skip in report?
parstoskip <- c("")

#What are the strings that need to be interpreted as NA values?
nas <- c("NULL", "NA", "")

reffilepath <- "output/ScriptResults/Database_Thresholds.xlsx"
ref_parameters <- setDT(read.xlsx(reffilepath, sheet = 1, startRow = 7))

# Make copy of original ref_parameters file
ref_parameters_original <- copy(ref_parameters)
ref_parameters <- ref_parameters[IndicatorName!="Acreage", ]

win_threshold_path <- "http://publicfiles.dep.state.fl.us/DEAR/WIN/MDQS/4_Activity_Result_WIN_Standards.xlsx"

#### Update WIN Thresholds to latest version ----
win_thresholds <- setDT(read.xlsx(win_threshold_path, sheet = "RangeCheck(Matrix-238)", startRow = 3))
win_thresholds <- win_thresholds[Matrix=="AQUEOUS-Surface Water"]

# Value imputation must account for when value==0, and convert to +/-0.000001
impute_val <- function(val, thres){
  if(thres=="low"){
    ifelse(val==0, -0.000001, val)
  } else if(thres=="high"){
    ifelse(val==0, 0.000001, val)
  }
}

# Initialize a data.table to store the differences
threshold_changes <- data.table(ParameterName = character(), 
                                OldLowThreshold = numeric(), NewLowThreshold = numeric(),
                                OldHighThreshold = numeric(), NewHighThreshold = numeric(),
                                change = logical())

# Mapping of ParameterName to AnalyteName and thresholds
threshold_mapping <- list(
  list(param = "Dissolved Oxygen", analyte = "Dissolved Oxygen"),
  list(param = "Dissolved Oxygen Saturation", analyte = "Dissolved oxygen saturation"),
  list(param = "pH", analyte = "pH"),
  list(param = "Specific Conductivity", analyte = "Specific Conductance", factor = 1000),
  list(param = "Water Temperature", analyte = "Temperature, Water", sheet = "RangeCheck(ActivityType-237)", activity_type = "Field")
)

# Function to get the thresholds and log changes if they differ
get_thresholds_and_log_changes <- function(param_info) {
  factor <- param_info$factor %||% 1 # Default factor is 1 if not specified
  sheet <- param_info$sheet %||% NULL
  activity_type <- param_info$activity_type %||% NULL
  
  if (!is.null(sheet)) {
    win_thresholds_wt <- setDT(read.xlsx(win_threshold_path, sheet = sheet, startRow = 3))
    thresholds <- win_thresholds_wt[AnalyteName == param_info$analyte & Activity.Type == activity_type]
  } else {
    thresholds <- win_thresholds[AnalyteName == param_info$analyte]
  }
  
  # Impute values
  imputed_low <- impute_val(thresholds[, Lowest.Allowable.Threshold] / factor, "low")
  imputed_high <- impute_val(thresholds[, Highest.Allowable.Threshold] / factor, "high")
  
  # Extract the old values from ref_parameters
  old_low <- ref_parameters[CombinedTable == "Discrete WQ" & ParameterName == param_info$param, LowThreshold]
  old_high <- ref_parameters[CombinedTable == "Discrete WQ" & ParameterName == param_info$param, HighThreshold]
  
  # Determine if there's a change in either threshold
  change_flag <- (!is.na(old_low) && old_low != imputed_low) || (!is.na(old_high) && old_high != imputed_high)
  
  # Log all entries and mark if there was a change
  threshold_changes <<- rbind(threshold_changes, 
                              data.table(ParameterName = param_info$param, 
                                         OldLowThreshold = old_low, NewLowThreshold = imputed_low,
                                         OldHighThreshold = old_high, NewHighThreshold = imputed_high,
                                         change = change_flag))
  
  list(low = imputed_low, high = imputed_high)
}

# Apply the thresholds and log changes once per ParameterName
for (param_info in threshold_mapping) {
  thresholds <- get_thresholds_and_log_changes(param_info)
  
  # Update both LowThreshold and HighThreshold in a single call
  ref_parameters[CombinedTable == "Discrete WQ" & ParameterName == param_info$param, 
                 `:=` (LowThreshold = thresholds$low, HighThreshold = thresholds$high)]
}

# Continue set-up ----

#Specify GitHub user info
github_user = "tylerhill122"
github_email = "tyler.hill@floridadep.gov"

#Get current script git commit and path, and create a version label
scriptpath <- rstudioapi::getSourceEditorContext()$path
scriptname <- str_sub(scriptpath, max(str_locate_all(scriptpath, "/")[[1]]) + 1, -1)
gitcommit_script <- system(paste0("git rev-list HEAD -1 ",scriptname), intern=TRUE) #NOTE: this command only looks within the current branch (assumes the user is already using 'main').
scriptversion <- paste0(scriptname, ", Git Commit ID: ", gitcommit_script)

#Get latest git commit ID
gitcommit <- system("git rev-parse HEAD", intern=TRUE)

table_template <- function(){
  return(
    data.table(
      ParameterID = numeric(),
      ParameterName = character(),
      ParameterUnits = character(),
      IndicatorID = numeric(),
      IndicatorName = character(),
      Habitat = character(),
      ThresholdID = numeric(),
      sub_parameter = character(),
      q_low = integer(),
      q_high = integer(),
      mean = integer(),
      n_tot = integer(),
      n_q_low = integer(),
      n_q_high = integer(),
      QuadSize_m2 = integer()
    )
  )
}

# Empty frames/list to store results
data_directory <- list()
qs <- table_template()
wq_qs <- table_template()

# Filter by species group? FOR CORAL ONLY - Indicator/Parameter SG1 combos
species_group_filtering <- TRUE

# List to store flagged data for Habitat Water Column
wq_flagged_data_list <- list()

# Select which continuous parameters to include:
all_params <- c(
  "Dissolved Oxygen",
  "Dissolved Oxygen Saturation",
  "pH",
  "Salinity",
  "Turbidity",
  "Water Temperature"
)

# Select which regions to include Continuous data for
regions <- c(
  "NE",
  "NW",
  "SE",
  "SW"
)

# list of habitats to generate reports for
habitats <- unique(ref_parameters$Habitat)
# subset for a given report
# habitats <- c("Submerged Aquatic Vegetation")

# Loop through each habitat ----
tic()
for (h in habitats){
  
  if(h=="Water Column"){
    
    # list to store shortened file names to display in report
    file_short_list <- list()
    water_column_summary_directory <- list()
    program_counts <- data.table()
    
    # Check if Nekton file is present, add to report
    if(length(nekton_file)>0){
      type_name <- "Nekton"
      file <- nekton_file
      
      # Record shortened file name
      file_short <- tail(str_split(file, "/")[[1]], 1)
      file_short_list[[type_name]] <- file_short
      
      qs_dat <- table_template()
      
      data <- fread(file, sep='|', na.strings = nas)
      data <- data[Include==1 & MADup==1 & !is.na(ResultValue) & 
                     SpeciesGroup1 %in% c("Grazers and reef dependent species", "Reef fish"), ]
      
      # Record parameter name and units
      param_ids <- unique(data$ParameterID)
      param_names <- unique(data$ParameterName)
      i <- unique(data$IndicatorName)
      i_id <- unique(data$IndicatorID)
      
      # Record data totals by parameter
      p_count <- data %>%
        dplyr::group_by(ProgramID, ParameterName) %>%
        dplyr::summarise(n_tot = n(), .groups = "keep")
      p_count$typeName <- type_name
      
      program_counts <- bind_rows(program_counts, p_count)
      
      # Nekton Processing
      for (p_id in param_ids){
        
        p <- ref_parameters[ParameterID==p_id & IndicatorID==i_id, ParameterName]
        threshold_id <- ref_parameters[ParameterID==p_id & CombinedTable==type_name, 
                                       ThresholdID]
        
        if(p %in% parstoskip){next}
        
        dat_par <- data[ParameterName==p, 
                        .(ParameterID = p_id,
                          ParameterName = p,
                          ParameterUnits = NA,
                          IndicatorID = i_id,
                          IndicatorName = i,
                          Habitat = h,
                          ThresholdID = threshold_id,
                          q_low = quantile(ResultValue, probs = quant_low),
                          q_high = quantile(ResultValue, probs = quant_high),
                          mean = mean(ResultValue),
                          n_tot = length(ResultValue))] %>% unique()
        
        # pull high and low quantiles for filtering
        quant_low_value <- dat_par$q_low
        quant_high_value <- dat_par$q_high
        
        # grab subset of data that falls below quantile limit
        subset_low <- data[ParameterName==p & ResultValue < quant_low_value, ]
        subset_low$q_subset <- "low"
        
        # grab subset of data that falls above quantile limit
        subset_high <- data[ParameterName==p & ResultValue > quant_high_value, ]
        subset_high$q_subset <- "high"
        
        # combine datasets for display in report
        combined_subset <- bind_rows(subset_low, subset_high)
        
        # Append the combined data frame to the result list
        wq_flagged_data_list[[type_name]][[i]][[p]] <- combined_subset
        
        # Add n_q_low and n_q_high to dat_par table
        dat_par$n_q_low <- nrow(subset_low)
        dat_par$n_q_high <- nrow(subset_high)
        dat_par[ , c('sub_parameter', 'QuadSize_m2')] = NA
        
        qs_dat <- rbind(qs_dat, dat_par)
        
        print(paste0(p, " sequencing complete"))
        
      }
      
      # Record overall results
      wq_qs <- rbind(wq_qs, qs_dat)
      
      # File into directory to display summaries for each Indicator
      water_column_summary_directory[[type_name]] <- qs_dat
      
      print(paste0(file_short, " export done"))
    }
    
    # Check if WQ_Disc files are present, add to report
    if(length(wq_disc_files)>0){
      # DISCRETE
      type_name <- "Discrete WQ"
      
      qs_dat <- table_template()
      
      # Discrete processing
      for (file in wq_disc_files){
        
        # shortened file name
        file_short <- tail(str_split(file, "/")[[1]], 1)
        
        data <- fread(file, sep='|', na.strings = nas)
        data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
        
        param_id <- unique(data$ParameterID)
        param_name <- unique(data$ParameterName)
        param_units <- unique(data$ParameterUnits)
        
        # Update ParameterName and Units in ref_parameters
        # This maintains consistency with new exports
        ref_parameters[ParameterID==param_id, `:=` (ParameterName = param_name,
                                                    Units = param_units)]
        
        for (p in param_name){
          if(p %in% parstoskip){next}
          
          threshold_id <- ref_parameters[ParameterID==param_id & CombinedTable==type_name, 
                                         ThresholdID]
          # Set indicator name for each parameter (WC, WQ, NUT)
          i <- ref_parameters[ParameterID==param_id & CombinedTable==type_name, 
                              unique(IndicatorName)]
          i_id <- ref_parameters[ParameterID==param_id & CombinedTable==type_name, 
                                 unique(IndicatorID)]
          
          # If parameter is "Total Nitrogen", calculate quantiles/SDs separately for "uncalculated" records
          if(p == "Total Nitrogen"){
            
            # Record data totals by parameter (for both All & No Calc)
            p_count_all <- data %>%
              dplyr::group_by(ProgramID, ParameterName) %>%
              dplyr::summarise(n_tot = n(), .groups = "keep")
            p_count_all$typeName <- type_name
            
            program_counts <- bind_rows(program_counts, p_count_all)
            
            p_count_nocalc <- data[str_detect(SEACAR_QAQCFlagCode, "1Q", negate = TRUE), ] %>%
              dplyr::group_by(ProgramID, ParameterName) %>%
              dplyr::summarise(n_tot = n(), .groups = "keep")
            p_count_nocalc$typeName <- type_name
            
            program_counts <- bind_rows(program_counts, p_count_nocalc)
            
            dat_par_all <- data[ParameterName==p,
                                .(ParameterID = param_id,
                                  ParameterName = p,
                                  ParameterUnits = param_units,
                                  IndicatorID = i_id,
                                  IndicatorName = i,
                                  Habitat = h,
                                  ThresholdID = 21,
                                  q_low = quantile(ResultValue, probs = quant_low),
                                  q_high = quantile(ResultValue, probs = quant_high),
                                  mean = mean(ResultValue),
                                  n_tot = length(ResultValue))] %>% unique()
            dat_par_all[, sub_parameter := "All"]
            
            dat_par_nocalc <- data[ParameterName==p & str_detect(SEACAR_QAQCFlagCode, "1Q", negate = TRUE),
                                   .(ParameterID = param_id,
                                     ParameterName = p,
                                     ParameterUnits = param_units,
                                     IndicatorID = i_id,
                                     IndicatorName = i,
                                     Habitat = h,
                                     ThresholdID = 31,
                                     q_low = quantile(ResultValue, probs = quant_low),
                                     q_high = quantile(ResultValue, probs = quant_high),
                                     mean = mean(ResultValue),
                                     n_tot = length(ResultValue))] %>% unique()
            dat_par_nocalc[, sub_parameter := "Calculated"]
            
            dat_par <- rbind(dat_par_all, dat_par_nocalc)
            
            for (sub_param in unique(dat_par$sub_parameter)){
              
              if(sub_param == "Calculated"){
                sub_data <- data[str_detect(SEACAR_QAQCFlagCode, "1Q", negate = TRUE), ]
              } else {
                sub_data <- data
              }
              
              # pull high and low quantiles for filtering
              quant_low_value <- dat_par[sub_parameter==sub_param, q_low]
              quant_high_value <- dat_par[sub_parameter==sub_param, q_high]
              
              # grab subset of data that falls below quantile limit
              subset_low <- sub_data[ParameterName==p & ResultValue < quant_low_value, ]
              subset_low$q_subset <- "low"
              
              # grab subset of data that falls above quantile limit
              subset_high <- sub_data[ParameterName==p & ResultValue > quant_high_value, ]
              subset_high$q_subset <- "high"
              
              # combine datasets for display in report
              combined_subset <- bind_rows(subset_low, subset_high)
              combined_subset$sub_parameter <- sub_param
              
              # new combined parameter name (Total Nitrogen + sub_param)
              new_param_name <- paste0("Total Nitrogen (", sub_param, ")")
              combined_subset[sub_parameter==sub_param, `:=` (ParameterName = new_param_name)]
              
              # Append the combined data frame to the result list
              wq_flagged_data_list[[type_name]][[i]][[new_param_name]] <- combined_subset
              
              # Append file_short to include all file names for WQ
              file_short_list[[type_name]][[i]][[p]] <- file_short
              
              # Add n_q_low and n_q_high to dat_par table
              dat_par[sub_parameter==sub_param, `:=` (n_q_low = nrow(subset_low))]
              dat_par[sub_parameter==sub_param, `:=` (n_q_high = nrow(subset_high))]
              
              # Rename Total Nitrogen parameter to include Sub_parameter
              # dat_par[sub_parameter==sub_param, `:=` (parameter = paste0("Total Nitrogen (", sub_param, ")"))]
              
              dat_par[ , c('QuadSize_m2')] = NA
              
            }
            
          } else {
            
            # Record data totals by parameter
            p_count <- data %>%
              dplyr::group_by(ProgramID, ParameterName) %>%
              dplyr::summarise(n_tot = n(), .groups = "keep")
            p_count$typeName <- type_name
            
            program_counts <- bind_rows(program_counts, p_count)
            
            dat_par <- data[ParameterName==p & !is.na(ResultValue) & MADup == 1 & Include == 1,
                            .(ParameterID = param_id,
                              ParameterName = p,
                              ParameterUnits = param_units,
                              IndicatorID = i_id,
                              IndicatorName = i,
                              Habitat = h,
                              ThresholdID = threshold_id,
                              q_low = quantile(ResultValue, probs = quant_low),
                              q_high = quantile(ResultValue, probs = quant_high),
                              mean = mean(ResultValue),
                              n_tot = length(ResultValue))]
            
            # pull high and low quantiles for filtering
            quant_low_value <- dat_par$q_low
            quant_high_value <- dat_par$q_high
            
            # grab subset of data that falls below quantile limit
            subset_low <- data[ParameterName==p & ResultValue < quant_low_value, ]
            subset_low$q_subset <- "low"
            
            # grab subset of data that falls above quantile limit
            subset_high <- data[ParameterName==p & ResultValue > quant_high_value, ]
            subset_high$q_subset <- "high"
            
            # combine datasets for display in report
            combined_subset <- bind_rows(subset_low, subset_high)
            
            # Append the combined data frame to the result list
            wq_flagged_data_list[[type_name]][[i]][[p]] <- combined_subset
            
            # Append file_short to include all file names for WQ
            file_short_list[[type_name]][[i]][[p]] <- file_short
            
            # Add n_q_low and n_q_high to dat_par table
            dat_par$n_q_low <- nrow(subset_low)
            dat_par$n_q_high <- nrow(subset_high)
            dat_par[ , c('sub_parameter', 'QuadSize_m2')] = NA
            
            print(paste0(p, " sequencing complete"))
          }
          
          # append to make long-form table
          qs_dat <- rbind(qs_dat, dat_par)
        }
      }
      
      # Record overall results
      wq_qs <- rbind(wq_qs, qs_dat)
      
      # File into directory to display summaries for each Indicator
      water_column_summary_directory[[type_name]] <- wq_qs
      
      print(paste0(file_short, " export done"))
      
    }
    
    # Check if WQ_Cont files are present, add to report
    if(length(wq_cont_files)>0){
      # Continuous
      type_name <- "Continuous WQ"
      
      cont_dat <- table_template()
      
      for(p in all_params){
        
        print(paste0("Starting Continuous parameter: ", p))
        
        par_name <- str_replace_all(p," ","_")
        
        data_combined <- list()
        region_files <- list()
        
        for(region in regions){
          # Pattern used to locate correct Parameter / Region combination
          pattern <- paste0(par_name,"_",region)
          
          file <- str_subset(wq_cont_files, pattern)
          file_short <- tail(str_split(file, "/")[[1]], 1)
          
          # record short file names for display in report
          region_files <- c(region_files, file_short)
          
          # Read in data file
          data <- fread(file, sep='|', na.strings = nas)
          data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
          
          # Record region name as column "region"
          data$region <- region
          # Ensure ValueQualifier column is interpreted as numeric
          data$ValueQualifier <- as.numeric(data$ValueQualifier)
          
          # combine regional data sets for a given parameter
          data_combined <- bind_rows(data_combined, data)
        }
        
        param_id <- unique(data_combined$ParameterID)
        param_name <- unique(data_combined$ParameterName)
        param_units <- unique(data_combined$ParameterUnits)
        threshold_id <- ref_parameters[ParameterID==param_id & CombinedTable==type_name, 
                                       ThresholdID]
        
        # Set indicator name for each parameter (WC, WQ, NUT)
        i <- ref_parameters[ParameterID==param_id & CombinedTable==type_name, 
                            IndicatorName]
        i_id <- ref_parameters[ParameterID==param_id & CombinedTable==type_name, 
                               IndicatorID]
        
        # Record data totals by parameter
        p_count <- data_combined %>%
          dplyr::group_by(ProgramID, ParameterName) %>%
          dplyr::summarise(n_tot = n(), .groups = "keep")
        p_count$typeName <- type_name
        
        program_counts <- bind_rows(program_counts, p_count)
        
        # Append file_short to include all file names for WQ
        file_short_list[[type_name]][[i]][[p]] <- region_files
        
        dat_par <- data_combined[ParameterName==p,
                                 .(ParameterID = param_id,
                                   ParameterName = param_name,
                                   ParameterUnits = param_units,
                                   IndicatorID = i_id,
                                   IndicatorName = i,
                                   Habitat = h,
                                   ThresholdID = threshold_id,
                                   q_low = quantile(ResultValue, probs = quant_low),
                                   q_high = quantile(ResultValue, probs = quant_high),
                                   mean = mean(ResultValue),
                                   n_tot = length(ResultValue))]
        
        # pull high and low quantiles for filtering
        quant_low_value <- dat_par$q_low
        quant_high_value <- dat_par$q_high
        
        # grab subset of data that falls below quantile limit
        subset_low <- data_combined[ParameterName==p & ResultValue < quant_low_value, ]
        subset_low$q_subset <- "low"
        
        # grab subset of data that falls above quantile limit
        subset_high <- data_combined[ParameterName==p & ResultValue > quant_high_value, ]
        subset_high$q_subset <- "high"
        
        # combine datasets for display in report
        combined_subset <- bind_rows(subset_low, subset_high)
        
        # Append the flagged data to data directory
        wq_flagged_data_list[[type_name]][[i]][[p]] <- combined_subset
        
        # Add n_q_low and n_q_high to dat_par table
        dat_par$n_q_low <- nrow(subset_low)
        dat_par$n_q_high <- nrow(subset_high)
        
        # Record results
        cont_dat <- rbind(cont_dat, dat_par, fill=TRUE)
        
        print(paste0(p, " Continuous processing complete!"))
        
      }
      
      water_column_summary_directory[[type_name]] <- cont_dat
      
      # append to make long-form table
      wq_qs <- rbind(wq_qs, cont_dat)
    }
    
    # Combine all flagged data outputs for each indicator into single directory
    data_directory[[h]] <- wq_flagged_data_list
    
    # Report filename
    file_out <- paste0(gsub("/","_",gsub(" ", "_", h)), "_IQ_Report")
    
    # Render report
    if(render_reports){
      rmarkdown::render(input = "IQ_Report.Rmd",
                        output_format = "pdf_document",
                        output_file = paste0(file_out, ".pdf"),
                        output_dir = "output",
                        clean = TRUE)
      unlink(paste0("output/", file_out, ".md"))
    }
  }
  
  if(h=="Submerged Aquatic Vegetation"){
    file <- str_subset(seacardat, "All_SAV")
    
    # shortened filename for display in report
    file_short <- tail(str_split(file, "/")[[1]], 1)
    
    # get list of indicators within a given habitat
    indicators <- ref_parameters[Habitat==h, unique(IndicatorName)]
    
    # load in habitat data
    data <- fread(file, sep="|", na.strings = nas)
    data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
    
    qs_dat <- table_template()
    
    flagged_data_list <- list()
    
    for (i in indicators){
      
      # unique threshold_ids included for each indicator/habitat combo
      threshold_ids <- ref_parameters[Habitat==h & IndicatorName==i, unique(ThresholdID)]
      
      for(threshold_id in threshold_ids){
        isSpeciesSpecific <- ref_parameters[ThresholdID==threshold_id, isSpeciesSpecific]
        if(isSpeciesSpecific){
          sg1_include <- c("Seagrass","Total SAV")
        } else {
          sg1_include <- c("Macroalgae","Seagrass","Total SAV")
        }
        
        indicator_data <- data[SpeciesGroup1 %in% sg1_include, ]
        
        p <- ref_parameters[ThresholdID==threshold_id, ParameterName]
        param_id <- ref_parameters[ThresholdID==threshold_id, ParameterID]
        i_id <- ref_parameters[ThresholdID==threshold_id, IndicatorID]
        
        # Record summary for table overview
        dat_par <- indicator_data[ParameterName==p, 
                                  .(ParameterID = param_id,
                                    ParameterName = p,
                                    ParameterUnits = NA,
                                    IndicatorID = i_id,
                                    IndicatorName = i,
                                    Habitat = h,
                                    ThresholdID = threshold_id,
                                    q_low = quantile(ResultValue, probs = quant_low),
                                    q_high = quantile(ResultValue, probs = quant_high),
                                    mean = mean(ResultValue),
                                    n_tot = length(ResultValue))]
        
        # pull high and low quantiles for filtering
        quant_low_value <- dat_par$q_low
        quant_high_value <- dat_par$q_high
        
        # grab subset of data that falls below quantile limit
        subset_low <- indicator_data[ParameterName==p & ResultValue < quant_low_value, ]
        subset_low$q_subset <- "low"
        
        # grab subset of data that falls above quantile limit
        subset_high <- indicator_data[ParameterName==p & ResultValue > quant_high_value, ]
        subset_high$q_subset <- "high"
        
        # combine datasets for display in report
        combined_subset <- bind_rows(subset_low, subset_high)
        
        # Append data directory with included, excluded data
        flagged_data_list[[i]][[p]] <- combined_subset
        
        # Add n_q_low and n_q_high to dat_par table
        dat_par$n_q_low <- nrow(subset_low)
        dat_par$n_q_high <- nrow(subset_high)
        
        dat_par[ , c('sub_parameter', 'QuadSize_m2')] = NA
        
        # append to make long-form table
        qs_dat <- rbind(qs_dat, dat_par)
        
      }
    }
    
    # Record overall results
    qs <- rbind(qs, qs_dat)
    
    print(paste0(file_short, " export done"))
    
    data_directory[[h]] <- flagged_data_list
    
    # Report filename
    file_out <- paste0(gsub("/","_",gsub(" ", "_", h)), "_IQ_Report")
    
    # Render report
    if(render_reports){
      rmarkdown::render(input = "IQ_Report.Rmd",
                        output_format = "pdf_document",
                        output_file = paste0(file_out, ".pdf"),
                        output_dir = "output",
                        clean = TRUE)
      unlink(paste0("output/", file_out, ".md"))      
    }
  }
  
  if(h=="Oyster/Oyster Reef"){
    file <- str_subset(seacardat, "All_OYSTER")
    
    # shortened filename for display in report
    file_short <- tail(str_split(file, "/")[[1]], 1)
    
    # get list of indicators within a given habitat
    indicators <- ref_parameters[Habitat==h, unique(IndicatorName)]
    
    # load in habitat data
    data <- fread(file, sep="|", na.strings = nas)
    ##### TEMPORARY
    data[ProgramID==4016 & Month==6 & Year==2024 & LiveDate_Qualifier=="Estimated", `:=` (LiveDate_Qualifier = "Estimate")]
    #####
    data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
    
    # Adjustments to quad size & other temporary fixes
    data[ProgramID == 4042 & is.na(QuadSize_m2), QuadSize_m2 := fcase(SampleDate == as_date("2014-06-11"), 1,
                                                                      SampleDate >= as_date("2014-11-11") & SampleDate <= as_date("2015-01-22"), 0.33,
                                                                      SampleDate >= as_date("2015-03-04"), 0.0625)]
    # ID_5025 Size Class data: quad sizes used to determine 2D surface area
    # 3D samples could be unrepresentative of numbers and size ranges of specimens from normal, surface quad sizes.
    data[ProgramID == 5035 & IndicatorName=="Size Class", QuadSize_m2 := NA]
    
    qs_dat <- table_template()
    
    flagged_data_list <- list()
    
    for (i in indicators){
      i_id <- ref_parameters[Habitat==h & IndicatorName==i, IndicatorID]
      
      # unique parameters included for each indicator/habitat combo
      threshold_ids <- ref_parameters[Habitat==h & IndicatorName==i, unique(ThresholdID)]
      
      qs_dat <- table_template()
      
      for(t_id in threshold_ids){
        p <- ref_parameters[ThresholdID==t_id, ParameterName]
        param_id <- ref_parameters[ThresholdID==t_id, ParameterID]
        
        if(p %in% c("Density","Reef Height","Percent Live")){
          # Record summary for table overview
          dat_par <- data[ParameterName==p, 
                          .(ParameterID = param_id,
                            ParameterName = p,
                            ParameterUnits = NA,
                            IndicatorID = i_id,
                            IndicatorName = i,
                            Habitat = h,
                            ThresholdID = t_id,
                            q_low = quantile(ResultValue, probs = quant_low),
                            q_high = quantile(ResultValue, probs = quant_high),
                            mean = mean(ResultValue),
                            n_tot = length(ResultValue))] %>% unique()
          
          # pull high and low quantiles for filtering
          quant_low_value <- dat_par$q_low
          quant_high_value <- dat_par$q_high
          
          # grab subset of data that falls below quantile limit
          subset_low <- indicator_data[ParameterName==p & ResultValue < quant_low_value, ]
          subset_low$q_subset <- "low"
          
          # grab subset of data that falls above quantile limit
          subset_high <- indicator_data[ParameterName==p & ResultValue > quant_high_value, ]
          subset_high$q_subset <- "high"
          
          # combine datasets for display in report
          combined_subset <- bind_rows(subset_low, subset_high)
          
          # Append data directory with included, excluded data
          flagged_data_list[[i]][[p]] <- combined_subset
          
          # Add n_q_low and n_q_high to dat_par table
          dat_par$n_q_low <- nrow(subset_low)
          dat_par$n_q_high <- nrow(subset_high)
          dat_par$QuadSize_m2 <- NA
          
          # append to make long-form table
          qs_dat <- rbind(qs_dat, dat_par, fill=TRUE)
        } else {
          quad_size <- ref_parameters[ThresholdID==t_id, unique(QuadSize_m2)]
          if(!is.na(quad_size)){
            filtered_data <- data[ParameterName==p & QuadSize_m2==quad_size, ]
            dat_par <- filtered_data[ ,
                                      .(ParameterID = param_id,
                                        ParameterName = p,
                                        ParameterUnits = NA,
                                        IndicatorID = i_id,
                                        IndicatorName = i,
                                        Habitat = h,
                                        ThresholdID = t_id,
                                        QuadSize_m2 = quad_size,
                                        q_low = quantile(ResultValue, probs = quant_low),
                                        q_high = quantile(ResultValue, probs = quant_high),
                                        mean = mean(ResultValue),
                                        n_tot = length(ResultValue))] %>% unique()
            
            # pull high and low quantiles for filtering
            quant_low_value <- dat_par$q_low
            quant_high_value <- dat_par$q_high
            
            # grab subset of data that falls below quantile limit
            subset_low <- filtered_data[ResultValue < quant_low_value, ]
            subset_low$q_subset <- "low"
            
            # grab subset of data that falls above quantile limit
            subset_high <- filtered_data[ResultValue > quant_high_value, ]
            subset_high$q_subset <- "high"
            
            # combine datasets for display in report
            combined_subset <- bind_rows(subset_low, subset_high)
            
            # New param name
            newParamName <- paste0(p,"(",quad_size,")")
            
            # Append data directory with included, excluded data
            flagged_data_list[[i]][[newParamName]] <- combined_subset
            
            # Add n_q_low and n_q_high to dat_par table
            dat_par$n_q_low <- nrow(subset_low)
            dat_par$n_q_high <- nrow(subset_high)
            
          } else {
            filtered_data <- data[ParameterName==p & is.na(QuadSize_m2), ]
            dat_par <- filtered_data[ ,
                                      .(ParameterID = param_id,
                                        ParameterName = p,
                                        IndicatorID = i_id,
                                        IndicatorName = i,
                                        Habitat = h,
                                        ThresholdID = t_id,
                                        QuadSize_m2 = NA,
                                        q_low = quantile(ResultValue, probs = quant_low),
                                        q_high = quantile(ResultValue, probs = quant_high),
                                        mean = mean(ResultValue),
                                        n_tot = length(ResultValue))] %>% unique()
            
            # pull high and low quantiles for filtering
            quant_low_value <- dat_par$q_low
            quant_high_value <- dat_par$q_high
            
            # grab subset of data that falls below quantile limit
            subset_low <- filtered_data[ResultValue < quant_low_value, ]
            subset_low$q_subset <- "low"
            
            # grab subset of data that falls above quantile limit
            subset_high <- filtered_data[ResultValue > quant_high_value, ]
            subset_high$q_subset <- "high"
            
            # combine datasets for display in report
            combined_subset <- bind_rows(subset_low, subset_high)
            
            # New param name
            newParamName <- paste0(p,"(",quad_size,")")
            
            # Append data directory with included, excluded data
            flagged_data_list[[i]][[newParamName]] <- combined_subset
            
            # Add n_q_low and n_q_high to dat_par table
            dat_par$n_q_low <- nrow(subset_low)
            dat_par$n_q_high <- nrow(subset_high)
            
          }
          
          # append to make long-form table
          qs_dat <- rbind(qs_dat, dat_par, fill=TRUE)
        }
      }
      
      # Record overall results
      qs <- rbind(qs, qs_dat, fill=TRUE)
    }
    
    print(paste0(file_short, " export done"))
    
    data_directory[[h]] <- flagged_data_list
    
    # Report filename
    file_out <- paste0(gsub("/","_",gsub(" ", "_", h)), "_IQ_Report")
    
    # Render report
    if(render_reports){
      rmarkdown::render(input = "IQ_Report.Rmd",
                        output_format = "pdf_document",
                        output_file = paste0(file_out, ".pdf"),
                        output_dir = "output",
                        clean = TRUE)
      unlink(paste0("output/", file_out, ".md"))      
    }
  }
  
  if(h=="Coastal Wetlands"){
    file <- str_subset(seacardat, "All_CW")
    
    # shortened filename for display in report
    file_short <- tail(str_split(file, "/")[[1]], 1)
    
    # get list of indicators within a given habitat
    indicators <- ref_parameters[Habitat==h, unique(IndicatorName)]
    
    # load in habitat data
    data <- fread(file, sep="|", na.strings = nas)
    data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
    
    qs_dat <- table_template()
    
    flagged_data_list <- list()
    
    for (i in indicators){
      
      # unique parameters included for each indicator/habitat combo
      parameters <- ref_parameters[Habitat==h & IndicatorName==i, unique(ParameterName)]
      
      #Sg1 for Coastal Wetlands
      sg1_include <- c("Mangroves and associates","Marsh","Invasives")
      
      for (p in parameters){
        
        if(p=="Total/Canopy Percent Cover"){
          indicator_data <- data
        } else {
          indicator_data <- data[SpeciesGroup1 %in% sg1_include, ]
        }
        
        # Grab relevant IDs for each parameter
        param_id <- ref_parameters[Habitat==h & ParameterName==p & IndicatorName==i, ParameterID]
        threshold_id <- ref_parameters[Habitat==h & ParameterName==p & IndicatorName==i, ThresholdID]
        i_id <- ref_parameters[Habitat==h & ParameterName==p & IndicatorName==i, IndicatorID]
        
        # Record summary for table overview
        dat_par <- indicator_data[ParameterName==p, 
                                  .(ParameterID = param_id,
                                    ParameterName = p,
                                    ParameterUnits = NA,
                                    IndicatorID = i_id,
                                    IndicatorName = i,
                                    Habitat = h,
                                    ThresholdID = threshold_id,
                                    q_low = quantile(ResultValue, probs = quant_low),
                                    q_high = quantile(ResultValue, probs = quant_high),
                                    mean = mean(ResultValue),
                                    n_tot = length(ResultValue))]
        
        # pull high and low quantiles for filtering
        quant_low_value <- dat_par$q_low
        quant_high_value <- dat_par$q_high
        
        # grab subset of data that falls below quantile limit
        subset_low <- indicator_data[ParameterName==p & ResultValue < quant_low_value, ]
        subset_low$q_subset <- "low"
        
        # grab subset of data that falls above quantile limit
        subset_high <- indicator_data[ParameterName==p & ResultValue > quant_high_value, ]
        subset_high$q_subset <- "high"
        
        # combine datasets for display in report
        combined_subset <- bind_rows(subset_low, subset_high)
        
        # Append data directory with included, excluded data
        flagged_data_list[[i]][[p]] <- combined_subset
        
        # Add n_q_low and n_q_high to dat_par table
        dat_par$n_q_low <- nrow(subset_low)
        dat_par$n_q_high <- nrow(subset_high)
        
        dat_par[ , c('sub_parameter', 'QuadSize_m2')] = NA
        
        # append to make long-form table
        qs_dat <- rbind(qs_dat, dat_par)
        
      }
    }
    
    # Record overall results
    qs <- rbind(qs, qs_dat)
    
    print(paste0(file_short, " export done"))
    
    data_directory[[h]] <- flagged_data_list
    
    # Report filename
    file_out <- paste0(gsub("/","_",gsub(" ", "_", h)), "_IQ_Report")
    
    # Render report
    if(render_reports){
      rmarkdown::render(input = "IQ_Report.Rmd",
                        output_format = "pdf_document",
                        output_file = paste0(file_out, ".pdf"),
                        output_dir = "output",
                        clean = TRUE)
      unlink(paste0("output/", file_out, ".md"))
    }
  }
  
  if(h=="Coral/Coral Reef"){
    
    file <- str_subset(seacardat, "All_CORAL")
    
    # shortened filename for display in report
    file_short <- tail(str_split(file, "/")[[1]], 1)
    
    # get list of indicators within a given habitat
    indicators <- ref_parameters[Habitat==h, unique(IndicatorName)]
    
    # load in habitat data
    # NOTE: Coral is imported without NA strings to allow "NULL" subsetting in SG1
    data <- fread(file, sep="|")
    data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
    
    qs_dat <- table_template()
    
    flagged_data_list <- list()
    
    for (i in indicators){
      
      # unique parameters included for each indicator/habitat combo
      parameters <- ref_parameters[Habitat==h & IndicatorName==i, unique(ParameterName)]
      
      # Filtering for SpeciesGroup1 with indicator/parameter combos
      if (i=="Grazers and Reef Dependent Species"){
        sg1_include <- c("Grazers and reef dependent species","Reef fish")
      }
      
      if (i=="Community Composition"){
        # SG1 for "Count", add additional for PA and Colony Density
        sg1_include <- c("Corallimorpharians", "Milleporans", "Octocorals", "Others", "Porifera", "Scleractinians", "NULL")
        if (p=="Presence/Absence"){
          sg1_include <- c(sg1_include, "Cyanobacteria", "Macroalgae", "Zoanthids")
        }
        if (p=="Colony Density"){
          sg1_include <- c(sg1_include, "Cyanobacteria", "Macroalgae", "Zoanthids", "Substrate")
        }
      }
      
      if (i == "Percent Cover"){
        # SG1 for "Colony..."
        sg1_include <- c("Corallimorpharians", "Milleporans", "Octocorals", "Others", "Porifera", "Scleractinians", "NULL")
        if (p=="Percent Cover"){
          sg1_include <- c(sg1_include, "Cyanobacteria", "Macroalgae", "Zoanthids", "Substrate", "Seagrass")
        }
        if (p=="Percent Live Tissue"){
          sg1_include <- c(sg1_include, "Zoanthids")
        }
        if (p=="Presence/Absence"){
          sg1_include <- "Seagrass"
        }
      }
      
      # filter data or not (TRUE/FALSE)
      if(species_group_filtering==TRUE){
        # Filter data for desired SpeciesGroup1
        indicator_data <- data[SpeciesGroup1 %in% sg1_include, ]
      } else {
        indicator_data <- data
      }
      
      for (p in parameters){
        
        # Grab relevant IDs for each parameter
        param_id <- ref_parameters[Habitat==h & ParameterName==p & IndicatorName==i, ParameterID]
        threshold_id <- ref_parameters[Habitat==h & ParameterName==p & IndicatorName==i, ThresholdID]
        i_id <- ref_parameters[Habitat==h & ParameterName==p & IndicatorName==i, IndicatorID]
        
        # Record summary for table overview
        dat_par <- indicator_data[ParameterName==p,
                                  .(ParameterID = param_id,
                                    ParameterName = p,
                                    ParameterUnits = NA,
                                    IndicatorID = i_id,
                                    IndicatorName = i,
                                    Habitat = h,
                                    ThresholdID = threshold_id,
                                    q_low = quantile(ResultValue, probs = quant_low),
                                    q_high = quantile(ResultValue, probs = quant_high),
                                    mean = mean(ResultValue),
                                    n_tot = length(ResultValue))]
        
        # pull high and low quantiles for filtering
        quant_low_value <- dat_par$q_low
        quant_high_value <- dat_par$q_high
        
        # grab subset of data that falls below quantile limit
        subset_low <- indicator_data[ParameterName==p & ResultValue < quant_low_value, ]
        subset_low$q_subset <- "low"
        
        # grab subset of data that falls above quantile limit
        subset_high <- indicator_data[ParameterName==p & ResultValue > quant_high_value, ]
        subset_high$q_subset <- "high"
        
        # combine datasets for display in report
        combined_subset <- bind_rows(subset_low, subset_high)
        
        # Append data directory with included, excluded data
        flagged_data_list[[i]][[p]] <- combined_subset
        
        # Add n_q_low and n_q_high to dat_par table
        dat_par$n_q_low <- nrow(subset_low)
        dat_par$n_q_high <- nrow(subset_high)
        
        dat_par[ , c('sub_parameter', 'QuadSize_m2')] = NA
        
        # append to make long-form table
        qs_dat <- rbind(qs_dat, dat_par)
        
      }
    }
    
    # Record overall results
    qs <- rbind(qs, qs_dat)
    
    print(paste0(file_short, " export done"))
    
    data_directory[[h]] <- flagged_data_list
    
    # Report filename
    file_out <- paste0(gsub("/","_",gsub(" ", "_", h)), "_IQ_Report")
    
    # Render report
    if(render_reports){
      rmarkdown::render(input = "IQ_Report.Rmd",
                        output_format = "pdf_document",
                        output_file = paste0(file_out, ".pdf"),
                        output_dir = "output",
                        clean = TRUE)
      unlink(paste0("output/", file_out, ".md"))
    }
  }
}
toc()

# Bind together all parameter results into single table
summary_table <- bind_rows(bind_rows(water_column_summary_directory) %>% unique(), 
                           qs %>% unique())

# Dataframe to use within add_buffer function below
# Adds a buffer of +/- 0.000001 to quantile values at high or low thresholds
buffered_params <- data.table(
  parameter = c("Braun Blanquet Score","Modified Braun Blanquet Score",
                "Presence/Absence","Percent Cover","Percent Occurrence",
                "Percent Live","Percent Live Tissue"),
  max_thresh = c(5,5,1,100,100,100,100),
  min_thresh = c(0,0,0,0,0,0,0))

# Function to provide buffer to selected values (listed above)
# i.e., alter Braun Blanquet q_high of 5 to 5.000001
add_buffer <- function(value, param, quantile){
  if(param %in% buffered_params$parameter){
    if(quantile=="high"){
      max_val <- buffered_params[parameter==param, max_thresh]
      new_val <- ifelse(value==max_val, value+0.000001, value)
    }
    return(new_val)
  } else {
    return(value)
  }
}

# Make a copy of summary_table
# This will act as the "new" results table for comparison below
# Prepare for display in excel workbook
qs2 <- summary_table %>%
  dplyr::rowwise() %>%
  dplyr::mutate(q_high = add_buffer(q_high, ParameterName, "high"),
                q_low = ifelse(q_low==0, -0.000001, q_low),
                sub_parameter = ifelse(ThresholdID==ref_parameters_original[Calculated==1, ThresholdID], 1, 0)) %>%
  dplyr::rename("LowQuantile" = "q_low",
                "HighQuantile" = "q_high",
                "Calculated" = "sub_parameter")
setDT(qs2)
setcolorder(qs2,c("ThresholdID", "ParameterID","Habitat","IndicatorID",
                  "IndicatorName","ParameterName","ParameterUnits","Calculated",
                  "LowQuantile","HighQuantile","mean","n_tot","n_q_low",
                  "n_q_high","QuadSize_m2"))
setorder(qs2, Habitat, IndicatorName, ParameterName, Calculated, QuadSize_m2)

# Output file names
rawoutputname <- "Database_Thresholds"
rawoutputextension <- ".xlsx"
output_file_date <- paste0("output/ScriptResults/",rawoutputname,"_",
                           str_replace_all(Sys.Date(), "-", ""), rawoutputextension)
output_file <- paste0("output/ScriptResults/",rawoutputname, rawoutputextension)

# QuantileSource text, capture git commit ID
qSourceText <- paste0(rawoutputname, rawoutputextension, ", Git Commit ID: ", gitcommit)

# New copy of original to update with values
updatedTable <- copy(ref_parameters_original)
updatedTable$ActionNeededDate <- as_date(updatedTable$ActionNeededDate, origin = "1899-12-30")
updatedTable$ScriptLatestRunDate <- as_date(updatedTable$ScriptLatestRunDate, origin = "1899-12-30")
updatedTable$QuantileDate <- as_date(updatedTable$QuantileDate, origin = "1899-12-30")

# Create table to compare current vs. previous quantile results
# Append "updatedTable" copy results, update where needed
results_table <- data.table()
for(t_id in unique(qs2$ThresholdID)){
  # Grab combined table name for display in overview
  combinedTable <- ref_parameters[ThresholdID==t_id, unique(CombinedTable)]
  
  # Original_subset and new_subset to compare old and new values
  original_subset <- ref_parameters_original[ThresholdID==t_id, ]
  new_subset <- qs2[ThresholdID==t_id, ]
  
  # Original ParameterName and units
  og_param <- unique(original_subset$ParameterName)
  original_param_units <- unique(original_subset$Units)
  
  # Original Quantile values, low and high
  original_q_low <- round(original_subset$LowQuantile,6)
  original_q_high <- round(original_subset$HighQuantile,6)
  # Original Threshold values, low and high
  original_t_low <- round(original_subset$LowThreshold,6)
  original_t_high <- round(original_subset$HighThreshold,6)
  
  # New ParameterName and units
  new_param <- unique(new_subset$ParameterName)
  new_param_units <- unique(new_subset$ParameterUnits)
  
  # New Quantile values, low and high
  new_q_low <- round(new_subset$LowQuantile,6)
  new_q_high <- round(new_subset$HighQuantile,6)
  
  # Grab new threshold values from "modified" ref_parameters
  # This accounts for the updated WIN thresholds
  new_t_low <- round(ref_parameters[ThresholdID==t_id, ]$LowThreshold,6)
  new_t_high <- round(ref_parameters[ThresholdID==t_id, ]$HighThreshold,6)
  
  # Update actionNeeded column
  actionNeeded <- ifelse(original_q_low!=new_q_low | original_q_high!=new_q_high | 
                           original_t_low!=new_t_low | original_t_high!=new_t_high, "U", NA)
  
  # Determine if names or units differ
  nameDifferent <- ifelse(og_param!=new_param, TRUE, FALSE)
  # variable to store new units if new, old units if not
  updatedUnits <- ifelse(is.na(new_param_units), original_param_units, new_param_units)
  # T/F value if units have changed
  unitDifference <- ifelse(original_param_units!=updatedUnits, TRUE, FALSE)
  
  # Create table to display change overview
  results_df <- data.table(
    "ThresholdID" = t_id,
    "CombinedTable" = combinedTable,
    "Habitat" = unique(new_subset$Habitat),
    "QuadSize_m2" = unique(new_subset$QuadSize_m2),
    "OriginalParameterName" = og_param,
    "NewParameterName" = new_param,
    "NameDifference" = nameDifferent,
    "OriginalParmaterUnits" = original_param_units,
    "NewParmaterUnits" = updatedUnits,
    "UnitDifference" = unitDifference,
    
    "OriginalLowQuantile" = original_q_low,
    "NewLowQuantile" = new_q_low,
    
    "OriginalHighQuantile" = original_q_high,
    "NewHighQuantile" = new_q_high,
    
    "OriginalLowThreshold" = original_t_low,
    "NewLowThreshold" = new_t_low,
    
    "OriginalHighThreshold" = original_t_high,
    "NewHighThreshold" = new_t_high,
    
    "ActionNeeded" = actionNeeded,
    "ActionNeededDate" = ifelse(!is.na(actionNeeded), Sys.Date(), NA),
    
    "mean" = new_subset$mean,
    "n_tot" = new_subset$n_tot,
    "n_q_low" = new_subset$n_q_low,
    "n_q_high" = new_subset$n_q_high
  )
  
  # Save change_overview information
  results_table <- bind_rows(results_table, results_df)
  
  # Append results to original table
  # Add new units
  updatedTable[
    ThresholdID==t_id, `:=` (
      ParameterName = ifelse(nameDifferent, new_param, ParameterName),
      Units = updatedUnits,
      ActionNeeded = actionNeeded,
      LowQuantile = ifelse(original_q_low!=new_q_low, new_q_low, LowQuantile),
      HighQuantile = ifelse(original_q_high!=new_q_high, new_q_high, HighQuantile),
      LowThreshold = ifelse(original_t_low!=new_t_low, new_t_low, LowThreshold),
      HighThreshold = ifelse(original_t_high!=new_t_high, new_t_high, HighThreshold),
      ActionNeededDate = ifelse(!is.na(actionNeeded), Sys.Date(), NA),
      QuantileDate = ifelse(!is.na(actionNeeded), Sys.Date(), QuantileDate),
      QuantileSource = ifelse(!is.na(actionNeeded), qSourceText, QuantileSource)
      )]
}
results_table$ActionNeededDate <- as.Date(results_table$ActionNeededDate)
updatedTable$QuantileDate <- as.Date(updatedTable$QuantileDate)
updatedTable$ActionNeededDate <- as.Date(updatedTable$ActionNeededDate)

# Remove BB & P/A from Quantiles
results_table[
  OriginalParameterName %in% c("Braun Blanquet Score", 
                               "Modified Braun Blanquet Score", 
                               "Presence/Absence"),
  `:=` (NewLowQuantile = NA, NewHighQuantile = NA, ActionNeeded = NA)]

# Setting headerstyle and creating "Difference Overview" workbook output
wb <- createWorkbook()
hs <- openxlsx::createStyle(textDecoration = "BOLD")
addWorksheet(wb, "Quantile Difference Overview")
writeDataTable(wb, 1, results_table, tableStyle = "TableStyleMedium2", headerStyle = hs)

# Apply condtional formatting to highlight UpdateNeeded column
updateStyle <- createStyle(fontColour = "#007B74", bgFill = "#C6EFCE")
conditionalFormatting(wb, "Quantile Difference Overview",
                      rows = 2:nrow(results_table), type="beginsWith",
                      cols = 19, rule = "U", style=updateStyle)
# Highlight New Quantiles High/Low
conditionalFormatting(wb, "Quantile Difference Overview",
                      rows = 2:nrow(results_table), type = "expression", 
                      cols = 12, rule = "K2<>L2", style = updateStyle)
conditionalFormatting(wb, "Quantile Difference Overview",
                      rows = 2:nrow(results_table), type = "expression", 
                      cols = 14, rule = "M2<>N2", style = updateStyle)
# Highlight if there is a difference in WIN Thresholds High/Low
newThreshStyle <- createStyle(fontColour = "#AD5724", bgFill = "#FFEB9C")
conditionalFormatting(wb, "Quantile Difference Overview",
                      rows = 2:nrow(results_table), type = "expression", 
                      cols = 18, rule = "R2<>Q2", style = newThreshStyle)
conditionalFormatting(wb, "Quantile Difference Overview",
                      rows = 2:nrow(results_table), type = "expression", 
                      cols = 16, rule = "P2<>O2", style = newThreshStyle)
# Save QuantileDifferenceOverview workbook
saveWorkbook(wb, paste0("output/ScriptResults/ChangeOverview/QuantileDifferenceOverview_",str_replace_all(Sys.Date(), "-", ""), ".xlsx"), TRUE)

# Grab list of Threshold IDs which are not included in Export tables
# but are included in Database_Thresholds 
idsNotInExport <- setdiff(updatedTable$ThresholdID, qs2$ThresholdID)
updatedTable[
  ThresholdID %in% idsNotInExport, `:=` 
  (LowQuantile = NA,
    HighQuantile = NA,
    ActionNeeded = NA,
    ActionNeededDate = NA,
    QuantileSource = ifelse(is.na(QuantileSource), NA, QuantileSource),
    AdditionalComments = paste0(Habitat, " - ", ParameterName, " not included in SEACAR data export tables"))]

# Exclude Braun Blanquet (+Modified) & Presence/Absence from Quantile results
updatedTable[ParameterName %in% c("Braun Blanquet Score", 
                                  "Modified Braun Blanquet Score", 
                                  "Presence/Absence"),
             `:=` (LowQuantile = NA,
                   HighQuantile = NA,
                   ActionNeeded = NA,
                   ActionNeededDate = NA,
                   QuantileSource = NA,
                   AdditionalComments = paste0(Habitat, " - ", ParameterName, " not included in quantile analysis"))]
# Use today's date as ScriptLatestRunDate, provide scriptversion
updatedTable[, `:=` (ScriptLatestRunVersion = scriptversion, 
             ScriptLatestRunDate = Sys.Date())]
updatedTable[, `:=` (ActionNeededDate = as_date(ActionNeededDate, origin = "1899-12-30"),
             ScriptLatestRunDate = as_date(ScriptLatestRunDate, origin = "1899-12-30"))]

# Load in previous workbook as template
wb <- loadWorkbook("output/ScriptResults/Database_Thresholds.xlsx")
# wb <- loadWorkbook("output/ScriptResults/Database_Thresholds_20240715.xlsx")
# Text to display date script was run
updateText <- paste0("Script Run: ",Sys.Date())

# Ensure Calculated and IsSpeciesSpecific columns are displayed as 1,0
updatedTable$Calculated <- as.integer(as.logical(updatedTable$Calculated))
updatedTable$isSpeciesSpecific <- as.integer(as.logical(updatedTable$isSpeciesSpecific))

# Setting columns to "impute", writing updated data tables into previous template
cols1 <- c(1:17)
cols2 <- seq(18, ncol(updatedTable))
writeData(wb, sheet = 1, updateText, startRow=2)
writeData(wb, sheet = 1, "", startRow=3)
writeData(wb, sheet = 1, updatedTable[, ..cols1], startRow=7)
writeData(wb, sheet = 1, updatedTable[, ..cols2], startRow=7, startCol=19)
saveWorkbook(wb, here::here(output_file_date), overwrite = T)
saveWorkbook(wb, here::here(output_file), overwrite = T)