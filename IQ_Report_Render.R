## This file executes IQ_Report.Rmd to render PDF reports detailing data that has
## been flagged for falling outside of IQ range, as determined dynamically based on the data

library(tidyverse)
library(data.table)
library(doFuture)
library(lubridate)
library(stringr)
library(openxlsx)
library(dplyr)
library(tictoc)

# import "seacar_data_location" variable which points to data directory
source("seacar_data_location.R")

# Create output path if it doesn't already exist
output_path <- c("output","output/data")
for (path in output_path) {
  if (!dir.exists(path)){dir.create(path, recursive = TRUE)}
}

#List data files
seacardat <- list.files(seacar_data_location, full.names = TRUE)

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
parstoskip <- ""

#What are the strings that need to be interpreted as NA values?
nas <- c("NULL", "NA", "")

ref_parameters <- fread("data/Ref_Parameters.csv")
ref_parameters <- ref_parameters[IndicatorName!="Acreage", ]

# list of habitats to generate reports for
habitats <- unique(ref_parameters$Habitat)
# coral subset
habitats <- habitats[1]

table_template <- function(){
  return(
    data.table(
      habitat = character(),
      indicator = character(),
      parameter = character(),
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

tic()
# Empty frames/list to store results
data_directory <- list()
qs <- table_template()
wq_qs <- table_template()

# Filter by species group? To check Indicator/Parameter SG1 combos
species_group_filtering <- TRUE

# Exclude these Programs (using ProgramID) from print-outs
# exclude_programs <- c(5002, 103, 514)
exclude_programs <- c(5002)

for (h in habitats){
  
  if(h=="Water Column"){
    
    # list to store shortened file names to display in report
    file_short_list <- list()
    water_column_summary_directory <- list()
    
    # Check if Nekton file is present, add to report
    if(length(nekton_file)>0){
      # NEKTON
    }
    
    # Check if WQ_Disc files are present, add to report
    if(length(wq_disc_files)>0){
      # DISCRETE
      type_name <- "Discrete"
      
      qs_dat <- table_template()
      wq_flagged_data_list <- list()
      
      # Discrete processing
      for (file in wq_disc_files){
        
        # shortened file name
        file_short <- tail(str_split(file, "/")[[1]], 1)
        
        data <- fread(file, sep='|')
        data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
        
        for (p in unique(data$ParameterName)){
          if(p %in% parstoskip){next}
          
          # Set indicator name for each parameter (WC, WQ, NUT)
          i <- ref_parameters[ParameterName==p, IndicatorName]
          
          # If parameter is "Total Nitrogen", calculate quantiles/SDs separately for "uncalculated" records
          if(p == "Total Nitrogen"){
            dat_par_all <- data[ParameterName==p & !is.na(ResultValue) & MADup == 1 & Include == 1,
                                .(parameter = p,
                                  indicator = i,
                                  habitat = h,
                                  q_low = quantile(ResultValue, probs = quant_low),
                                  q_high = quantile(ResultValue, probs = quant_high),
                                  mean = mean(ResultValue),
                                  n_tot = length(ResultValue))]
            dat_par_all[, sub_parameter := "All"]
            
            dat_par_nocalc <- data[ParameterName==p & !is.na(ResultValue) & MADup == 1 & Include == 1 & str_detect(SEACAR_QAQCFlagCode, "1Q", negate = TRUE),
                                   .(parameter = p,
                                     indicator = i,
                                     habitat = h,
                                     q_low = quantile(ResultValue, probs = quant_low),
                                     q_high = quantile(ResultValue, probs = quant_high),
                                     mean = mean(ResultValue),
                                     n_tot = length(ResultValue))]
            dat_par_nocalc[, sub_parameter := "No calc"]
            
            dat_par <- rbind(dat_par_all, dat_par_nocalc)
            
            for (sub_param in unique(dat_par$sub_parameter)){
              
              if(sub_param == "No calc"){
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
              dat_par[sub_parameter==sub_param, `:=` (parameter = paste0("Total Nitrogen (", sub_param, ")"))]
              
              dat_par[ , c('QuadSize_m2')] = ""
              
            }
            
          } else {
            dat_par <- data[ParameterName==p & !is.na(ResultValue) & MADup == 1 & Include == 1,
                            .(parameter = p,
                              indicator = i,
                              habitat = h,
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
            dat_par[ , c('sub_parameter', 'QuadSize_m2')] = ""
            
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
      # CONTINUOUS
    }
    
    # Combine all flagged data outputs for each indicator into single directory
    data_directory[[h]] <- wq_flagged_data_list
    
    # Report filename
    file_out <- paste0(gsub("/","_",gsub(" ", "_", h)), "_IQ_Report")
    
    # Render report
    rmarkdown::render(input = "IQ_Report.Rmd",
                      output_format = "pdf_document",
                      output_file = paste0(file_out, ".pdf"),
                      output_dir = "output",
                      clean = TRUE)
    
  }
  
  if(h=="Submerged Aquatic Vegetation"){
    file <- str_subset(seacardat, "All_SAV")
    
    # shortened filename for display in report
    file_short <- tail(str_split(file, "/")[[1]], 1)
    
    # get list of indicators within a given habitat
    indicators <- ref_parameters[Habitat==h, unique(IndicatorName)]
    
    # load in habitat data
    data <- fread(file, sep="|")
    data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
    
    qs_dat <- table_template()
    
    flagged_data_list <- list()
    
    for (i in indicators){
      
      # unique parameters included for each indicator/habitat combo
      parameters <- ref_parameters[Habitat==h & IndicatorName==i, unique(ParameterName)]
      
      # SAV SG1 Groups
      sg1_include <- c("Macroalgae","Seagrass","Total SAV")
      
      indicator_data <- data[SpeciesGroup1 %in% sg1_include, ]
      
      for (p in parameters){
        
        # Record summary for table overview
        dat_par <- indicator_data[ParameterName==p, 
                                  .(parameter = p,
                                    indicator = i,
                                    habitat = h,
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
        
        dat_par[ , c('sub_parameter', 'QuadSize_m2')] = ""
        
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
    rmarkdown::render(input = "IQ_Report.Rmd",
                      output_format = "pdf_document",
                      output_file = paste0(file_out, ".pdf"),
                      output_dir = "output",
                      clean = TRUE)
    
  }
  
  if(h=="Oyster/Oyster Reef"){
    file <- str_subset(seacardat, "All_Oyster")
    
    # shortened filename for display in report
    file_short <- tail(str_split(file, "/")[[1]], 1)
    
    # get list of indicators within a given habitat
    indicators <- ref_parameters[Habitat==h, unique(IndicatorName)]
    
    # load in habitat data
    data <- fread(file, sep="|")
    data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
    
    # Adjustments to quad size & other temporary fixes
    data[QuadSize_m2 == 0.06, QuadSize_m2 := 0.0625]
    data[ProgramID == 4042 & is.na(QuadSize_m2), QuadSize_m2 := fcase(SampleDate == as_date("2014-06-11"), 1,
                                                                      SampleDate >= as_date("2014-11-11") & SampleDate <= as_date("2015-01-22"), 0.33,
                                                                      SampleDate >= as_date("2015-03-04"), 0.0625)]
    data[ProgramID == 5035, QuadSize_m2 := NA]
    
    qs_dat <- table_template()
    
    flagged_data_list <- list()
    
    for (i in indicators){
      
      # unique parameters included for each indicator/habitat combo
      parameters <- ref_parameters[Habitat==h & IndicatorName==i, unique(ParameterName)]
      
      indicator_data <- data
      
      for (p in parameters){
        
        #Calculate quantiles and SDs separately depending on whether parameter values need to be grouped by 'QuadSize_m2'
        if(p %in% c("Density","Reef Height","Percent Live")){
          # Record summary for table overview
          dat_par <- indicator_data[ParameterName==p, 
                                    .(parameter = p,
                                      indicator = i,
                                      habitat = h,
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
          dat_par$QuadSize_m2 <- "None"
          
          # append to make long-form table
          qs_dat <- rbind(qs_dat, dat_par, fill=TRUE)
          
        } else {
          dat_par <- indicator_data[ParameterName==p, 
                                    .(parameter = p,
                                      indicator = i,
                                      habitat = h,
                                      q_low = quantile(ResultValue, probs = quant_low),
                                      q_high = quantile(ResultValue, probs = quant_high),
                                      mean = mean(ResultValue),
                                      n_tot = length(ResultValue)), by = QuadSize_m2]
          
          #pull values for each individual quadsize
          quad_sizes <- unique(dat_par$QuadSize_m2)
          
          for (q in quad_sizes){
            # pull high and low quantiles for filtering
            quant_low_value <- dat_par[QuadSize_m2==q, q_low]
            quant_high_value <- dat_par[QuadSize_m2==q, q_high]
            
            # grab subset of data that falls below quantile limit
            subset_low <- indicator_data[ParameterName==p & QuadSize_m2==q & ResultValue < quant_low_value, ]
            subset_low$q_subset <- "low"
            
            # grab subset of data that falls above quantile limit
            subset_high <- indicator_data[ParameterName==p & QuadSize_m2==q & ResultValue > quant_high_value, ]
            subset_high$q_subset <- "high"
            
            # combine datasets for display in report
            combined_subset <- bind_rows(subset_low, subset_high)
            combined_subset$quad_size <- q
            
            # Append data directory with included, excluded data
            flagged_data_list[[i]][[p]] <- combined_subset
            
            # Add n_q_low and n_q_high to dat_par table
            dat_par[QuadSize_m2==q, `:=` (n_q_low = nrow(subset_low))]
            dat_par[QuadSize_m2==q, `:=` (n_q_high = nrow(subset_high))]
          }
          
          dat_par[ , c('sub_parameter')] = ""
          # append to make long-form table
          qs_dat <- rbind(qs_dat, dat_par, fill=TRUE)
        }
      }
    }
    
    # Record overall results
    qs <- rbind(qs, qs_dat, fill=TRUE)
    
    print(paste0(file_short, " export done"))
    
    data_directory[[h]] <- flagged_data_list
    
    # Report filename
    file_out <- paste0(gsub("/","_",gsub(" ", "_", h)), "_IQ_Report")
    
    # Render report
    rmarkdown::render(input = "IQ_Report.Rmd",
                      output_format = "pdf_document",
                      output_file = paste0(file_out, ".pdf"),
                      output_dir = "output",
                      clean = TRUE)
    
  }
  
  if(h=="Coastal Wetlands"){
    file <- str_subset(seacardat, "All_CW")
    
    # shortened filename for display in report
    file_short <- tail(str_split(file, "/")[[1]], 1)
    
    # get list of indicators within a given habitat
    indicators <- ref_parameters[Habitat==h, unique(IndicatorName)]
    
    # load in habitat data
    data <- fread(file, sep="|")
    data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
    
    qs_dat <- table_template()
    
    flagged_data_list <- list()
    
    for (i in indicators){
      
      # unique parameters included for each indicator/habitat combo
      parameters <- ref_parameters[Habitat==h & IndicatorName==i, unique(ParameterName)]
      
      #Sg1 for Coastal Wetlands
      sg1_include <- c("Mangroves","Marsh","Invasive")
      
      for (p in parameters){
        
        indicator_data <- data[SpeciesGroup1 %in% sg1_include, ]
        
        # Record summary for table overview
        dat_par <- indicator_data[ParameterName==p, 
                                  .(parameter = p,
                                    indicator = i,
                                    habitat = h,
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
        
        dat_par[ , c('sub_parameter', 'QuadSize_m2')] = ""
        
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
    rmarkdown::render(input = "IQ_Report.Rmd",
                      output_format = "pdf_document",
                      output_file = paste0(file_out, ".pdf"),
                      output_dir = "output",
                      clean = TRUE)
    
  }
  
  if(h=="Coral/Coral Reef"){
    
    file <- str_subset(seacardat, "All_CORAL")
    
    # shortened filename for display in report
    file_short <- tail(str_split(file, "/")[[1]], 1)
    
    # get list of indicators within a given habitat
    indicators <- ref_parameters[Habitat==h, unique(IndicatorName)]
    
    # load in habitat data
    data <- fread(file, sep="|")
    data <- data[Include==1 & MADup==1 & !is.na(ResultValue), ]
    
    qs_dat <- table_template()
    
    flagged_data_list <- list()
    
    for (i in indicators){
      
      # unique parameters included for each indicator/habitat combo
      parameters <- ref_parameters[Habitat==h & IndicatorName==i, unique(ParameterName)]
      
      # Filtering for SpeciesGroup1 with indicator/parameter combos
      if (i=="Grazers and Reef Dependent Species"){
        sg1_include <- c("Grazers and reef dependent species","Reef Fish")
      }
      
      if (i=="Community Composition"){
        # SG1 for "Count", add additional for PA and Colony Density
        sg1_include <- c("Corallimorpharians", "Milleporans", "Octocoral", "Others", "Porifera", "Scleractinian", "NULL")
        if (p=="Presence/Absence"){
          sg1_include <- c(sg1_include, "Cyanobacteria", "Macroalgae", "Zoanthid")
        }
        if (p=="Colony Density"){
          sg1_include <- c(sg1_include, "Cyanobacteria", "Macroalgae", "Zoanthid", "Substrate")
        }
      }
      
      if (i == "Percent Cover"){
        # SG1 for "Colony..."
        sg1_include <- c("Corallimorpharians", "Milleporans", "Octocoral", "Others", "Porifera", "Scleractinian", "NULL")
        if (p=="Percent Cover"){
          sg1_include <- c(sg1_include, "Cyanobacteria", "Macroalgae", "Zoanthid", "Substrate", "Seagrass")
        }
        if (p=="Percent Live Tissue"){
          sg1_include <- c(sg1_include, "Zoanthid")
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
        
        # Record summary for table overview
        dat_par <- indicator_data[ParameterName==p, 
                                  .(parameter = p,
                                    indicator = i,
                                    habitat = h,
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
        
        dat_par[ , c('sub_parameter', 'QuadSize_m2')] = ""
        
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
    rmarkdown::render(input = "IQ_Report.Rmd",
                      output_format = "pdf_document",
                      output_file = paste0(file_out, ".pdf"),
                      output_dir = "output",
                      clean = TRUE)
  }
}
toc()