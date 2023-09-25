## This file executes IQ_Report.Rmd to render PDF reports detailing data that has
## been flagged for falling outside of IQ range, as determined dynamically based on the data

library(tidyverse)
library(data.table)
library(doFuture)
library(lubridate)
library(stringr)
library(dplyr)

# Create output path if it doesn't already exist
output_path <- c("output","output/data")
for (path in output_path) {
  if (!dir.exists(path)){dir.create(path, recursive = TRUE)}
}

#List data files
seacardat <- list.files(here::here("SEACARdata"), full.names = TRUE)
#Uncomment
# seacardat <- seacardat[c(0:4)]

#Set which parameters to skip (e.g., those with DEAR thresholds and/or expected values)
seacardat <- subset(seacardat, str_detect(seacardat, "Oxygen|pH|Secchi|Salinity|Conductivity|Temperature|Blanquet|Percent", negate = TRUE))
parstoskip <- c("[PercentCover-SpeciesComposition_%]", 
                "[Total/CanopyPercentCover-SpeciesComposition_%]", 
                "Percent Live",
                "[PercentCover-SpeciesComposition_%]",
                "[%LiveTissue_%]",
                "Presence")

#Set quantile thresholds for flagging "questionable" values
quant_low <- 0.001
quant_high <- 0.999
num_sds <- 3

#What are the strings that need to be interpreted as NA values?
nas <- c("NULL")

# create empty data frame template function
create_empty_frame <- function() {
  return(data.table(
    habitat = character(),
    tn = character(),
    parameter = character(),
    median = numeric(),
    iqr = numeric(),
    qval_low = numeric(),
    qval_high = numeric(),
    q_low = numeric(),
    q_high = numeric(),
    mean = numeric(),
    sd = numeric(),
    num_sds = integer(),
    sdn_low = numeric(),
    sdn_high = numeric(),
    n_tot = integer(),
    n_q_low = integer(),
    n_q_high = integer(),
    n_sdn_low = integer(),
    n_sdn_high = integer(),
    pid = integer(),
    filename = character()
  ))
}

qs <- create_empty_frame()

qs2 <- create_empty_frame()

### Combined aggregating function ###
make_dat <- function (dat, par, habitat) {
  
  if (habitat == "Coral Reef" | habitat == "Water Column (Nekton)" | habitat == "Oyster Reef" | habitat == "Submerged Aquatic Vegetation") {
    filtered_dat <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1]
  } else {
    filtered_dat <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1]
  }
  
  # Compute the aggregate statistics based on the filtered data
  dat_par <- filtered_dat[, .(parameter = unique(ParameterName),
                              tn = NA,
                              median = median(ResultValue),
                              iqr = IQR(ResultValue),
                              qval_low = quant_low,
                              qval_high = quant_high,
                              q_low = quantile(ResultValue, probs = quant_low),
                              q_high = quantile(ResultValue, probs = quant_high),
                              mean = mean(ResultValue),
                              sd = sd(ResultValue),
                              num_sds = num_sds,
                              sdn_low = mean(ResultValue) - (num_sds * sd(ResultValue)),
                              sdn_high = mean(ResultValue) + (num_sds * sd(ResultValue)),
                              n_tot = length(ResultValue),
                              n_q_low = nrow(filtered_dat[ResultValue < quantile(ResultValue, probs = quant_low)]),
                              n_q_high = nrow(filtered_dat[ResultValue > quantile(ResultValue, probs = quant_high)]),
                              n_sdn_low = nrow(filtered_dat[ResultValue < mean(ResultValue) - (num_sds * sd(ResultValue))]),
                              n_sdn_high = nrow(filtered_dat[ResultValue > mean(ResultValue) + (num_sds * sd(ResultValue))]) )]
  
  return(dat_par)
}

# Create list of WC NUT files to process into single report
wq_wc_nut_files <- seacardat[str_detect(seacardat, "Combined_WQ_WC_NUT_")]

# Start processing report for "Combined_WQ_WC_NUT_" files if they exist
if(length(wq_wc_nut_files) > 0) {
  # create list to save flagged (high & low) data for WQ
  wq_flagged_data_list <- list()
  # create empty frame to store summary data
  qs_dat <- create_empty_frame()
  
  parameter_list <- list()
  file_short_list <- list()
  for(file in wq_wc_nut_files){
    # create shortened filename for display in report
    file_short <- tail(str_split(file, "/")[[1]], 1)
    
    habitat <- "Water Column (Discrete)"
    
    # read in data file
    dat <- fread(file, sep = "|", na.strings = nas)
    
    params <- unique(dat$ParameterName)
    
    param <- gsub(" ", "_", unique(dat$ParameterName))
    
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
      if(par == "Total Nitrogen"){
        dat_par_all <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, .(parameter = unique(ParameterName),
                                                                                                     median = median(ResultValue),
                                                                                                     iqr = IQR(ResultValue),
                                                                                                     qval_low = quant_low,
                                                                                                     qval_high = quant_high,
                                                                                                     q_low = quantile(ResultValue, probs = quant_low),
                                                                                                     q_high = quantile(ResultValue, probs = quant_high),
                                                                                                     mean = mean(ResultValue),
                                                                                                     sd = sd(ResultValue),
                                                                                                     num_sds = num_sds,
                                                                                                     sdn_low = mean(ResultValue) - (num_sds * sd(ResultValue)),
                                                                                                     sdn_high = mean(ResultValue) + (num_sds * sd(ResultValue)),
                                                                                                     n_tot = length(ResultValue),
                                                                                                     n_q_low = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue < dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, quantile(ResultValue, probs = quant_low)], ]),
                                                                                                     n_q_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, quantile(ResultValue, probs = quant_high)], ]),
                                                                                                     n_sdn_low = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue < dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, mean(ResultValue) - (num_sds * sd(ResultValue))], ]),
                                                                                                     n_sdn_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, mean(ResultValue) + (num_sds * sd(ResultValue))], ]))]
        dat_par_all[, tn := "All"]
        
        dat_par_nocalc <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & str_detect(SEACAR_QAQCFlagCode, "1Q", negate = TRUE), .(parameter = unique(ParameterName),
                                                                                                                                                               median = median(ResultValue),
                                                                                                                                                               iqr = IQR(ResultValue),
                                                                                                                                                               qval_low = quant_low,
                                                                                                                                                               qval_high = quant_high,
                                                                                                                                                               q_low = quantile(ResultValue, probs = quant_low),
                                                                                                                                                               q_high = quantile(ResultValue, probs = quant_high),
                                                                                                                                                               mean = mean(ResultValue),
                                                                                                                                                               sd = sd(ResultValue),
                                                                                                                                                               num_sds = num_sds,
                                                                                                                                                               sdn_low = mean(ResultValue) - (num_sds * sd(ResultValue)),
                                                                                                                                                               sdn_high = mean(ResultValue) + (num_sds * sd(ResultValue)),
                                                                                                                                                               n_tot = length(ResultValue),
                                                                                                                                                               n_q_low = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue < dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, quantile(ResultValue, probs = quant_low)], ]),
                                                                                                                                                               n_q_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, quantile(ResultValue, probs = quant_high)], ]),
                                                                                                                                                               n_sdn_low = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue < dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, mean(ResultValue) - (num_sds * sd(ResultValue))], ]),
                                                                                                                                                               n_sdn_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, mean(ResultValue) + (num_sds * sd(ResultValue))], ]))]
        dat_par_nocalc[, tn := "No calc"]
        
        dat_par <- rbind(dat_par_all, dat_par_nocalc)
        
        
      } else{
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, .(parameter = unique(ParameterName),
                                                                                                 tn = NA,
                                                                                                 median = median(ResultValue),
                                                                                                 iqr = IQR(ResultValue),
                                                                                                 qval_low = quant_low,
                                                                                                 qval_high = quant_high,
                                                                                                 q_low = quantile(ResultValue, probs = quant_low),
                                                                                                 q_high = quantile(ResultValue, probs = quant_high),
                                                                                                 mean = mean(ResultValue),
                                                                                                 sd = sd(ResultValue),
                                                                                                 num_sds = num_sds,
                                                                                                 sdn_low = mean(ResultValue) - (num_sds * sd(ResultValue)),
                                                                                                 sdn_high = mean(ResultValue) + (num_sds * sd(ResultValue)),
                                                                                                 n_tot = length(ResultValue),
                                                                                                 n_q_low = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue < dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, quantile(ResultValue, probs = quant_low)], ]),
                                                                                                 n_q_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, quantile(ResultValue, probs = quant_high)], ]),
                                                                                                 n_sdn_low = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue < dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, mean(ResultValue) - (num_sds * sd(ResultValue))], ]),
                                                                                                 n_sdn_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, mean(ResultValue) + (num_sds * sd(ResultValue))], ]))]
        
      }
      
      # Compute quantile for the low condition
      quant_low_value <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, Include == 1) %>%
        summarize(quant = quantile(ResultValue, probs = quant_low)) %>%
        pull(quant)
      
      # Compute quantile for the high condition
      quant_high_value <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, Include == 1) %>%
        summarize(quant = quantile(ResultValue, probs = quant_high)) %>%
        pull(quant)
      
      # Subset for values falling below quantile
      subset_low <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, Include == 1, ResultValue < quant_low_value) %>%
        mutate(q_subset = "low")
      
      # Subset for values above quantile
      subset_high <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, Include == 1, ResultValue > quant_high_value) %>%
        mutate(q_subset = "high")
      
      # Combine subsets for the current parameter
      combined_subset <- bind_rows(subset_low, subset_high)
      
      # Append the combined data frame to the result list
      wq_flagged_data_list[[par]] <- combined_subset
      # Append file_short to include all file names for WQ
      file_short_list[[par]] <- file_short
      # Append params to include as potential params in overview
      parameter_list[[par]] <- params
      
      dat_par[, `:=` (habitat = habitat,
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
      print(paste0(par, " sequencing complete"))
    }
  }
  
  qs <- rbind(qs, qs_dat)
  
  print("Combined_WQ_WC_NUT processing complete")
  
  wq_flagged_combined_df <- bind_rows(wq_flagged_data_list)
  short_files <- bind_rows(file_short_list)
  params_list <- bind_rows(parameter_list)
  
  # Data export
  fwrite(wq_flagged_combined_df, "output/data/WC_Disc_IQ_data.txt", sep="|")
  
  # Rename file for recognition in IQ_Report.Rmd
  flagged_combined_df <- wq_flagged_combined_df
  
  ### Report Rendering ###
  file_out <- "WC_Disc_IQ_Report"
  rmarkdown::render(input = "IQ_Report.Rmd",
                    output_format = "pdf_document",
                    output_file = paste0(file_out, ".pdf"),
                    output_dir = "output",
                    clean = TRUE)
  
  print(paste0(file_short, " rendering complete"))
  unlink(paste0("output/",file_out, ".md"))
  
  
}

# Start report rendering for Coral, Nekton, SAV, Oyster

for(file in seacardat){
  # create shortened filename for display in report
  file_short <- tail(str_split(file, "/")[[1]], 1)
  qs_dat <- create_empty_frame()
  
  if(str_detect(file, "Combined_WQ_WC_NUT_cont_")) next
  
  if(str_detect(file, "Combined_WQ_WC_NUT_")) next
  
  if(str_detect(file, "All_CORAL_Parameters")){
    habitat <- "Coral Reef"
    
    # read in data file
    dat <- fread(file, sep = "|", na.strings = nas)
    params_list <- unique(dat$ParameterName)
    
    # create list to save flagged (high & low) data
    flagged_data_list <- list()
    
    # loop through parameters contained in data file
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
      # Create data table snippet for excel export
      dat_par <- make_dat(dat, par, habitat)
      
      # Compute quantile for the low condition
      quant_low_value <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1) %>%
        summarize(quant = quantile(ResultValue, probs = quant_low)) %>%
        pull(quant)
      
      # Compute quantile for the high condition
      quant_high_value <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1) %>%
        summarize(quant = quantile(ResultValue, probs = quant_high)) %>%
        pull(quant)
      
      # Subset for values falling below quantile
      subset_low <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, ResultValue < quant_low_value) %>%
        mutate(q_subset = "low")
      
      # Subset for values above quantile
      subset_high <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, ResultValue > quant_high_value) %>%
        mutate(q_subset = "high")
      
      # Combine subsets for the current parameter
      combined_subset <- bind_rows(subset_low, subset_high)
      
      # Append the combined data frame to the result list
      flagged_data_list[[par]] <- combined_subset
      
      dat_par[, `:=` (habitat = habitat,
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
      print(paste0(par, " sequencing complete"))
      
    }
    
    qs <- rbind(qs, qs_dat)
    
    print(paste0(file_short, " export done"))
    
    flagged_combined_df <- bind_rows(flagged_data_list)
    
    # Data export
    fwrite(flagged_combined_df, paste0("output/data/",gsub(" ", "_", habitat),"_data.txt"), sep="|")
    
    ### Report Rendering ###
    file_out <- paste0(gsub(" ", "_", habitat), "_IQ_Report")
    rmarkdown::render(input = "IQ_Report.Rmd",
                      output_format = "pdf_document",
                      output_file = paste0(file_out, ".pdf"),
                      output_dir = "output",
                      clean = TRUE)
    
    print(paste0(file_short, " rendering complete"))
    unlink(paste0("output/", file_out, ".md"))
    
  }
  
  if(str_detect(file, "All_NEKTON_Parameters")){
    habitat <- "Water Column (Nekton)"
    
    # read in data file
    dat <- fread(file, sep = "|", na.strings = nas)
    params_list <- unique(dat$ParameterName)
    
    dat[EffortCorrection_100m2 > 0, ResultValue := ResultValue/EffortCorrection_100m2]
    dat[, `:=` (ParameterName = "Count/100m2 (effort corrected)")]
    dat[SpeciesGroup1 == "", SpeciesGroup1 := NA]
    dat[CommonIdentifier == "Ophiothrix angulata", SpeciesGroup1 := "Grazers and reef dependent species"]
    
    # create list to save flagged (high & low) data
    flagged_data_list <- list()
    
    # loop through parameters contained in data file
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
      print(par)
      
      # Create data table snippet for excel export
      dat_par <- make_dat(dat, par, habitat)
      
      dat_par[, `:=` (habitat = habitat,
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
      # Create separate table entries for Grazers & Reef Dependent Species (Coral Reef classified)
      dat_par2 <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & SpeciesGroup1 == "Grazers and reef dependent species", .(parameter = paste0(unique(ParameterName), " - Grazers and reef dependent species"),
                                                                                                                                         tn = NA,
                                                                                                                                         median = median(ResultValue),
                                                                                                                                         iqr = IQR(ResultValue),
                                                                                                                                         qval_low = quant_low,
                                                                                                                                         qval_high = quant_high,
                                                                                                                                         q_low = quantile(ResultValue, probs = quant_low),
                                                                                                                                         q_high = quantile(ResultValue, probs = quant_high),
                                                                                                                                         mean = mean(ResultValue),
                                                                                                                                         sd = sd(ResultValue),
                                                                                                                                         num_sds = num_sds,
                                                                                                                                         sdn_low = mean(ResultValue) - (num_sds * sd(ResultValue)),
                                                                                                                                         sdn_high = mean(ResultValue) + (num_sds * sd(ResultValue)),
                                                                                                                                         n_tot = length(ResultValue),
                                                                                                                                         n_q_low = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & ResultValue < dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, quantile(ResultValue, probs = quant_low)], ]),
                                                                                                                                         n_q_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, quantile(ResultValue, probs = quant_high)], ]),
                                                                                                                                         n_sdn_low = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & ResultValue < dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, mean(ResultValue) - (num_sds * sd(ResultValue))], ]),
                                                                                                                                         n_sdn_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, mean(ResultValue) + (num_sds * sd(ResultValue))], ]))]
      
      
      dat_par2[, `:=` (habitat = "Coral Reef",
                       pid = Sys.getpid(),
                       filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par2)
      
      # Compute quantile for the low condition
      quant_low_value <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1) %>%
        summarize(quant = quantile(ResultValue, probs = quant_low)) %>%
        pull(quant)
      
      # Compute quantile for the high condition
      quant_high_value <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1) %>%
        summarize(quant = quantile(ResultValue, probs = quant_high)) %>%
        pull(quant)
      
      # Subset for values falling below quantile
      subset_low <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, ResultValue < quant_low_value) %>%
        mutate(q_subset = "low")
      
      # Subset for values above quantile
      subset_high <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, ResultValue > quant_high_value) %>%
        mutate(q_subset = "high")
      
      # Combine subsets for the current parameter
      combined_subset <- bind_rows(subset_low, subset_high)
      
      # Append the combined data frame to the result list
      flagged_data_list[[par]] <- combined_subset
      
      print(paste0(par, " sequencing complete"))
      
    }
    
    qs <- rbind(qs, qs_dat)
    
    print(paste0(file_short, " export done"))
    
    flagged_combined_df <- bind_rows(flagged_data_list)
    
    # Data export
    fwrite(flagged_combined_df, paste0("output/data/Water_Column_Nekton_data.txt"), sep="|")
    
    ### Report Rendering ###
    file_out <- "Water_Column_Nekton_IQ_Report"
    rmarkdown::render(input = "IQ_Report.Rmd",
                      output_format = "pdf_document",
                      output_file = paste0(file_out, ".pdf"),
                      output_dir = "output",
                      clean = TRUE)
    
    print(paste0(file_short, " rendering complete"))
    unlink(paste0("output/", file_out, ".md"))
    
  }
  
  if(str_detect(file, "All_Oyster_Parameters")) {
    habitat <- "Oyster Reef"
    
    # read in data file
    dat <- fread(file, sep = "|", na.strings = nas)
    params_list <- unique(dat$ParameterName)
    
    dat[ParameterName != "Density" & ParameterName != "Reef Height" & ParameterName != "Percent Live", 
        ParameterName := paste0(ParameterName, "/", QuadSize_m2, "m2")]
    
    # create list to save flagged (high & low) data
    flagged_data_list <- list()
    
    # loop through parameters contained in data file
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
      # Create data table snippet for excel export
      dat_par <- make_dat(dat, par, habitat)
      
      # Compute quantile for the low condition
      quant_low_value <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1) %>%
        summarize(quant = quantile(ResultValue, probs = quant_low)) %>%
        pull(quant)
      
      # Compute quantile for the high condition
      quant_high_value <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1) %>%
        summarize(quant = quantile(ResultValue, probs = quant_high)) %>%
        pull(quant)
      
      # Subset for values falling below quantile
      subset_low <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, ResultValue < quant_low_value) %>%
        mutate(q_subset = "low")
      
      # Subset for values above quantile
      subset_high <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, ResultValue > quant_high_value) %>%
        mutate(q_subset = "high")
      
      # Combine subsets for the current parameter
      combined_subset <- bind_rows(subset_low, subset_high)
      
      # Append the combined data frame to the result list
      flagged_data_list[[par]] <- combined_subset
      
      dat_par[, `:=` (habitat = habitat,
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
      print(paste0(par, " sequencing complete"))
      
    }
    
    qs <- rbind(qs, qs_dat)
    
    print(paste0(file_short, " export done"))
    
    flagged_combined_df <- bind_rows(flagged_data_list)
    
    # Data export
    fwrite(flagged_combined_df, paste0("output/data/",gsub(" ", "_", habitat),"_data.txt"), sep="|")
    
    ### Report Rendering ###
    file_out <- paste0(gsub(" ", "_", habitat), "_IQ_Report")
    rmarkdown::render(input = "IQ_Report.Rmd",
                      output_format = "pdf_document",
                      output_file = paste0(file_out, ".pdf"),
                      output_dir = "output",
                      clean = TRUE)
    
    print(paste0(file_short, " rendering complete"))
    unlink(paste0("output/", file_out, ".md"))
  }
  
  if(str_detect(file, "All_SAV_Parameters")) {
    
    habitat <- "Submerged Aquatic Vegetation"
    
    dat <- fread(file, sep = "|", na.strings = nas)
    params_list <- unique(dat$ParameterName)
    
    # create list to save flagged (high & low) data
    flagged_data_list <- list()
    
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
      # Create data table snippet for excel export
      dat_par <- make_dat(dat, par, habitat)
      
      # Compute quantile for the low condition
      quant_low_value <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1) %>%
        summarize(quant = quantile(ResultValue, probs = quant_low)) %>%
        pull(quant)
      
      # Compute quantile for the high condition
      quant_high_value <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1) %>%
        summarize(quant = quantile(ResultValue, probs = quant_high)) %>%
        pull(quant)
      
      # Subset for values falling below quantile
      subset_low <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, ResultValue < quant_low_value) %>%
        mutate(q_subset = "low")
      
      # Subset for values above quantile
      subset_high <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, ResultValue > quant_high_value) %>%
        mutate(q_subset = "high")
      
      # Combine subsets for the current parameter
      combined_subset <- bind_rows(subset_low, subset_high)
      
      # Append the combined data frame to the result list
      flagged_data_list[[par]] <- combined_subset
      
      dat_par[, `:=` (habitat = habitat,
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
      print(paste0(par, " sequencing complete"))
      
    }
    
    qs <- rbind(qs, qs_dat)
    
    print(paste0(file_short, " export done"))
    
    flagged_combined_df <- bind_rows(flagged_data_list)
    
    # Data export
    fwrite(flagged_combined_df, paste0("output/data/",gsub(" ", "_", habitat),"_data.txt"), sep="|")
    
    ### Report Rendering ###
    file_out <- paste0(gsub(" ", "_", habitat), "_IQ_Report")
    rmarkdown::render(input = "IQ_Report.Rmd",
                      output_format = "pdf_document",
                      output_file = paste0(file_out, ".pdf"),
                      output_dir = "output",
                      clean = TRUE)
    
    print(paste0(file_short, " rendering complete"))
    unlink(paste0("output/", file_out, ".md"))
  }
  
}

qs2 <- qs
nums <- colnames(qs2[, .SD, .SDcols = is.numeric])

# rounding operation
qs2[, (nums) := lapply(.SD, function(x) round(x,3)), .SDcols = nums]

qs2[, parameter := as.character(parameter)]
setorder(qs2, habitat, parameter)

hs <- openxlsx::createStyle(textDecoration = "BOLD")
openxlsx::write.xlsx(qs2, here::here(paste0("IndicatorQuantiles_", Sys.Date(), ".xlsx")), colNames = TRUE, headerStyle = hs, colWidths = "auto")