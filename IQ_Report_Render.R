library(tidyverse)
library(data.table)
library(doFuture)
library(lubridate)
library(stringr)
library(dplyr)

# Create output path if it doesn't already exist
output_path <- "output/"
if (!dir.exists(output_path)){dir.create(output_path, recursive = TRUE)}

#List data files
seacardat <- list.files(here::here("SEACARdata"), full.names = TRUE)

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

qs <- data.table(habitat = character(),
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
                 filename = character())

qs2 <- data.table(habitat = character(),
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
                  filename = character())

for(file in seacardat){
  file_short <- tail(str_split(file, "/")[[1]], 1)
  qs_dat <- data.table(habitat = character(),
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
                       filename = character())
  
  if(str_detect(file, "All_CORAL_Parameters")){
    # read in data file
    dat <- fread(file, sep = "|", na.strings = nas)
    
    # create list to save excluded (high & low) data
    excluded_data_list <- list()
    
    # loop through parameters contained in data file
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      # Create data table snippet for excel export
      dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, .(parameter = unique(ParameterName),
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
        select(ProgramID, ProgramName, ParameterName, ProgramLocationID, SampleDate, AreaID, CommonIdentifier, ResultValue) %>%
        mutate(q_subset = "low")
      
      # Subset for values above quantile
      subset_high <- dat %>%
        filter(ParameterName == par, !is.na(ResultValue), MADup == 1, ResultValue > quant_high_value) %>%
        select(ProgramID, ProgramName, ParameterName, ProgramLocationID, SampleDate, AreaID, CommonIdentifier, ResultValue) %>%
        mutate(q_subset = "high")
      
      # Combine subsets for the current parameter
      combined_subset <- bind_rows(subset_low, subset_high)
      
      # Append the combined data frame to the result list
      excluded_data_list[[par]] <- combined_subset
      
      dat_par[, `:=` (habitat = "Coral Reef",
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
      print(paste0(par, " sequencing complete"))
      
    }
    
    qs <- rbind(qs, qs_dat)
    
    print(paste0(file_short, " export done"))
    
    excluded_combined_df <- bind_rows(excluded_data_list)
    
    ### Report Rendering ###
    file_out <- "Coral_Reef_IQ_Report"
    rmarkdown::render(input = "IQ_Report.Rmd",
                      output_format = "pdf_document",
                      output_file = paste0(file_out, ".pdf"),
                      output_dir = output_path,
                      clean = TRUE)
    
    print(paste0(file_short, " rendering complete"))
    unlink(paste0(output_path, "/", file_out, ".md"))
    
  }
}