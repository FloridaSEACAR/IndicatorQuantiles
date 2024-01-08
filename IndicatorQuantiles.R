# Script to calculate specified quantile and standard deviation threshold values for each parameter 
# in the SEACAR DDI data exports for use in flagging unusual values for QA/QC.
#
# Author: Stephen R. Durham, PhD
#         Florida Department of Environmental Protection
#         stephen.durham@floridadep.gov
#
# Date:    01/08/2024


# Setup------------------------------------------

#Load libraries
library(tidyverse)
library(data.table)
library(doFuture)
library(lubridate)
library(stringr)
library(openxlsx)
library(git2r)
options(scipen = 999) #prevent scientific notation in outputs

# #Process new data export downloads if needed
# downloaddate <- as_date("2023-12-20")
# zips <- file.info(list.files("C:/Users/steph/Downloads/", full.names = TRUE, pattern="*.zip"))
# zips <- subset(zips, date(zips$mtime) == downloaddate)
# 
# for(z in row.names(zips)){
#   unzip(z, exdir = here::here("SEACARdata"), junkpaths = TRUE)
#   
#   while(TRUE %in% str_detect(list.files(here::here("SEACARdata")), ".zip$")){
#     for(zz in list.files(here::here("SEACARdata"), full.names = TRUE, pattern = ".zip$")){
#       unzip(zz, exdir = here::here("SEACARdata"), junkpaths = TRUE)
#       file.remove(zz)
#     }
#   }
#   # file.remove(z)
# }

#Set quantile and number of standard deviation values that should be used
quant_low <- 0.001
quant_high <- 0.999
num_sds <- 3

#Set the strings that need to be interpreted as NA values when loading data files
nas <- c("NULL", "NA", "")

#Identify and check date for reference thresholds file
reffilename <- "Database_Thresholds.xlsx"
reffilepath <- list.files(here::here("output/ScriptResults/"), pattern = reffilename, full.names = TRUE)
ref_info <- file.info(reffilepath)
warning(paste0("The supplied ref. file (", reffilename, ") was created ", ref_info$ctime, " and last modified ", ref_info$mtime, ". Proceed if you are sure this is the most up-to-date version."))

#Specify GitHub user info
github_user = "srdurham"
github_email = "stephen.durham@floridadep.gov"

#Get current script git commit and path, and create a version label
gitcommit_script <- system("git rev-list HEAD -1 IndicatorQuantiles.R", intern=TRUE) #NOTE: this command only looks within the current branch (assumes the user is already using 'main').
scriptpath <- rstudioapi::getSourceEditorContext()$path
scriptname <- str_sub(scriptpath, max(str_locate_all(scriptpath, "/")[[1]]) + 1, -1)
scriptversion <- paste0(scriptname, ", Git Commit ID: ", gitcommit_script)

#This script can be run using parallel processing to speed it up, but note that progress messages will not be printed to the console.
#To run in parallel, uncomment the following line and any other lines marked with "uncomment for parallel use" and comment lines marked
#"comment for parallel use".
# options(future.globals.maxSize = 6291456000) #only necessary if using the parallel processing version of the script

#List data files
seacardat <- list.files(here::here("SEACARdata"), full.names = TRUE, pattern = ".txt")

#Set which parameters to skip (if needed)
parstoskip <- c()

#Remove all but one file for each habitat x parameter
seacardat_forit <- c(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_|Species Richness  - ", negate = TRUE)), subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_.+NE"))) #, subset(seacardat, str_detect(seacardat, "Species Richness  - "))[1])
speciesdat <- sort(subset(seacardat_forit, str_detect(seacardat_forit, "CORAL|CW|NEKTON|SAV")))
names(speciesdat) <- c("coral", "cw", "nekton", "sav")

# Load reference thresholds file-------------------------------
refdat <- setDT(read.xlsx(reffilepath, sheet = 1, startRow = 3))
refdat[, `:=` (ActionNeededDate = as_date(ActionNeededDate, origin = "1899-12-30"),
               ScriptLatestRunVersion = as.character(ScriptLatestRunVersion),
               ScriptLatestRunDate = as_date(ScriptLatestRunDate, origin = "1899-12-30"))][, `:=` (ScriptLatestRunVersion = scriptversion,
                                                                                                   ScriptLatestRunDate = Sys.Date())]

# Load and combine the data exports that include species information------------------------------------ 
spec_dat <- lapply(speciesdat, function(x){
                   assign("dt", fread(x, sep = "|", na.strings = nas))
                   dt[, export := str_sub(x, 61, -1)]
            })

spec_dat <- rbindlist(spec_dat, fill = TRUE, idcol = TRUE)
spec_dat[, `:=` (rowcode = paste0(.id, RowID), Habitat = fcase(.id == "sav", "Submerged Aquatic Vegetation",
                                                               .id == "coral", "Coral/Coral Reef",
                                                               .id == "nekton", "Water Column",
                                                               .id == "cw", "Coastal Wetlands",
                                                               default = NA))]

isspspec <- spec_dat[CommonIdentifier %in% c("Total seagrass", "Total SAV") & ParameterName %in% c("Percent Cover", "Percent Occurrence"), ]
isspspec_habs <- unique(isspspec$Habitat)

spec_dat2 <- merge(spec_dat[!(rowcode %in% isspspec$rowcode), ], refdat[Calculated == 0 & isSpeciesSpecific == 0 & is.na(QuadSize_m2) & IndicatorName != "Grazers and Reef Dependent Species", -c("QuadSize_m2")], by = c("Habitat", "ParameterName"), all.x = TRUE)
spec_dat2[ParameterName %in% c("Count", "Presence/Absence", "Standard Length") & SpeciesGroup1 %in% c("Grazers and reef dependent species", "Reef Fish"), `:=` (IndicatorID = 11, 
                                                                                                                                                                IndicatorName = "Grazers and Reef Dependent Species", 
                                                                                                                                                                ParameterID = fcase(ParameterName == "Count", 46,
                                                                                                                                                                                    ParameterName == "Presence/Absence", 47,
                                                                                                                                                                                    ParameterName == "Standard Length", 81),
                                                                                                                                                                ThresholdID = fcase(ParameterName == "Count", 49,
                                                                                                                                                                                    ParameterName == "Presence/Absence", 50,
                                                                                                                                                                                    ParameterName == "Standard Length", 87))]

#This merging step could be simplified if there was a row in the refdat table for coral percent cover with isSpeciesSpecific == 1 to cover the records in the coral export that have CommonIdentifier == "Total SAV" or "Total seagrass".
for(h in isspspec_habs){
  if(h == "Submerged Aquatic Vegetation"){
    spec_dat2b <- merge(spec_dat[rowcode %in% isspspec$rowcode & Habitat == h, ], refdat[Calculated == 0 & isSpeciesSpecific == 1 & is.na(QuadSize_m2) & Habitat == h, -c("QuadSize_m2", "Habitat")], by = c("ParameterName"), all.x = TRUE)
  } else{
    spec_dat2b <- merge(spec_dat[rowcode %in% isspspec$rowcode & Habitat == h, ], refdat[Calculated == 0 & is.na(QuadSize_m2) & Habitat == h, -c("QuadSize_m2", "Habitat")], by = c("ParameterName"), all.x = TRUE)
  }
  spec_dat2 <- rbind(spec_dat2, spec_dat2b)
}

spec_dat <- copy(spec_dat2)
rm(isspspec, isspspec_habs, spec_dat2, spec_dat2b)
gc()

# plan(multisession, workers = 10) #uncomment for parallel use

#comment for parallel use
qs <- data.table(primaryHab = character(),
                 primaryCombTab = character(),
                 primaryIndID = integer(),
                 primaryInd = character(),
                 primaryParID = integer(),
                 primaryParNm = character(),
                 primaryThrID = integer(),
                 habitat = character(),
                 combinedTable = character(),
                 indicatorID = integer(),
                 indicatorName = character(),
                 parameterID = integer(),
                 parameterName = character(),
                 thresholdID = integer(),
                 calculated = logical(),
                 isSpeciesSpec = logical(),
                 QuadSize_m2 = numeric(),
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
                 export = character())

pars <- data.table(file = character(),
                   param = character())

# Build new quantiles summary data----------------------------------------------------------------
# qs <- foreach(file = seacardat_forit, .combine = rbind) %dofuture% { #uncomment for parallel use
for(file in seacardat_forit){ #comment for parallel use
  file_short <- str_sub(file, 61, -1)
  qs_dat <- data.table(primaryHab = character(),
                       primaryCombTab = character(),
                       primaryIndID = integer(),
                       primaryInd = character(),
                       primaryParID = integer(),
                       primaryParNm = character(),
                       primaryThrID = integer(),
                       habitat = character(),
                       combinedTable = character(),
                       indicatorID = integer(),
                       indicatorName = character(),
                       parameterID = integer(),
                       parameterName = character(),
                       thresholdID = integer(),
                       calculated = logical(),
                       isSpeciesSpec = logical(),
                       QuadSize_m2 = numeric(),
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
                       export = character())
  
  cat("Starting ", file_short, "\n", sep = "")

  if(str_detect(file, "Combined_WQ_WC_NUT_cont_")){
    ## Continuous WQ-----------------------------------------------------------
    #Load and combine all regional continuous WQ files
    #Exception for loading dissolved oxygen data because the parameter name "Dissolved Oxygen" is an exact subset of "Dissolved Oxygen Saturation"
    if(str_detect(file, "Dissolved_Oxygen_Saturation")){
      cont_dat <- lapply(subset(seacardat, str_detect(seacardat, str_sub(file, str_locate(file, "Combined_WQ_WC_NUT_cont_Dissolved_Oxygen_Saturation.+NE")[1], str_locate(file, "Combined_WQ_WC_NUT_cont_Dissolved_Oxygen_Saturation.+NE")[2] - 2))), function(x){
        assign(paste0("cont_", which(str_detect(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_Dissolved_Oxygen_Saturation")), x))),
               fread(x, sep = "|", na.strings = nas))
        
        #record the export name in the data.table
        eval(as.name(paste0("cont_", which(str_detect(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_Dissolved_Oxygen_Saturation")), x)))))[, export := str_sub(x, 61, -1)]
      })
      
      #Keep track of the parameter and the export file it came from
      lapply(cont_dat, function(x){
        pars_f <- data.table(file = unique(x$export),
                             param = unique(x$ParameterName))
        pars <<- rbind(pars, pars_f)
      })
      
    } else{
      #remove "Dissolved Oxygen Saturation" file before selecting and loading data files
      seacardat_sub <- subset(seacardat, str_detect(seacardat, "Dissolved_Oxygen_Saturation", negate = TRUE))
      cont_dat <- lapply(subset(seacardat_sub, str_detect(seacardat_sub, str_sub(file, str_locate(file, "Combined_WQ_WC_NUT_cont_.+NE")[1], str_locate(file, "Combined_WQ_WC_NUT_cont_.+NE")[2] - 2))), function(x){
        assign(paste0("cont_", which(str_detect(subset(seacardat_sub, str_detect(seacardat_sub, "Combined_WQ_WC_NUT_cont_")), x))),
               fread(x, sep = "|", na.strings = nas))
        
        eval(as.name(paste0("cont_", which(str_detect(subset(seacardat_sub, str_detect(seacardat_sub, "Combined_WQ_WC_NUT_cont_")), x)))))[, export := str_sub(x, 61, -1)]
      })
      
      #Keep track of the parameter and the export file it came from
      lapply(cont_dat, function(x){
        pars_f <- data.table(file = unique(x$export),
                             param = unique(x$ParameterName))
        pars <<- rbind(pars, pars_f)
      })
    }
    
    #Combine the loaded data tables and add the Habitat and 'refdat' columns
    cont_dat <- rbindlist(cont_dat)
    cont_dat[, Habitat := "Water Column"]
    cont_dat <- merge(cont_dat, refdat[CombinedTable == "Continuous WQ" & ParameterName %in% unique(cont_dat$ParameterName), ], by = c("Habitat", "ParameterName"), all.x = TRUE)
    
    #Calculate quantile and standard deviation results for each parameter
    for(par in unique(cont_dat$ParameterName)){
      #if the parameter is in the skip parameters list, move on to the next parameter
      if(par %in% parstoskip) next
      
      parid <- cont_dat[ParameterName == par, unique(ParameterID)]

      cont_dat_par <- cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, .(habitat = list(unique(Habitat)),
                                                                                                         combinedTable = list(unique(CombinedTable)),
                                                                                                         indicatorID = list(unique(IndicatorID)),
                                                                                                         indicatorName = list(unique(IndicatorName)),
                                                                                                         parameterID = list(unique(ParameterID)),
                                                                                                         parameterName = unique(ParameterName),
                                                                                                         thresholdID = list(unique(ThresholdID)),
                                                                                                         calculated = unique(Calculated),
                                                                                                         isSpeciesSpec = unique(isSpeciesSpecific),
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
                                                                                                         n_q_low = nrow(cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue < cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, quantile(ResultValue, probs = quant_low)], ]),
                                                                                                         n_q_high = nrow(cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue > cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, quantile(ResultValue, probs = quant_high)], ]),
                                                                                                         n_sdn_low = nrow(cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue < cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, mean(ResultValue) - (num_sds * sd(ResultValue))], ]),
                                                                                                         n_sdn_high = nrow(cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue > cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, mean(ResultValue) + (num_sds * sd(ResultValue))], ]))]

      #Some parameters exist across export files, so record which parameter these calculations are for specifically
      cont_dat_par[, `:=` (primaryHab = "Water Column",
                           primaryCombTab = "Continuous WQ",
                           primaryIndID = cont_dat[ParameterID == parid, unique(IndicatorID)],
                           primaryInd = cont_dat[ParameterID == parid, unique(IndicatorName)],
                           primaryParID = parid,
                           primaryParNm = cont_dat[ParameterID == parid, unique(ParameterName)],
                           primaryThrID = cont_dat[ParameterID == parid, unique(ThresholdID)],
                           QuadSize_m2 = NA,
                           pid = Sys.getpid(), #processor ID (when not running the script in parallel, these will all be the same)
                           export = paste0("example: ", file_short))]

      #Record the parameter results in the 'qs_dat' data.table
      qs_dat <- rbind(qs_dat, cont_dat_par)
      
      #print a progress message
      cat("\t", parid, ": ", par, "\n", sep = "")
    }
    
    #Record the parameter x export file results in the 'qs' data.table
    qs <- rbind(qs, qs_dat) #comment for parallel use
    
    #Remove unnecessary data objects to save memory
    rm(cont_dat, cont_dat_par, pars_f)
    
    #print a progress message
    cat("\t Done! \n\n", sep = "")
    
    # qs_dat #uncomment for parallel use

  } else if(str_detect(file, "Combined_WQ_WC_NUT_")){
    ## Discrete WQ-----------------------------------------------------------
    #Load the data file and specify the habitat
    dat <- fread(file, sep = "|", na.strings = nas)
    dat[, Habitat := "Water Column"]
    
    #Merge refdat variables with the data file separately by calculated versus uncalculated, then recombine
    dat1 <- merge(dat[str_detect(SEACAR_QAQCFlagCode, "1Q", negate = TRUE), ], refdat[CombinedTable == "Discrete WQ" & Calculated == 0 & ParameterName %in% unique(dat$ParameterName), ], by = c("Habitat", "ParameterName"))
    dat2 <- merge(dat, refdat[CombinedTable == "Discrete WQ" & Calculated == 1 & ParameterName %in% unique(dat$ParameterName), ], by = c("Habitat", "ParameterName"))
    dat <- rbind(dat1, dat2)
    
    #Record the parameters in the data file to 'pars' object
    pars_f <- data.table(file = file_short,
                         param = dat[!is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    #Calculate quantile and standard deviation results for each parameter
    for(par in unique(dat$ParameterName)){
      #if the parameter is in the skip parameters list, move on to the next parameter
      if(par %in% parstoskip) next
      
      parid <- dat[ParameterName == par, unique(ParameterID)]
      
      #If parameter is "Total Nitrogen", calculate quantiles/SDs separately for "uncalculated" records
      if(par == "Total Nitrogen"){
        dat_par_all <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & Calculated == 1, .(habitat = list(unique(Habitat)),
                                                                                                                       combinedTable = list(unique(CombinedTable)),
                                                                                                                       indicatorID = list(unique(IndicatorID)),
                                                                                                                       indicatorName = list(unique(IndicatorName)),
                                                                                                                       parameterID = list(unique(ParameterID)),
                                                                                                                       parameterName = unique(ParameterName),
                                                                                                                       thresholdID = list(unique(ThresholdID)),
                                                                                                                       calculated = unique(Calculated),
                                                                                                                       isSpeciesSpec = unique(isSpeciesSpecific),
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
        
        dat_par_nocalc <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & str_detect(SEACAR_QAQCFlagCode, "1Q", negate = TRUE) & Calculated == 0, 
                              .(habitat = list(unique(Habitat)),
                                combinedTable = list(unique(CombinedTable)),
                                indicatorID = list(unique(IndicatorID)),
                                indicatorName = list(unique(IndicatorName)),
                                parameterID = list(unique(ParameterID)),
                                parameterName = unique(ParameterName),
                                thresholdID = list(unique(ThresholdID)),
                                calculated = unique(Calculated),
                                isSpeciesSpec = unique(isSpeciesSpecific),
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
        
        dat_par <- rbind(dat_par_all, dat_par_nocalc)
        
        #Some parameters exist across export files, so record which parameter these calculations are for specifically
        dat_par[, `:=` (primaryHab = rep("Water Column", 2),
                        primaryCombTab = rep("Discrete WQ", 2),
                        primaryIndID = rep(dat[ParameterID == parid, unique(IndicatorID)], 2),
                        primaryInd = rep(dat[ParameterID == parid, unique(IndicatorName)], 2),
                        primaryParID = rep(parid, 2),
                        primaryParNm = rep(dat[ParameterID == parid, unique(ParameterName)], 2),
                        primaryThrID = dat[ParameterID == parid, sort(unique(ThresholdID), decreasing = TRUE)],
                        QuadSize_m2 = NA,
                        pid = Sys.getpid(), #processor ID (when not running the script in parallel, these will all be the same)
                        export = file_short)]
        
      } else{
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, .(habitat = list(unique(Habitat)),
                                                                                                 combinedTable = list(unique(CombinedTable)),
                                                                                                 indicatorID = list(unique(IndicatorID)),
                                                                                                 indicatorName = list(unique(IndicatorName)),
                                                                                                 parameterID = list(unique(ParameterID)),
                                                                                                 parameterName = unique(ParameterName),
                                                                                                 thresholdID = list(unique(ThresholdID)),
                                                                                                 calculated = unique(Calculated),
                                                                                                 isSpeciesSpec = unique(isSpeciesSpecific),
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

      
        #Some parameters exist across export files, so record which parameter these calculations are for specifically
        dat_par[, `:=` (primaryHab = "Water Column",
                      primaryCombTab = "Discrete WQ",
                      primaryIndID = dat[ParameterID == parid, unique(IndicatorID)],
                      primaryInd = dat[ParameterID == parid, unique(IndicatorName)],
                      primaryParID = parid,
                      primaryParNm = dat[ParameterID == parid, unique(ParameterName)],
                      primaryThrID = dat[ParameterID == parid, unique(ThresholdID)],
                      QuadSize_m2 = NA,
                      pid = Sys.getpid(), #processor ID (when not running the script in parallel, these will all be the same)
                      export = file_short)]
      
      }
      
      #Record the parameter results in the 'qs_dat' data.table
      qs_dat <- rbind(qs_dat, dat_par)
      
      #Print a progress message
      cat("\t", parid, ": ", par, "\n", sep = "")
      
    }
    
    #Record the parameter x export file results in the 'qs' data.table
    qs <- rbind(qs, qs_dat) #comment for parallel use
    
    #Remove unnecessary data objects to save memory
    if(par == "Total Nitrogen"){
      rm(dat, dat_par, dat_par_all, dat_par_nocalc, pars_f)
    } else{
      rm(dat, dat_par, pars_f)
    }
    
    #print a progress message
    cat("\t Done! \n\n", sep = "")
    
    # qs_dat #uncomment for parallel use
    
  } else if(str_detect(file, "All_CW_Parameters")){
    ## Coastal Wetlands----------------------------------
    
    #use combined species data object
    dat <- spec_dat
    
    #Only needed for old-style wide-format exports
    # dat <- melt(dat, 
    #             measure.vars = c("[PercentCover-SpeciesComposition_%]", "[StemDensity_#/m2]", "[Total/CanopyPercentCover-SpeciesComposition_%]", "[BasalArea_m2/ha]"),
    #             variable.name = "ParameterName",
    #             value.name = "ResultValue")
    # dat[, ResultValue := as.numeric(ResultValue)]
    
    #Record parameters in the 'pars' object
    pars_f <- data.table(file = file_short,
                         param = dat[.id == "cw" & !is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    #Calculate quantile and standard deviation results for each parameter
    for(par in dat[.id == "cw", unique(ParameterName)]){
      #if the parameter is in the skip parameters list, move on to the next parameter
      if(par %in% parstoskip) next
      
      parid <- dat[.id == "cw" & ParameterName == par, unique(ParameterID)]
      
      dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & str_detect(SpeciesGroup1, "Mangroves|Marsh|Invasive"), .(habitat = list(unique(Habitat)),
                                                                                                                                        combinedTable = list(unique(CombinedTable)),
                                                                                                                                        indicatorID = list(unique(IndicatorID)),
                                                                                                                                        indicatorName = list(unique(IndicatorName)),
                                                                                                                                        parameterID = list(unique(ParameterID)),
                                                                                                                                        parameterName = unique(ParameterName),
                                                                                                                                        thresholdID = list(unique(ThresholdID)),
                                                                                                                                        calculated = unique(Calculated),
                                                                                                                                        isSpeciesSpec = unique(isSpeciesSpecific),
                                                                                                                                        export = list(unique(export)),
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
      
      #Some parameters exist across export files, so record which parameter these calculations are for specifically
      dat_par[, `:=` (primaryHab = "Coastal Wetlands",
                      primaryCombTab = "CW",
                      primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(IndicatorID)], dat[ParameterID == parid, unique(IndicatorID)]),
                      primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(IndicatorName)], dat[ParameterID == parid, unique(IndicatorName)]),
                      primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(ParameterID)], parid),
                      primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(ParameterName)], dat[ParameterID == parid, unique(ParameterName)]),
                      primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(ThresholdID)], dat[ParameterID == parid, unique(ThresholdID)]),
                      QuadSize_m2 = NA,
                      pid = Sys.getpid())] #processor ID (when not running the script in parallel, these will all be the same)
      
      #Record the parameter results in the 'qs_dat' data.table
      qs_dat <- rbind(qs_dat, dat_par)
      
      #print a progress message
      cat("\t", ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(ParameterID)], parid), ": ", par, "\n", sep = "")
    }
    
    #Record the parameter x export file results in the 'qs' data.table
    qs <- rbind(qs, qs_dat) #comment for parallel use
    
    #Remove unnecessary data objects to save memory
    rm(dat, dat_par, pars_f)
    
    #print a progress message
    cat("\t Done! \n\n", sep = "")
    
    # qs_dat #uncomment for parallel use

  } else if(str_detect(file, "All_CORAL_Parameters")){
    ## Coral-------------------------------------------------
    
    # #Only needed when coral exports were separated by region
    # coral_dat <- lapply(subset(seacardat, str_detect(seacardat, "Species Richness  - ")), function(x){
    #   assign(paste0("coral_", which(str_detect(subset(seacardat, str_detect(seacardat, "Species Richness  - ")), x))),
    #          fread(x))
    # })
    #
    # coral_dat <- rbindlist(coral_dat)
    
    #use combined species data object
    dat <- spec_dat
    
    # #Only needed for old-style wide formatted exports
    # dat <- melt(coral_dat, 
    #             measure.vars = c("[PercentCover-SpeciesComposition_%]", "[%LiveTissue_%]", "Height_cm", "Width_cm", "Diameter_cm"),
    #             variable.name = "ParameterName",
    #             value.name = "ResultValue")
    # dat[, ResultValue := as.numeric(ResultValue)]
    
    #Record parameters in the 'pars' object
    pars_f <- data.table(file = file_short,
                         param = dat[.id == "coral" & !is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    #Calculate quantile and standard deviation results for each parameter
    for(par in dat[.id == "coral", unique(ParameterName)]){
      #if the parameter is in the skip parameters list, move on to the next parameter
      if(par %in% parstoskip) next
      
      #Get the Coral parameter ID (exclude "Grazers and Reef Dependent Species" indicator)
      parid <- dat[.id == "coral" & ParameterName == par & IndicatorName != "Grazers and Reef Dependent Species", unique(ParameterID)]
      
      #Calculate quantiles/SDs for data subset from Coral species groups
      dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & str_detect(SpeciesGroup1, "Coral|Cyanobacteria|Milleporans|Octocoral|Porifera|Scleractinian|Zoanthid"), 
                     .(habitat = list(unique(Habitat)),
                       combinedTable = list(unique(CombinedTable)),
                       indicatorID = list(unique(IndicatorID)),
                       indicatorName = list(unique(IndicatorName)),
                       parameterID = list(unique(ParameterID)),
                       parameterName = unique(ParameterName),
                       thresholdID = list(unique(ThresholdID)),
                       calculated = unique(Calculated),
                       isSpeciesSpec = unique(isSpeciesSpecific),
                       export = list(unique(export)),
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
      
      #Some parameters exist across export files, so record which parameter these calculations are for specifically
      dat_par[, `:=` (primaryHab = "Coral/Coral Reef",
                      primaryCombTab = "Coral",
                      primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(IndicatorID)], dat[ParameterID == parid, unique(IndicatorID)]),
                      primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(IndicatorName)], dat[ParameterID == parid, unique(IndicatorName)]),
                      primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(ParameterID)], parid),
                      primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(ParameterName)], dat[ParameterID == parid, unique(ParameterName)]),
                      primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(ThresholdID)], dat[ParameterID == parid, unique(ThresholdID)]),
                      QuadSize_m2 = NA,
                      pid = Sys.getpid())] #processor ID (when not running the script in parallel, these will all be the same)
      
      #Record the parameter results in the 'qs_dat' data.table
      qs_dat <- rbind(qs_dat, dat_par)
      
      #print a progress message
      cat("\t", ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(ParameterID)], parid), ": ", par, "\n", sep = "")
    }
    
    #Record the parameter x export file results in the 'qs' data.table
    qs <- rbind(qs, qs_dat) #comment for parallel use
    
    #Remove unnecessary data objects to save memory
    rm(dat, dat_par, pars_f)
    
    #print a progress message
    cat("\t Done! \n\n", sep = "")
    
    # qs_dat #uncomment for parallel use
    
  } else if(str_detect(file, "All_NEKTON_Parameters")){
    ## Nekton-------------------------------------------------
    
    #use combined species data object
    dat <- spec_dat
    
    dat[.id == "nekton" & SpeciesGroup1 == "", SpeciesGroup1 := NA]
    dat[.id == "nekton" & CommonIdentifier == "Ophiothrix angulata", SpeciesGroup1 := "Grazers and reef dependent species"]
    
    #Record parameters in the 'pars' object
    pars_f <- data.table(file = file_short,
                         param = dat[.id == "nekton" & !is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    #Calculate quantile and standard deviation results for each parameter
    for(par in dat[.id == "nekton", unique(ParameterName)]){
      #if the parameter is in the skip parameters list, move on to the next parameter
      if(par %in% parstoskip) next
      
      #get non-"Grazers and Reef Dependent Species" parameter ID
      parid <- dat[.id == "nekton" & ParameterName == par & IndicatorName != "Grazers and Reef Dependent Species", unique(ParameterID)]
      
      #Calculate quantiles/SDs for data subset from all Nekton species groups
      dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & str_detect(SpeciesGroup2, "Nekton"), .(habitat = list(unique(Habitat)),
                                                                                                                      combinedTable = list(unique(CombinedTable)),
                                                                                                                      indicatorID = list(unique(IndicatorID)),
                                                                                                                      indicatorName = list(unique(IndicatorName)),
                                                                                                                      parameterID = list(unique(ParameterID)),
                                                                                                                      parameterName = unique(ParameterName),
                                                                                                                      thresholdID = list(unique(ThresholdID)),
                                                                                                                      calculated = unique(Calculated),
                                                                                                                      isSpeciesSpec = unique(isSpeciesSpecific),
                                                                                                                      export = list(unique(export)),
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
      
      #Some parameters exist across export files, so record which parameter these calculations are for specifically
      dat_par[, `:=` (primaryHab = "Water Column",
                      primaryCombTab = "Nekton",
                      primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(IndicatorID)], dat[ParameterID == parid, unique(IndicatorID)]),
                      primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(IndicatorName)], dat[ParameterID == parid, unique(IndicatorName)]),
                      primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(ParameterID)], parid),
                      primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(ParameterName)], dat[ParameterID == parid, unique(ParameterName)]),
                      primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(ThresholdID)], dat[ParameterID == parid, unique(ThresholdID)]),
                      QuadSize_m2 = NA,
                      pid = Sys.getpid())] #processor ID (when not running the script in parallel, these will all be the same)
      
      #Record the parameter results in the 'qs_dat' data.table
      qs_dat <- rbind(qs_dat, dat_par)
      
      #print a progress message
      cat("\t", ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(ParameterID)], parid), ": ", par, "\n", sep = "")
      
      #get "Grazers and Reef Dependent Species" parameter ID
      parid <- dat[.id == "coral" & ParameterName == par & IndicatorName == "Grazers and Reef Dependent Species", unique(ParameterID)]
      
      #Calculate quantiles/SDs for data subset from only the "Grazers and reef dependent species" and "Reef Fish" species groups
      dat_par2 <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & SpeciesGroup1 %in% c("Grazers and reef dependent species", "Reef Fish"), .(habitat = list(unique(Habitat)),
                                                                                                                                                           combinedTable = list(unique(CombinedTable)),
                                                                                                                                                           indicatorID = list(unique(IndicatorID)),
                                                                                                                                                           indicatorName = list(unique(IndicatorName)),
                                                                                                                                                           parameterID = list(unique(ParameterID)),
                                                                                                                                                           parameterName = unique(ParameterName),
                                                                                                                                                           thresholdID = list(unique(ThresholdID)),
                                                                                                                                                           calculated = unique(Calculated),
                                                                                                                                                           isSpeciesSpec = unique(isSpeciesSpecific),
                                                                                                                                                           export = list(unique(export)),
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
      
      #Some parameters exist across export files, so record which parameter these calculations are for specifically
      dat_par2[, `:=` (primaryHab = "Coral/Coral Reef",
                       primaryCombTab = "Coral",
                       primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(IndicatorID)], dat[ParameterID == parid, unique(IndicatorID)]),
                       primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(IndicatorName)], dat[ParameterID == parid, unique(IndicatorName)]),
                       primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(ParameterID)], parid),
                       primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(ParameterName)], dat[ParameterID == parid, unique(ParameterName)]),
                       primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(ThresholdID)], dat[ParameterID == parid & IndicatorName == "Grazers and Reef Dependent Species", unique(ThresholdID)]),
                       QuadSize_m2 = NA,
                       pid = Sys.getpid())] #processor ID (when not running the script in parallel, these will all be the same)
      
      #Record the parameter results in the 'qs_dat' data.table
      qs_dat <- distinct(rbind(qs_dat, dat_par2))
      
      #print a progress message
      cat("\t", ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(ParameterID)], parid), ": ", par, "\n", sep = "")
    }
    
    #Record the parameter x export file results in the 'qs' data.table
    qs <- rbind(qs, qs_dat) #comment for parallel use
    
    #Remove unnecessary data objects to save memory
    rm(dat, dat_par, dat_par2, pars_f)
    
    #print a progress message
    cat("\t Done! \n\n", sep = "")
    
    # qs_dat #uncomment for parallel use
    
  } else if(str_detect(file, "All_Oyster_Parameters")){
    ## Oyster-----------------------------------------
    
    #load oyster data file and make some temporary fixes that aren't already in the DDI export
    dat <- fread(file, sep = "|", na.strings = nas)
    dat[QuadSize_m2 == 0.06, QuadSize_m2 := 0.0625]
    dat[ProgramID == 4042 & is.na(QuadSize_m2), QuadSize_m2 := fcase(SampleDate == as_date("2014-06-11"), 1,
                                                                     SampleDate >= as_date("2014-11-11") & SampleDate <= as_date("2015-01-22"), 0.33,
                                                                     SampleDate >= as_date("2015-03-04"), 0.0625)]
    dat[ProgramID == 5035, QuadSize_m2 := NA]
    dat[, Habitat := "Oyster/Oyster Reef"]
    
    #Merge refdat columns into the oyster data.table, but only include "QuadSize_m2" as a grouping variable for the 'number of oysters counted...' and 'shell height' variables
    dat1 <- merge(dat, refdat[CombinedTable == "Oyster" & ParameterID %in% c(26, 51, 27), -c("QuadSize_m2")], by = c("Habitat", "ParameterName"))
    dat2 <- merge(dat, refdat[CombinedTable == "Oyster" & !(ParameterID %in% c(26, 51, 27)), ], by = c("Habitat", "ParameterName", "QuadSize_m2"))
    dat <- rbind(dat1, dat2)
    
    #Record parameters in the 'pars' object
    pars_f <- data.table(file = file_short,
                         param = dat[!is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    #Calculate quantile and standard deviation results for each parameter
    for(par in unique(dat$ParameterName)){
      #if the parameter is in the skip parameters list, move on to the next parameter
      if(par %in% parstoskip) next
      
      parid <- dat[ParameterName == par, unique(ParameterID)]
      
      #Calculate quantiles and SDs separately depending on whether parameter values need to be grouped by 'QuadSize_m2'
      if(par %in% c("Density", "Reef Height", "Percent Live")){
        
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, .(habitat = list(unique(Habitat)),
                                                                                  combinedTable = list(unique(CombinedTable)),
                                                                                  indicatorID = list(unique(IndicatorID)),
                                                                                  indicatorName = list(unique(IndicatorName)),
                                                                                  parameterID = list(unique(ParameterID)),
                                                                                  parameterName = unique(ParameterName),
                                                                                  thresholdID = list(unique(ThresholdID)),
                                                                                  calculated = unique(Calculated),
                                                                                  isSpeciesSpec = unique(isSpeciesSpecific),
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
        
        #Some parameters exist across export files, so record which parameter these calculations are for specifically
        dat_par[, `:=` (primaryHab = "Oyster/Oyster Reef",
                        primaryCombTab = "Oyster",
                        primaryIndID = dat[ParameterID == parid, unique(IndicatorID)],
                        primaryInd = dat[ParameterID == parid, unique(IndicatorName)],
                        primaryParID = parid,
                        primaryParNm = dat[ParameterID == parid, unique(ParameterName)],
                        primaryThrID = dat[ParameterID == parid, unique(ThresholdID)],
                        QuadSize_m2 = NA,
                        pid = Sys.getpid(), #processor ID (when not running the script in parallel, these will all be the same)
                        export = file_short)]

      } else{
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, .(habitat = list(unique(Habitat)),
                                                                                  combinedTable = list(unique(CombinedTable)),
                                                                                  indicatorID = list(unique(IndicatorID)),
                                                                                  indicatorName = list(unique(IndicatorName)),
                                                                                  parameterID = list(unique(ParameterID)),
                                                                                  parameterName = unique(ParameterName),
                                                                                  thresholdID = list(unique(ThresholdID)),
                                                                                  calculated = unique(Calculated),
                                                                                  isSpeciesSpec = unique(isSpeciesSpecific),
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
                                                                                  n_sdn_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, mean(ResultValue) + (num_sds * sd(ResultValue))], ])), by = QuadSize_m2]
        
        #Some parameters exist across export files, so record which parameter these calculations are for specifically
        dat_par[, `:=` (primaryHab = "Oyster/Oyster Reef",
                        primaryCombTab = "Oyster",
                        primaryIndID = dat[ParameterID == parid, unique(IndicatorID)],
                        primaryInd = dat[ParameterID == parid, unique(IndicatorName)],
                        primaryParID = parid,
                        primaryParNm = dat[ParameterID == parid, unique(ParameterName)],
                        primaryThrID = dat[ParameterID == parid, unique(ThresholdID)],
                        pid = Sys.getpid(), #processor ID (when not running the script in parallel, these will all be the same)
                        export = file_short)]
      }

      #Record the parameter results in the 'qs_dat' data.table
      qs_dat <- rbind(qs_dat, dat_par)
      
      #print a progress message
      cat("\t", parid, ": ", par, "\n", sep = "")
    }
    
    #Record the parameter x export file results in the 'qs' data.table
    qs <- rbind(qs, qs_dat) #comment for parallel use
    
    #Remove unnecessary data objects to save memory
    rm(dat, dat_par, dat1, dat2, pars_f)
    
    #print a progress message
    cat("\t Done! \n\n", sep = "")
    
    # qs_dat #uncomment for parallel use
    
  } else {
    ## SAV---------------------------------------------------
    
    #use combined species data object
    dat <- spec_dat
    
    #Record parameters in the 'pars' object
    pars_f <- data.table(file = file_short,
                         param = dat[.id == "sav" & !is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    #Calculate quantile and standard deviation results for each parameter
    for(par in dat[.id == "sav", unique(ParameterName)]){
      #if the parameter is in the skip parameters list, move on to the next parameter
      if(par %in% parstoskip) next
      
      #Two separate options for calculations depending on whether the 'species specific' vs. 'total' distinction (i.e., 'isSpeciesSpecific' variable) is relevant for the parameter 
      if(str_detect(par, "Presence|Blanquet|Count")){
        
        parid <- dat[.id == "sav" & ParameterName == par & isSpeciesSpecific == 0, unique(ParameterID)]
        
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & SpeciesGroup1 %in% c("Seagrass", "Macroalgae") & isSpeciesSpecific == 0, 
                       .(habitat = list(unique(Habitat)),
                         combinedTable = list(unique(CombinedTable)),
                         indicatorID = list(unique(IndicatorID)),
                         indicatorName = list(unique(IndicatorName)),
                         parameterID = list(unique(ParameterID)),
                         parameterName = unique(ParameterName),
                         thresholdID = list(unique(ThresholdID)),
                         calculated = unique(Calculated),
                         isSpeciesSpec = unique(isSpeciesSpecific),
                         export = list(unique(export)),
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
        
        #Some parameters exist across export files, so record which parameter these calculations are for specifically
        dat_par[, `:=` (primaryHab = "Submerged Aquatic Vegetation",
                        primaryCombTab = "SAV",
                        primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par, unique(IndicatorID)], dat[ParameterID == parid, unique(IndicatorID)]),
                        primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par, unique(IndicatorName)], dat[ParameterID == parid, unique(IndicatorName)]),
                        primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par, unique(ParameterID)], parid),
                        primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par, unique(ParameterName)], dat[ParameterID == parid, unique(ParameterName)]),
                        primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par, unique(ThresholdID)], dat[ParameterID == parid, unique(ThresholdID)]),
                        QuadSize_m2 = NA,
                        pid = Sys.getpid())] #processor ID (when not running the script in parallel, these will all be the same)
        
      } else{
        
        parid <- dat[.id == "sav" & ParameterName == par & isSpeciesSpecific == 1, unique(ParameterID)]
        
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & SpeciesGroup1 %in% c("Seagrass", "Macroalgae"), .(habitat = list(unique(Habitat)),
                                                                                                                                   combinedTable = list(unique(CombinedTable)),
                                                                                                                                   indicatorID = list(unique(IndicatorID)),
                                                                                                                                   indicatorName = list(unique(IndicatorName)),
                                                                                                                                   parameterID = list(unique(ParameterID)),
                                                                                                                                   parameterName = unique(ParameterName),
                                                                                                                                   thresholdID = list(unique(ThresholdID)),
                                                                                                                                   calculated = unique(Calculated),
                                                                                                                                   export = list(unique(export)),
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
                                                                                                                                   n_sdn_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, mean(ResultValue) + (num_sds * sd(ResultValue))], ])), by = isSpeciesSpecific]
        
        setnames(dat_par, "isSpeciesSpecific", "isSpeciesSpec")
        
        
        #Some parameters exist across export files, so record which parameter these calculations are for specifically
        dat_par[, `:=` (primaryHab = "Submerged Aquatic Vegetation",
                        primaryCombTab = "SAV",
                        primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par & isSpeciesSpecific == isSpeciesSpec, unique(IndicatorID)], 
                                              dat[ParameterID == parid & isSpeciesSpecific == isSpeciesSpec, unique(IndicatorID)]),
                        primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par & isSpeciesSpecific == isSpeciesSpec, unique(IndicatorName)], 
                                            dat[ParameterID == parid & isSpeciesSpecific == isSpeciesSpec, unique(IndicatorName)]),
                        primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par & isSpeciesSpecific == isSpeciesSpec, unique(ParameterID)], parid),
                        primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par & isSpeciesSpecific == isSpeciesSpec, unique(ParameterName)], 
                                              dat[ParameterID == parid & isSpeciesSpecific == isSpeciesSpec, unique(ParameterName)]),
                        primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par & isSpeciesSpecific == isSpeciesSpec, unique(ThresholdID)], 
                                              dat[ParameterID == parid & isSpeciesSpecific == isSpeciesSpec, unique(ThresholdID)]),
                        QuadSize_m2 = NA,
                        pid = Sys.getpid()), by = isSpeciesSpec] #processor ID (when not running the script in parallel, these will all be the same)
      
      }
      
      #Record the parameter results in the 'qs_dat' data.table
      qs_dat <- rbind(qs_dat, dat_par)
      
      #print a progress message
      cat("\t", ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(ParameterID)], parid), ": ", par, "\n", sep = "")
      
    }
    
    #Record the parameter x export file results in the 'qs' data.table
    qs <- rbind(qs, qs_dat) #comment for parallel use
    
    #Remove unnecessary data objects to save memory
    rm(dat, dat_par, pars_f)
    
    #print a progress message
    cat("\t Done! \n\n", sep = "")
    
    # qs_dat #uncomment for parallel use
  }
  
  #run garbage collection after each iteration to help free up memory
  gc()
}

# Save the quantile results---------------------------------------
#copy the raw quantile results data.table
qs2 <- copy(qs)

#identify the numeric variables and round them to three decimal places (except 'QuadSize_m2').
nums <- colnames(qs2[, .SD, .SDcols = is.numeric])
for(n in nums){
  if(n == "QuadSize_m2") next
  qs2[, (n) := plyr::round_any(eval(as.name(n)), 0.001)]
}

#add variables identifying the script version and the date it was run
qs2[, `:=` (ScriptLatestRunVersion = scriptversion, ScriptLatestRunDate = Sys.Date())] 

## Save detailed indicator quantile results in .xlsx format-----------------------------------
rawoutputname <- "Database_Thresholds_details"
rawoutputextension <- ".xlsx"
hs <- openxlsx::createStyle(textDecoration = "BOLD")
openxlsx::write.xlsx(qs2, here::here(paste0("output/ScriptResults/", rawoutputname, "_", str_replace_all(Sys.Date(), "-", ""), rawoutputextension)), colNames = TRUE, headerStyle = hs, colWidths = "auto")
openxlsx::write.xlsx(qs2, here::here(paste0("output/ScriptResults/", rawoutputname, rawoutputextension)), colNames = TRUE, headerStyle = hs, colWidths = "auto")

#commit new detailed results files to git
repo <- repository(here::here())
config(repo, user.name = github_user, user.email = github_email)
add(repo, here::here(paste0("output/ScriptResults/", rawoutputname, "_", str_replace_all(Sys.Date(), "-", ""), rawoutputextension)))
add(repo, here::here(paste0("output/ScriptResults/", rawoutputname, rawoutputextension)))
commit(repo, paste0("Updated detailed indicator quantile results."))

## Update reference thresholds file--------------------------------------
#Restrict results data.table to only reference file columns and update column names to match
qs2 <- qs2[, .(Calculated = calculated, 
               isSpeciesSpecific = isSpeciesSpec, 
               CombinedTable = primaryCombTab, 
               Habitat = primaryHab, 
               IndicatorName = primaryInd, 
               IndicatorID = primaryIndID, 
               ParameterID = primaryParID, 
               ParameterName = primaryParNm, 
               ThresholdID = primaryThrID, 
               HighQuantile = q_high, 
               LowQuantile = q_low, 
               QuadSize_m2,
               ScriptLatestRunVersion,
               ScriptLatestRunDate)]

#Merge process information columns from reference thresholds file with new results file
qs2 <- merge(qs2, refdat[ThresholdID %in% unique(qs2$ThresholdID), .(ThresholdID, ActionNeeded, ActionNeededDate, AdditionalComments, Conversions, ExpectedValues, HighThreshold, LowThreshold, QuantileSource, Units)], by = "ThresholdID", all = TRUE)
qs2 <- rbind(qs2, refdat[ThresholdID %in% setdiff(refdat$ThresholdID, qs2$ThresholdID), ])
setkey(qs2, NULL) #remove any key created when merging files by ThresholdID

#Update column and row ordering
setcolorder(qs2, c("ThresholdID", "ParameterID", "Habitat", "IndicatorID", "IndicatorName", "CombinedTable", "ParameterName", "Units", "LowThreshold", "HighThreshold", "QuadSize_m2", 
                   "ExpectedValues", "Conversions", "LowQuantile", "HighQuantile", "Calculated", "isSpeciesSpecific", "ActionNeeded", "ActionNeededDate", "QuantileSource", "AdditionalComments", 
                   "ScriptLatestRunVersion", "ScriptLatestRunDate"))
setorder(qs2, Habitat, IndicatorName, ParameterName, Calculated, isSpeciesSpecific, QuadSize_m2)

#Get latest git commit ID
gitcommit <- system("git rev-parse HEAD", intern=TRUE)

#set variable to keep track of number of differences between new results and reference thresholds file
nchanges <- 0

#set index for number of threshold IDs that have been compared
n_id <- 0

#compare each ThresholdID between the new quantile results and the reference thresholds file
for(t in sort(unique(qs2$ThresholdID))){
  n_id <- n_id + 1
  
  #When new LowQuantile value is NA but reference file value is not
  if(is.na(qs2[ThresholdID == t, LowQuantile]) & !is.na(refdat[ThresholdID == t, LowQuantile])){
    refdat[ThresholdID == t, `:=` (LowQuantile = qs2[ThresholdID == t, LowQuantile], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuantileSource = paste0(rawoutputname, rawoutputextension, ", Git Commit ID: ", gitcommit))]
    
    nchanges <- nchanges + 1
    
  }
  
  #When new HighQuantile value is NA but reference file value is not
  if(is.na(qs2[ThresholdID == t, HighQuantile]) & !is.na(refdat[ThresholdID == t, HighQuantile])){
    refdat[ThresholdID == t, `:=` (HighQuantile = qs2[ThresholdID == t, HighQuantile], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuantileSource = paste0(rawoutputname, rawoutputextension, ", Git Commit ID: ", gitcommit))]
    
    nchanges <- nchanges + 1
    
  }
  
  #When neither LowQuantile value is NA and they do not match
  if(!is.na(qs2[ThresholdID == t, LowQuantile]) & refdat[ThresholdID == t, LowQuantile] != qs2[ThresholdID == t, LowQuantile]){
     refdat[ThresholdID == t, `:=` (LowQuantile = qs2[ThresholdID == t, LowQuantile], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuantileSource = paste0(rawoutputname, rawoutputextension, ", Git Commit ID: ", gitcommit))]
    
     nchanges <- nchanges + 1
    
  }
  
  #When neither HighQuantile value is NA and they do not match
  if(!is.na(qs2[ThresholdID == t, HighQuantile]) & refdat[ThresholdID == t, HighQuantile] != qs2[ThresholdID == t, HighQuantile]){
     refdat[ThresholdID == t, `:=` (HighQuantile = qs2[ThresholdID == t, HighQuantile], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuantileSource = paste0(rawoutputname, rawoutputextension, ", Git Commit ID: ", gitcommit))]
    
     nchanges <- nchanges + 1
    
  }
  
  #print progress message
  cat("\r", n_id, " / ", length(unique(qs2$ThresholdID)), " ThresholdIDs checked. ", nchanges, " quantile values updated.")
  
}

#remove any automatically created keys and ensure row ordering is the same
setkey(qs2, NULL)
setorder(qs2, Habitat, IndicatorName, ParameterName, Calculated, isSpeciesSpecific, QuadSize_m2)
setkey(refdat, NULL)
setorder(refdat, Habitat, IndicatorName, ParameterName, Calculated, isSpeciesSpecific, QuadSize_m2)

#If the updated reference file results table is the same as the new quantile results table, save the updated reference thresholds 
#in .xlsx format and commmit the new files to git, otherwise throw an error.
if(is.logical(all.equal(qs2[, -c("ActionNeeded", "ActionNeededDate", "QuantileSource")], refdat[, -c("ActionNeeded", "ActionNeededDate", "QuantileSource")]))){
  #Save updates to the reference sheet
  wb <- loadWorkbook(reffilepath)
  cols1 <- c(1:17)
  cols2 <- seq(18, ncol(refdat))
  writeData(wb, sheet = 1, refdat[, ..cols1], startRow = 3)
  writeData(wb, sheet = 1, refdat[, ..cols2], startRow = 3, startCol = 19)
  saveWorkbook(wb, here::here("output/ScriptResults/Database_Thresholds.xlsx"), overwrite = T)
  saveWorkbook(wb, here::here(paste0("output/ScriptResults/Database_Thresholds_", str_replace_all(Sys.Date(), "-", ""), ".xlsx")), overwrite = T)
  Sys.sleep(5)
  
  #Commit new files to Git
  add(repo, here::here("output/ScriptResults/Database_Thresholds.xlsx"))
  add(repo, here::here(paste0("output/ScriptResults/Database_Thresholds_", str_replace_all(Sys.Date(), "-", ""), ".xlsx")))
  commit(repo, paste0("Updated Database_Thresholds indicator quantile files."))
  
} else{
  stop("The quantile updates have been completed, but one or more mismatches between the quantile results and reference data tables remain. \nPlease resolve them and re-run the script. \n'all.equal(qs2, refdat)' result: ", all.equal(qs2, refdat))
  
}






