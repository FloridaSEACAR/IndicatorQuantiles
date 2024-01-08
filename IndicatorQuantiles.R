library(tidyverse)
library(data.table)
library(doFuture)
library(lubridate)
library(stringr)
library(openxlsx)
library(git2r)
options(scipen = 999)

github_user = "srdurham"
github_email = "stephen.durham@floridadep.gov"

#Get current git commit and script path
gitcommit <- system("git rev-parse HEAD", intern=TRUE)
scriptpath <- rstudioapi::getSourceEditorContext()$path
scriptname <- str_sub(scriptpath, max(str_locate_all(scriptpath, "/")[[1]]) + 1, -1)
scriptversion <- paste0(scriptname, ", Git Commit ID: ", gitcommit)

# options(future.globals.maxSize = 6291456000) #only necessary if using the parallel processing version of the script

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

#List data files
seacardat <- list.files(here::here("SEACARdata"), full.names = TRUE, pattern = ".txt")

# #Set which parameters to skip (e.g., those with DEAR thresholds and/or expected values)
# seacardat <- subset(seacardat, str_detect(seacardat, "Oxygen|pH|Secchi|Salinity|Conductivity|Temperature|Blanquet|Percent", negate = TRUE))
# parstoskip <- c("[PercentCover-SpeciesComposition_%]", 
#                 "[Total/CanopyPercentCover-SpeciesComposition_%]", 
#                 "Percent Live",
#                 "[PercentCover-SpeciesComposition_%]",
#                 "[%LiveTissue_%]",
#                 "Presence")
parstoskip <- c()

#Remove all but one file for each habitat x parameter
seacardat_forit <- c(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_|Species Richness  - ", negate = TRUE)), subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_.+NE"))) #, subset(seacardat, str_detect(seacardat, "Species Richness  - "))[1])
speciesdat <- sort(subset(seacardat_forit, str_detect(seacardat_forit, "CORAL|CW|NEKTON|SAV")))
names(speciesdat) <- c("coral", "cw", "nekton", "sav")

#Set quantile thresholds for flagging "questionable" values
quant_low <- 0.001
quant_high <- 0.999
num_sds <- 3

#What are the strings that need to be interpreted as NA values?
nas <- c("NULL", "NA", "")

#Identify and load Reference Thresholds file
reffilename <- "Database_Thresholds_20231130_srd.xlsx"
reffilepath <- list.files(here::here(), pattern = reffilename, full.names = TRUE)
ref_info <- file.info(reffilepath)
print(paste0("Note: The supplied ref. file (", reffilename, ") was created ", ref_info$ctime, " and last modified ", ref_info$mtime, ". Proceed if you are sure this is the most up-to-date version."))

refdat <- setDT(read.xlsx(reffilepath, sheet = 1, startRow = 3))
refdat[, `:=` (ActionNeededDate = as_date(ActionNeededDate, origin = "1899-12-30"),
               ScriptLatestRunVersion = as.character(scriptversion),
               ScriptLatestRunDate = Sys.Date())]

#Load and combine the data exports that include species information 
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

plan(multisession, workers = 10)

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
                 #SpeciesGroup1 = character(),
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

#Build new quantiles summary data
# qs <- foreach(file = seacardat_forit, .combine = rbind) %dofuture% {
for(file in seacardat_forit){
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
                       #SpeciesGroup1 = character(),
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
   
    if(str_detect(file, "Dissolved_Oxygen_Saturation")){
      cont_dat <- lapply(subset(seacardat, str_detect(seacardat, str_sub(file, str_locate(file, "Combined_WQ_WC_NUT_cont_Dissolved_Oxygen_Saturation.+NE")[1], str_locate(file, "Combined_WQ_WC_NUT_cont_Dissolved_Oxygen_Saturation.+NE")[2] - 2))), function(x){
        assign(paste0("cont_", which(str_detect(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_Dissolved_Oxygen_Saturation")), x))),
               fread(x, sep = "|", na.strings = nas))
        
        eval(as.name(paste0("cont_", which(str_detect(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_Dissolved_Oxygen_Saturation")), x)))))[, export := str_sub(x, 61, -1)]
      })
      
      lapply(cont_dat, function(x){
        pars_f <- data.table(file = unique(x$export),
                             param = unique(x$ParameterName))
        pars <<- rbind(pars, pars_f)
      })
      
    } else{
      seacardat_sub <- subset(seacardat, str_detect(seacardat, "Dissolved_Oxygen_Saturation", negate = TRUE))
      cont_dat <- lapply(subset(seacardat_sub, str_detect(seacardat_sub, str_sub(file, str_locate(file, "Combined_WQ_WC_NUT_cont_.+NE")[1], str_locate(file, "Combined_WQ_WC_NUT_cont_.+NE")[2] - 2))), function(x){
        assign(paste0("cont_", which(str_detect(subset(seacardat_sub, str_detect(seacardat_sub, "Combined_WQ_WC_NUT_cont_")), x))),
               fread(x, sep = "|", na.strings = nas))
        
        eval(as.name(paste0("cont_", which(str_detect(subset(seacardat_sub, str_detect(seacardat_sub, "Combined_WQ_WC_NUT_cont_")), x)))))[, export := str_sub(x, 61, -1)]
      })
      
      lapply(cont_dat, function(x){
        pars_f <- data.table(file = unique(x$export),
                             param = unique(x$ParameterName))
        pars <<- rbind(pars, pars_f)
      })
    }

    cont_dat <- rbindlist(cont_dat)
    cont_dat[, Habitat := "Water Column"]
    # cont_dat <- cont_dat[refdat[CombinedTable == "Continuous WQ" & ParameterName %in% unique(cont_dat$ParameterName), ], , on = c("Habitat", "ParameterName")]
    cont_dat <- merge(cont_dat, refdat[CombinedTable == "Continuous WQ" & ParameterName %in% unique(cont_dat$ParameterName), ], by = c("Habitat", "ParameterName"), all.x = TRUE)
    # setkey(refdat, Habitat, ParameterName)
    # setkey(cont_dat, Habitat, ParameterName)
    # cont_dat <- cont_dat[refdat[CombinedTable == "Continuous WQ", ], ]

    for(par in unique(cont_dat$ParameterName)){
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
                                                                                                         # type = NA_character_,
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

      cont_dat_par[, `:=` (# habitat = "Water Column (Continuous)",
                           # indicator = fcase(str_detect(par, "Temperature|Salinity|Oxygen|pH"), "Water Quality",
                           #                   str_detect(par, "Nitrogen|Phosphorus|NO2"), "Nutrients",
                           #                   str_detect(par, "Turbidity|Chlorophyll"), "Water Clarity"),
                           primaryHab = "Water Column",
                           primaryCombTab = "Continuous WQ",
                           primaryIndID = cont_dat[ParameterID == parid, unique(IndicatorID)],
                           primaryInd = cont_dat[ParameterID == parid, unique(IndicatorName)],
                           primaryParID = parid,
                           primaryParNm = cont_dat[ParameterID == parid, unique(ParameterName)],
                           primaryThrID = cont_dat[ParameterID == parid, unique(ThresholdID)],
                           QuadSize_m2 = NA,
                           #SpeciesGroup1 = NA,
                           pid = Sys.getpid(),
                           export = paste0("example: ", file_short))]

      qs_dat <- rbind(qs_dat, cont_dat_par)
      
      cat("\t", parid, ": ", par, "\n", sep = "")
    }
    
    qs <- rbind(qs, qs_dat)
    
    rm(cont_dat, cont_dat_par, pars_f)
    cat("\t Done! \n\n", sep = "")
    # qs_dat

  } else if(str_detect(file, "Combined_WQ_WC_NUT_")){
    
    dat <- fread(file, sep = "|", na.strings = nas)
    dat[, Habitat := "Water Column"]
    # dat1 <- dat[refdat[CombinedTable == "Discrete WQ" & Calculated == 0 & ParameterName %in% unique(dat$ParameterName), ], , on = c("Habitat", "ParameterName")]
    # dat2 <- dat[refdat[CombinedTable == "Discrete WQ" & Calculated == 1 & ParameterName %in% unique(dat$ParameterName), ], , on = c("Habitat", "ParameterName")]
    dat1 <- merge(dat[str_detect(SEACAR_QAQCFlagCode, "1Q", negate = TRUE), ], refdat[CombinedTable == "Discrete WQ" & Calculated == 0 & ParameterName %in% unique(dat$ParameterName), ], by = c("Habitat", "ParameterName"))
    dat2 <- merge(dat, refdat[CombinedTable == "Discrete WQ" & Calculated == 1 & ParameterName %in% unique(dat$ParameterName), ], by = c("Habitat", "ParameterName"))
    # setkey(refdat, Habitat, ParameterName)
    # setkey(dat, Habitat, ParameterName)
    # dat1 <- dat[refdat[CombinedTable == "Discrete WQ" & Calculated == 0, ], ]
    # dat2 <- dat[refdat[CombinedTable == "Discrete WQ" & Calculated == 1, ], ]
    dat <- rbind(dat1, dat2)
    
    pars_f <- data.table(file = file_short,
                         param = dat[!is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
      parid <- dat[ParameterName == par, unique(ParameterID)]
      
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
        # dat_par_all[, type := "All"]

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
        # dat_par_nocalc[, type := "No calc"]
        
        dat_par <- rbind(dat_par_all, dat_par_nocalc)
        
        dat_par[, `:=` (primaryHab = rep("Water Column", 2),
                        primaryCombTab = rep("Discrete WQ", 2),
                        primaryIndID = rep(dat[ParameterID == parid, unique(IndicatorID)], 2),
                        primaryInd = rep(dat[ParameterID == parid, unique(IndicatorName)], 2),
                        primaryParID = rep(parid, 2),
                        primaryParNm = rep(dat[ParameterID == parid, unique(ParameterName)], 2),
                        primaryThrID = dat[ParameterID == parid, sort(unique(ThresholdID), decreasing = TRUE)],
                        QuadSize_m2 = NA,
                        #SpeciesGroup1 = NA,
                        pid = Sys.getpid(),
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

      
      dat_par[, `:=` (# habitat = "Water Column (Discrete)",
                      # indicator = fcase(str_detect(par, "Temperature|Salinity|Oxygen|pH|Conductivity"), "Water Quality",
                      #                   str_detect(par, "Nitrogen|Phosphorus|NH3|NH4|Nitrate|Nitrite|NO2|PO4|Kjeldahl"), "Nutrients",
                      #                   str_detect(par, "Turbidity|Chlorophyll|Colored|Fluorescent|Secchi|Suspended"), "Water Clarity",
                      #                   str_detect(par, "Extinction"), "Additional Indicators"),
                      primaryHab = "Water Column",
                      primaryCombTab = "Discrete WQ",
                      primaryIndID = dat[ParameterID == parid, unique(IndicatorID)],
                      primaryInd = dat[ParameterID == parid, unique(IndicatorName)],
                      primaryParID = parid,
                      primaryParNm = dat[ParameterID == parid, unique(ParameterName)],
                      primaryThrID = dat[ParameterID == parid, unique(ThresholdID)],
                      QuadSize_m2 = NA,
                      #SpeciesGroup1 = NA,
                      pid = Sys.getpid(),
                      export = file_short)]
      
      }
      qs_dat <- rbind(qs_dat, dat_par)
      
      cat("\t", parid, ": ", par, "\n", sep = "")
      
    }
    
    qs <- rbind(qs, qs_dat)
    
    if(par == "Total Nitrogen"){
      rm(dat, dat_par, dat_par_all, dat_par_nocalc, pars_f)
    } else{
      rm(dat, dat_par, pars_f)
    }
    cat("\t Done! \n\n", sep = "")
    # qs_dat
    
  } else if(str_detect(file, "All_CW_Parameters")){
    
    # dat <- fread(file, sep = "|", na.string = nas)
    dat <- spec_dat
    
    # #Only needed for old-style wide-format exports
    # dat <- melt(dat, 
    #             measure.vars = c("[PercentCover-SpeciesComposition_%]", "[StemDensity_#/m2]", "[Total/CanopyPercentCover-SpeciesComposition_%]", "[BasalArea_m2/ha]"),
    #             variable.name = "ParameterName",
    #             value.name = "ResultValue")
    # dat[, ResultValue := as.numeric(ResultValue)]
    
    pars_f <- data.table(file = file_short,
                         param = dat[.id == "cw" & !is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    for(par in dat[.id == "cw", unique(ParameterName)]){
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
      
      dat_par[, `:=` (# habitat = "Coastal Wetlands",
                      # indicator = "Species Composition",
                      primaryHab = "Coastal Wetlands",
                      primaryCombTab = "CW",
                      primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(IndicatorID)], dat[ParameterID == parid, unique(IndicatorID)]),
                      primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(IndicatorName)], dat[ParameterID == parid, unique(IndicatorName)]),
                      primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(ParameterID)], parid),
                      primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(ParameterName)], dat[ParameterID == parid, unique(ParameterName)]),
                      primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(ThresholdID)], dat[ParameterID == parid, unique(ThresholdID)]),
                      QuadSize_m2 = NA,
                      #SpeciesGroup1 = NA,
                      pid = Sys.getpid())]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
      cat("\t", ifelse(purrr::is_empty(parid), refdat[CombinedTable == "CW" & ParameterName == par, unique(ParameterID)], parid), ": ", par, "\n", sep = "")
    }
    
    qs <- rbind(qs, qs_dat)
    
    rm(dat, dat_par, pars_f)
    cat("\t Done! \n\n", sep = "")
    # qs_dat

  } else if(str_detect(file, "All_CORAL_Parameters")){
    
    # #Only needed when coral exports were separated by region
    # coral_dat <- lapply(subset(seacardat, str_detect(seacardat, "Species Richness  - ")), function(x){
    #   assign(paste0("coral_", which(str_detect(subset(seacardat, str_detect(seacardat, "Species Richness  - ")), x))),
    #          fread(x))
    # })
    #
    # coral_dat <- rbindlist(coral_dat)
    
    # dat <- fread(file, sep = "|", na.strings = nas)
    dat <- spec_dat
    
    # #Only needed for old-style wide formatted exports
    # dat <- melt(coral_dat, 
    #             measure.vars = c("[PercentCover-SpeciesComposition_%]", "[%LiveTissue_%]", "Height_cm", "Width_cm", "Diameter_cm"),
    #             variable.name = "ParameterName",
    #             value.name = "ResultValue")
    # dat[, ResultValue := as.numeric(ResultValue)]
    
    pars_f <- data.table(file = file_short,
                         param = dat[.id == "coral" & !is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    for(par in dat[.id == "coral", unique(ParameterName)]){
      if(par %in% parstoskip) next
      
      parid <- dat[.id == "coral" & ParameterName == par & IndicatorName != "Grazers and Reef Dependent Species", unique(ParameterID)]
      
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
      
      dat_par[, `:=` (# habitat = "Coral Reef",
                      # indicator = fcase(str_detect(par, "Percent|Tissue|Diameter|Height|Width|Length"), "Percent Cover", # Colony|Height|Diameter|Width|Length|
                      #                   default = "Structural Community Composition"),
                      primaryHab = "Coral/Coral Reef",
                      primaryCombTab = "Coral",
                      primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(IndicatorID)], dat[ParameterID == parid, unique(IndicatorID)]),
                      primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(IndicatorName)], dat[ParameterID == parid, unique(IndicatorName)]),
                      primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(ParameterID)], parid),
                      primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(ParameterName)], dat[ParameterID == parid, unique(ParameterName)]),
                      primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(ThresholdID)], dat[ParameterID == parid, unique(ThresholdID)]),
                      QuadSize_m2 = NA,
                      #SpeciesGroup1 = NA,
                      pid = Sys.getpid())]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
      cat("\t", ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(ParameterID)], parid), ": ", par, "\n", sep = "")
    }
    
    qs <- rbind(qs, qs_dat)
    
    rm(dat, dat_par, pars_f)
    cat("\t Done! \n\n", sep = "")
    # qs_dat
    
  } else if(str_detect(file, "All_NEKTON_Parameters")){
    
    # dat <- fread(file, sep = "|", na.strings = nas)
    dat <- spec_dat
    
    # dat[EffortCorrection_100m2 > 0, ResultValue := ResultValue/EffortCorrection_100m2] #Effort correction not necessary - need to show Program editors values they can find in their raw data
    # dat[, `:=` (ParameterName = "Count/100m2 (effort corrected)")]  #Effort correction not necessary - need to show Program editors values they can find in their raw data
    dat[.id == "nekton" & SpeciesGroup1 == "", SpeciesGroup1 := NA]
    dat[.id == "nekton" & CommonIdentifier == "Ophiothrix angulata", SpeciesGroup1 := "Grazers and reef dependent species"]
    
    pars_f <- data.table(file = file_short,
                         param = dat[.id == "nekton" & !is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    for(par in dat[.id == "nekton", unique(ParameterName)]){
      if(par %in% parstoskip) next
      
      parid <- dat[.id == "nekton" & ParameterName == par & IndicatorName != "Grazers and Reef Dependent Species", unique(ParameterID)]
      
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
      
      dat_par[, `:=` (# habitat = "Water Column (Nekton)",
                      # indicator = "All Nekton",
                      primaryHab = "Water Column",
                      primaryCombTab = "Nekton",
                      primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(IndicatorID)], dat[ParameterID == parid, unique(IndicatorID)]),
                      primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(IndicatorName)], dat[ParameterID == parid, unique(IndicatorName)]),
                      primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(ParameterID)], parid),
                      primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(ParameterName)], dat[ParameterID == parid, unique(ParameterName)]),
                      primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(ThresholdID)], dat[ParameterID == parid, unique(ThresholdID)]),
                      QuadSize_m2 = NA,
                      #SpeciesGroup1 = NA,
                      pid = Sys.getpid())]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
      cat("\t", ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Nekton" & ParameterName == par, unique(ParameterID)], parid), ": ", par, "\n", sep = "")
      
      parid <- dat[.id == "coral" & ParameterName == par & IndicatorName == "Grazers and Reef Dependent Species", unique(ParameterID)]
      
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
      
      dat_par2[, `:=` (# habitat = "Coral Reef (Nekton)",
                       # indicator = "Grazers and Reef Dependent Species",
                       primaryHab = "Coral/Coral Reef",
                       primaryCombTab = "Coral",
                       primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(IndicatorID)], dat[ParameterID == parid, unique(IndicatorID)]),
                       primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(IndicatorName)], dat[ParameterID == parid, unique(IndicatorName)]),
                       primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(ParameterID)], parid),
                       primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(ParameterName)], dat[ParameterID == parid, unique(ParameterName)]),
                       primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(ThresholdID)], dat[ParameterID == parid & IndicatorName == "Grazers and Reef Dependent Species", unique(ThresholdID)]),
                       QuadSize_m2 = NA,
                       #SpeciesGroup1 = NA,
                       pid = Sys.getpid())]
      
      qs_dat <- distinct(rbind(qs_dat, dat_par2))
      
      cat("\t", ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & IndicatorName == "Grazers and Reef Dependent Species" & ParameterName == par, unique(ParameterID)], parid), ": ", par, "\n", sep = "")
    }
    
    qs <- rbind(qs, qs_dat)
    
    rm(dat, dat_par, dat_par2, pars_f)
    cat("\t Done! \n\n", sep = "")
    # qs_dat
    
  } else if(str_detect(file, "All_Oyster_Parameters")){
    
    dat <- fread(file, sep = "|", na.strings = nas)
    dat[QuadSize_m2 == 0.06, QuadSize_m2 := 0.0625]
    dat[ProgramID == 4042 & is.na(QuadSize_m2), QuadSize_m2 := fcase(SampleDate == as_date("2014-06-11"), 1,
                                                                     SampleDate >= as_date("2014-11-11") & SampleDate <= as_date("2015-01-22"), 0.33,
                                                                     SampleDate >= as_date("2015-03-04"), 0.0625)]
    dat[ProgramID == 5035, QuadSize_m2 := NA]
    dat[, Habitat := "Oyster/Oyster Reef"]
    # dat <- dat[refdat[CombinedTable == "Oyster", ], , on = c("Habitat", "ParameterName")]
    # dat1 <- dat[refdat[CombinedTable == "Oyster" & ParameterID %in% c(26, 51, 27), -c("QuadSize_m2")], , on = c("Habitat", "ParameterName")]
    # dat2 <- dat[refdat[CombinedTable == "Oyster" & !(ParameterID %in% c(26, 51, 27)), ], , on = c("Habitat", "ParameterName", "QuadSize_m2")]
    dat1 <- merge(dat, refdat[CombinedTable == "Oyster" & ParameterID %in% c(26, 51, 27), -c("QuadSize_m2")], by = c("Habitat", "ParameterName"))
    dat2 <- merge(dat, refdat[CombinedTable == "Oyster" & !(ParameterID %in% c(26, 51, 27)), ], by = c("Habitat", "ParameterName", "QuadSize_m2"))
    # setkey(refdat, Habitat, ParameterName)
    # setkey(dat, Habitat, ParameterName)
    # dat1 <- dat[refdat[CombinedTable == "Oyster" & ParameterID %in% c(26, 51, 27), ], ]
    # setkey(refdat, Habitat, ParameterName, QuadSize_m2)
    # setkey(dat, Habitat, ParameterName, QuadSize_m2)
    # dat2 <- dat[refdat[CombinedTable == "Oyster" & !(ParameterID %in% c(26, 51, 27)), ], ]
    dat <- rbind(dat1, dat2)
    
    # dat[ParameterName != "Density" & ParameterName != "Reef Height" & ParameterName != "Percent Live", ParameterName := paste0(ParameterName, "/", QuadSize_m2, "m2")]
    
    pars_f <- data.table(file = file_short,
                         param = dat[!is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
      parid <- dat[ParameterName == par, unique(ParameterID)]
      
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
        
        dat_par[, `:=` (# habitat = "Oyster Reef",
                        # indicator = fcase(par == "Density", "Density",
                        #                   par == "Percent Live", "Percent Live",
                        #                   par == "Reef Height", "Additional Indicators"),
                        primaryHab = "Oyster/Oyster Reef",
                        primaryCombTab = "Oyster",
                        primaryIndID = dat[ParameterID == parid, unique(IndicatorID)],
                        primaryInd = dat[ParameterID == parid, unique(IndicatorName)],
                        primaryParID = parid,
                        primaryParNm = dat[ParameterID == parid, unique(ParameterName)],
                        primaryThrID = dat[ParameterID == parid, unique(ThresholdID)],
                        QuadSize_m2 = NA,
                        # SpeciesGroup1 = NA,
                        pid = Sys.getpid(),
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
        
        dat_par[, `:=` (# habitat = "Oyster Reef",
                        # indicator = fcase(par == "Shell Height", "Size Class",
                        #                   default = "Additional Indicators"),
                        # SpeciesGroup1 = NA,
                        primaryHab = "Oyster/Oyster Reef",
                        primaryCombTab = "Oyster",
                        primaryIndID = dat[ParameterID == parid, unique(IndicatorID)],
                        primaryInd = dat[ParameterID == parid, unique(IndicatorName)],
                        primaryParID = parid,
                        primaryParNm = dat[ParameterID == parid, unique(ParameterName)],
                        primaryThrID = dat[ParameterID == parid, unique(ThresholdID)],
                        pid = Sys.getpid(),
                        export = file_short)]
      }

      qs_dat <- rbind(qs_dat, dat_par)
      
      cat("\t", parid, ": ", par, "\n", sep = "")
    }
    
    qs <- rbind(qs, qs_dat)
    
    rm(dat, dat_par, dat1, dat2, pars_f)
    cat("\t Done! \n\n", sep = "")
    # qs_dat
    
  } else {
    
    # dat <- fread(file, sep = "|", na.strings = nas)
    dat <- spec_dat
    # dat[.id == "sav" & ParameterName != "Shoot Count", type := ifelse(CommonIdentifier %in% c("Total_SAV", "Total SAV", "Total seagrass"), "Total", "By species")]
    
    pars_f <- data.table(file = file_short,
                         param = dat[.id == "sav" & !is.na(ResultValue), unique(ParameterName)])
    pars <- rbind(pars, pars_f)
    
    for(par in dat[.id == "sav", unique(ParameterName)]){
      if(par %in% parstoskip) next
      
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
        
        dat_par[, `:=` (primaryHab = "Submerged Aquatic Vegetation",
                        primaryCombTab = "SAV",
                        primaryIndID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par, unique(IndicatorID)], dat[ParameterID == parid, unique(IndicatorID)]),
                        primaryInd = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par, unique(IndicatorName)], dat[ParameterID == parid, unique(IndicatorName)]),
                        primaryParID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par, unique(ParameterID)], parid),
                        primaryParNm = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par, unique(ParameterName)], dat[ParameterID == parid, unique(ParameterName)]),
                        primaryThrID = ifelse(purrr::is_empty(parid), refdat[CombinedTable == "SAV" & ParameterName == par, unique(ThresholdID)], dat[ParameterID == parid, unique(ThresholdID)]),
                        QuadSize_m2 = NA,
                        #SpeciesGroup1 = NA,
                        pid = Sys.getpid())]
        
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
        
        
        dat_par[, `:=` (# habitat = "Submerged Aquatic Vegetation",
                        # indicator = fcase(par == "Shoot Count", "Additional Indicators",
                        #                   default = "Percent Cover"),
                        primaryHab = "Submerged Aquatic Vegetation",
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
                        #SpeciesGroup1 = NA,
                        pid = Sys.getpid()), by = isSpeciesSpec]
      
      }
      qs_dat <- rbind(qs_dat, dat_par)
      
      cat("\t", ifelse(purrr::is_empty(parid), refdat[CombinedTable == "Coral" & ParameterName == par, unique(ParameterID)], parid), ": ", par, "\n", sep = "")
      
    }
    
    qs <- rbind(qs, qs_dat)
    
    rm(dat, dat_par, pars_f)
    cat("\t Done! \n\n", sep = "")
    # qs_dat
  }
  
  gc()
}

# qs2 <- rbindlist(qs)

#Save the quantile results
qs2 <- copy(qs)
nums <- colnames(qs2[, .SD, .SDcols = is.numeric])
for(n in nums){
  if(n == "QuadSize_m2") next
  qs2[, (n) := plyr::round_any(eval(as.name(n)), 0.001)]
}

qs2[, `:=` (ScriptLatestRunVersion = paste0(currentscriptname, ", Git Commit ID: ", gitcommit), ScriptLatestRunDate = Sys.Date())] #parameterName = as.character(parameterName), 

####SAVE DETAIL FILE HERE
rawoutputname <- "Database_Thresholds_details"
rawoutputextension <- ".xlsx"
hs <- openxlsx::createStyle(textDecoration = "BOLD")
openxlsx::write.xlsx(qs2, here::here(paste0("output/ScriptResults/", rawoutputname, "_", str_replace_all(Sys.Date(), "-", ""), rawoutputextension)), colNames = TRUE, headerStyle = hs, colWidths = "auto")
openxlsx::write.xlsx(qs2, here::here(paste0("output/ScriptResults/", rawoutputname, rawoutputextension)), colNames = TRUE, headerStyle = hs, colWidths = "auto")

repo <- repository(here::here())
# config(repo, user.name = github_user, user.email = github_email)
add(repo, here::here(paste0("output/ScriptResults/", rawoutputname, "_", str_replace_all(Sys.Date(), "-", ""), rawoutputextension)))
add(repo, here::here(paste0("output/ScriptResults/", rawoutputname, rawoutputextension)))
commit(repo, paste0("Updated detailed indicator quantile results."))

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

qs2 <- merge(qs2, refdat[ThresholdID %in% unique(qs2$ThresholdID), .(ThresholdID, ActionNeeded, ActionNeededDate, AdditionalComments, Conversions, ExpectedValues, HighThreshold, LowThreshold, QuantileSource, Units)], by = "ThresholdID", all = TRUE)
qs2 <- rbind(qs2, refdat[ThresholdID %in% setdiff(refdat$ThresholdID, qs2$ThresholdID), ])
setkey(qs2, NULL)

setcolorder(qs2, c("ThresholdID", "ParameterID", "Habitat", "IndicatorID", "IndicatorName", "CombinedTable", "ParameterName", "Units", "LowThreshold", "HighThreshold", "QuadSize_m2", 
                   "ExpectedValues", "Conversions", "LowQuantile", "HighQuantile", "Calculated", "isSpeciesSpecific", "ActionNeeded", "ActionNeededDate", "QuantileSource", "AdditionalComments", 
                   "ScriptLatestRunVersion", "ScriptLatestRunDate"))
setorder(qs2, Habitat, IndicatorName, ParameterName, Calculated, isSpeciesSpecific, QuadSize_m2)

# fwrite(qs2, here::here(paste0("IndicatorQuantiles_", Sys.Date(), ".csv")))


#crosswalk the ParameterNames
# qs2[, parameter2 := parameter]
# qs2[str_detect(parameter, "Composition|Grazer"), parameter2 := fcase(str_detect(parameter, "Dependent"), str_replace(parameter, " - Grazers and Reef Dependent Species", ""),
#                                                                      str_detect(parameter, "dependent"), str_replace(parameter, " - Grazers and reef dependent species", ""),
#                                                                      str_detect(parameter, "Composition"), str_replace(parameter, " - Species Composition", ""))]
# 
# qs2[parameter2 %in% setdiff(sort(unique(qs2$parameter2)), sort(unique(refdat$ParameterName))), parameter2 := fcase(str_detect(parameter, "Tissue"), "Percent Live Tissue", 
#                                                                                                                    str_detect(parameter, "Ammonia"), "Ammonia, Un-ionized (NH3)",
#                                                                                                                    str_detect(parameter, "corrected"), "Chlorophyll a, Corrected for Pheophytin",
#                                                                                                                    str_detect(parameter, "uncorrected"), "Chlorophyll a, Uncorrected for Pheophytin",
#                                                                                                                    str_detect(parameter, "Colony"), "Density",
#                                                                                                                    str_detect(parameter, "Colored"), "Colored Dissolved Organic Matter",
#                                                                                                                    str_detect(parameter, "^Diameter"), "Colony Diameter",
#                                                                                                                    str_detect(parameter, "^Height"), "Colony Height",
#                                                                                                                    str_detect(parameter, "^Length"), "Colony Length",
#                                                                                                                    str_detect(parameter, "NH4"), "Ammonium, Filtered (NH4)",
#                                                                                                                    str_detect(parameter, "Nitrate"), "Nitrate (NO3)",
#                                                                                                                    str_detect(parameter, "Nitrite"), "Nitrite (NO2)",
#                                                                                                                    str_detect(parameter, "Nitrogen, organic"), "Nitrogen, Organic",
#                                                                                                                    str_detect(parameter, "NO2"), "NO2+3, Filtered",
#                                                                                                                    str_detect(parameter, "PO4"), "Phosphate, Filtered (PO4)",
#                                                                                                                    str_detect(parameter, "Presence"), "Presence/Absence",
#                                                                                                                    str_detect(parameter, "Kjeldahl"), "Total Kjeldahl Nitrogen",
#                                                                                                                    str_detect(parameter, "TSS"), "Total Suspended Solids")]
# qs2[habitat == "Coral Reef" & parameter == "Width", parameter2 := "Colony Width"]

gitcommit <- system("git rev-parse HEAD", intern=TRUE)
nchanges <- 0

#Update the quantile values for any parameter for which they have changed since the previous version of the reference workbook
# for(r in seq(1, nrow(qs2))){
#   
#   if(str_detect(qs2[r, combinedTable], "Continuous")){
#     refrow <- refdat[HabitatName == str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Continuous.")[1]) & IndicatorName == paste0(qs2[r, indicator], " - Continuous") & ParameterName == qs2[r, parameter2], ]
#     rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
#     
#   } else if(str_detect(qs2[r, habitat], "Discrete")){
#     if(is.na(qs2[r, type])){
#       refrow <- refdat[HabitatName == str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Discrete.")[1]) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2], ]
#       rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
#       
#     } else if(qs2[r, type] == "All"){
#       refrow <- refdat[HabitatName == str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Discrete.")[1]) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2] & str_detect(Comments, "calculated"), ]
#       rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName & str_detect(refdat$Comments, "calculated"))
#       
#     } else{
#       refrow <- refdat[HabitatName == str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Discrete.")[1]) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2] & str_detect(Comments, "calculated", negate = TRUE), ]
#       rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName & str_detect(refdat$Comments, "calculated", negate = TRUE))
#       
#     }
#     
#   } else if(str_detect(qs2[r, habitat], "Nekton")){
#     if(qs2[r, indicator] == "All Nekton"){
#       refrow <- refdat[str_detect(HabitatName, str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Nekton.")[1])) & IndicatorName == str_sub(qs2[r, indicator], str_locate(qs2[r, indicator], "All .")[2], -1) & ParameterName == qs2[r, parameter2], ]
#       rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
#       
#     } else{
#       refrow <- refdat[str_detect(HabitatName, str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Nekton.")[1])) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2], ]
#       rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
#       
#     }
#     
#   } else if(qs2[r, indicator] == "Structural Community Composition"){
#     refrow <- refdat[str_detect(HabitatName, qs2[r, habitat]) & IndicatorName == str_sub(qs2[r, indicator], str_locate(qs2[r, indicator], "Structural C")[2], -1) & ParameterName == qs2[r, parameter2], ]
#     rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
#     
#   } else if(!is.na(qs2[r, QuadSize_m2])){
#     refrow <- refdat[str_detect(HabitatName, qs2[r, habitat]) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2] & QuadSize_m2 == qs2[r, QuadSize_m2], ]
#     rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName & refdat$QuadSize_m2 == refrow$QuadSize_m2)
#     
#   } else if(!is.na(qs2[r, type])){
#     if(qs2[r, type] == "By species"){
#       refrow <- refdat[str_detect(HabitatName, qs2[r, habitat]) & IndicatorName == paste0(qs2[r, indicator], " (by species)") & ParameterName == qs2[r, parameter2], ]
#       rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
#       
#     } else if(qs2[r, type] == "Total"){
#       refrow <- refdat[str_detect(HabitatName, qs2[r, habitat]) & IndicatorName == paste0(qs2[r, indicator], " (total)") & ParameterName == qs2[r, parameter2], ]
#       rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
#       
#     }
#     
#   } else{
#     refrow <- refdat[str_detect(HabitatName, qs2[r, habitat]) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2] & is.na(QuadSize_m2), ]
#     rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName & is.na(refdat$QuadSize_m2))
#     
#   }
#   
#   if(length(rr_ind) > 1) stop("Something went wrong. Multiple reference table rows matched to the provided parameter info.")
#   if(length(rr_ind) == 0) stop("Something went wrong. Failed to find reference table row index.")
#   
#   if(refrow$LowQuantile != qs2[r, q_low]){
#     refdat[rr_ind, `:=` (LowQuantile = qs2[r, q_low], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuestionableSource = paste0("IndicatorQuantiles.xlsx, Git Commit ID: ", gitcommit))]
#     
#     nchanges <- nchanges + 1
#     
#   }
#   
#   if(refrow$HighQuantile != qs2[r, q_high]){
#     refdat[rr_ind, `:=` (HighQuantile = qs2[r, q_high], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuestionableSource = paste0("IndicatorQuantiles.xlsx, Git Commit ID: ", gitcommit))]
#     
#     nchanges <- nchanges + 1
#     
#   }
#   
#   cat("\r", r, " / ", nrow(qs2), " parameters checked. ", nchanges, " quantile values updated.")
#   
# }
n_id <- 0
for(t in sort(unique(qs2$ThresholdID))){
  n_id <- n_id + 1
  
  if(is.na(qs2[ThresholdID == t, LowQuantile]) & !is.na(refdat[ThresholdID == t, LowQuantile])){
    refdat[ThresholdID == t, `:=` (LowQuantile = qs2[ThresholdID == t, LowQuantile], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuantileSource = paste0(rawoutputname, rawoutputextension, ", Git Commit ID: ", gitcommit))]
    
    nchanges <- nchanges + 1
    
  }
  
  if(is.na(qs2[ThresholdID == t, HighQuantile]) & !is.na(refdat[ThresholdID == t, HighQuantile])){
    refdat[ThresholdID == t, `:=` (HighQuantile = qs2[ThresholdID == t, HighQuantile], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuantileSource = paste0(rawoutputname, rawoutputextension, ", Git Commit ID: ", gitcommit))]
    
    nchanges <- nchanges + 1
    
  }
  
  if(!is.na(qs2[ThresholdID == t, LowQuantile]) & refdat[ThresholdID == t, LowQuantile] != qs2[ThresholdID == t, LowQuantile]){
     refdat[ThresholdID == t, `:=` (LowQuantile = qs2[ThresholdID == t, LowQuantile], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuantileSource = paste0(rawoutputname, rawoutputextension, ", Git Commit ID: ", gitcommit))]
    
     nchanges <- nchanges + 1
    
  }
  
  if(!is.na(qs2[ThresholdID == t, HighQuantile]) & refdat[ThresholdID == t, HighQuantile] != qs2[ThresholdID == t, HighQuantile]){
     refdat[ThresholdID == t, `:=` (HighQuantile = qs2[ThresholdID == t, HighQuantile], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuantileSource = paste0(rawoutputname, rawoutputextension, ", Git Commit ID: ", gitcommit))]
    
     nchanges <- nchanges + 1
    
  }
  
  cat("\r", n_id, " / ", length(unique(qs2$ThresholdID)), " ThresholdIDs checked. ", nchanges, " quantile values updated.")
  
}

setkey(qs2, NULL)
setorder(qs2, Habitat, IndicatorName, ParameterName, Calculated, isSpeciesSpecific, QuadSize_m2)
setkey(refdat, NULL)
refdat[, `:=` (ScriptLatestRunVersion = paste0(currentscriptname, ", Git Commit ID: ", gitcommit), ScriptLatestRunDate = Sys.Date())]
setorder(refdat, Habitat, IndicatorName, ParameterName, Calculated, isSpeciesSpecific, QuadSize_m2)

if(is.logical(all.equal(qs2, refdat))){
  #Save updates to the reference sheet
  wb <- loadWorkbook(reffilepath)
  cols1 <- c(1:17)
  cols2 <- seq(18, ncol(refdat))
  writeData(wb, sheet = 1, refdat[, ..cols1], startRow = 3)
  writeData(wb, sheet = 1, refdat[, ..cols2], startRow = 3, startCol = 19)
  saveWorkbook(wb, here::here("output/ScriptResults/Database_Thresholds.xlsx"), overwrite = T)
  saveWorkbook(wb, here::here(paste0("output/ScriptResults/Database_Thresholds_", str_replace_all(Sys.Date(), "-", ""), ".xlsx")), overwrite = T)
  
  #Commit new files to Git
  add(repo, here::here("output/ScriptResults/Database_Thresholds.xlsx"))
  add(repo, here::here(paste0("output/ScriptResults/Database_Thresholds_", str_replace_all(Sys.Date(), "-", ""), ".xlsx")))
  commit(repo, paste0("Updated Database_Thresholds indicator quantile files."))
  
} else{
  stop("The quantile updates have been completed, but one or more mismatches between the quantile results and reference data tables remain. \nPlease resolve them and re-run the script. \n'all.equal(qs2, refdat)' result: ", all.equal(qs2, refdat))
  
}






