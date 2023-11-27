library(tidyverse)
library(data.table)
library(doFuture)
library(lubridate)
library(stringr)
library(openxlsx)
options(scipen = 999)
# options(future.globals.maxSize = 6291456000) #only necessary if using the parallel processing version of the script

#Process new data export downloads if needed
downloaddate <- as_date("2023-11-20")
zips <- file.info(list.files("C:/Users/steph/Downloads/", full.names = TRUE, pattern="*.zip"))
zips <- subset(zips, date(zips$mtime) == downloaddate)

for(z in row.names(zips)){
  unzip(z, exdir = here::here("SEACARdata"), junkpaths = TRUE)
  
  while(TRUE %in% str_detect(list.files(here::here("SEACARdata")), ".zip$")){
    for(zz in list.files(here::here("SEACARdata"), full.names = TRUE, pattern = ".zip$")){
      unzip(zz, exdir = here::here("SEACARdata"), junkpaths = TRUE)
      file.remove(zz)
    }
  }
  # file.remove(z)
}

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
nas <- c("NULL")

spec_dat <- lapply(speciesdat, function(x){
                   assign("dt", fread(x, sep = "|", na.strings = nas))
                   dt[, export := str_sub(x, 61, -1)]
            })

spec_dat <- rbindlist(spec_dat, fill = TRUE, idcol = TRUE)


plan(multisession, workers = 10)

qs <- data.table(habitat = character(),
                 indicator = character(),
                 type = character(),
                 parameter = character(),
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

#Build new quantiles summary data
# qs <- foreach(file = seacardat_forit, .combine = rbind) %dofuture% {
for(file in seacardat_forit){
  file_short <- str_sub(file, 61, -1)
  qs_dat <- data.table(habitat = character(),
                       indicator = character(),
                       type = character(), 
                       parameter = character(),
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
  
  # #skip all continuous WQ datasets because they all have low/high thresholds already
  if(str_detect(file, "Combined_WQ_WC_NUT_cont_")){ #next
    
   cont_dat <- lapply(subset(seacardat, str_detect(seacardat, str_sub(file, str_locate(file, "Combined_WQ_WC_NUT_cont_.+NE")[1], str_locate(file, "Combined_WQ_WC_NUT_cont_.+NE")[2] - 2))), function(x){
      assign(paste0("cont_", which(str_detect(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_")), x))),
             fread(x, sep = "|", na.strings = nas))
    })

    cont_dat <- rbindlist(cont_dat)

    for(par in unique(cont_dat$ParameterName)){
      if(par %in% parstoskip) next

      cont_dat_par <- cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, .(parameter = unique(ParameterName),
                                                                                                         type = NA_character_,
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

      cont_dat_par[, `:=` (habitat = "Water Column (Continuous)",
                           indicator = fcase(str_detect(par, "Temperature|Salinity|Oxygen|pH"), "Water Quality",
                                             str_detect(par, "Nitrogen|Phosphorus|NO2"), "Nutrients",
                                             str_detect(par, "Turbidity|Chlorophyll"), "Water Clarity"),
                           QuadSize_m2 = NA,
                           #SpeciesGroup1 = NA,
                           pid = Sys.getpid(),
                           export = paste0("example: ", file_short))]

      qs_dat <- rbind(qs_dat, cont_dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    # qs_dat

  } else if(str_detect(file, "Combined_WQ_WC_NUT_")){
    
    dat <- fread(file, sep = "|", na.strings = nas)
    
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
        dat_par_all[, type := "All"]

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
        dat_par_nocalc[, type := "No calc"]
        
        dat_par <- rbind(dat_par_all, dat_par_nocalc)
        
        
      } else{
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, .(parameter = unique(ParameterName),
                                                                                                 type = NA_character_,
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
      
      dat_par[, `:=` (habitat = "Water Column (Discrete)",
                      indicator = fcase(str_detect(par, "Temperature|Salinity|Oxygen|pH|Conductivity"), "Water Quality",
                                        str_detect(par, "Nitrogen|Phosphorus|NH3|NH4|Nitrate|Nitrite|NO2|PO4|Kjeldahl"), "Nutrients",
                                        str_detect(par, "Turbidity|Chlorophyll|Colored|Fluorescent|Secchi|Suspended"), "Water Clarity",
                                        str_detect(par, "Extinction"), "Additional Indicators"),
                      QuadSize_m2 = NA,
                      #SpeciesGroup1 = NA,
                      pid = Sys.getpid(),
                      export = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
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
    
    for(par in dat[.id == "cw", unique(ParameterName)]){
      if(par %in% parstoskip) next
      
      dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & SpeciesGroup1 != "Seagrass", .(parameter = unique(ParameterName),
                                                                                                              type = NA_character_,
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
      
      dat_par[, `:=` (habitat = "Coastal Wetlands",
                      indicator = "Species Composition",
                      QuadSize_m2 = NA,
                      #SpeciesGroup1 = NA,
                      pid = Sys.getpid())]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    # qs_dat

  } else if(str_detect(file, "All_CORAL_Parameters")){
    
    # #Only needed when coral exports were speparated by region
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
    
    
    for(par in dat[.id == "coral", unique(ParameterName)]){
      if(par %in% parstoskip) next
      
      dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & !(SpeciesGroup1 %in% c("Grazers and reef dependent species", "Reef Fish", "Macroalgae", "Seagrass")), .(parameter = unique(ParameterName),
                                                                                                                          type = NA_character_,
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
      
      dat_par[, `:=` (habitat = "Coral Reef",
                      indicator = fcase(str_detect(par, "Percent|Tissue|Diameter|Height|Width|Length"), "Percent Cover", # Colony|Height|Diameter|Width|Length|
                                        default = "Structural Community Composition"),
                      QuadSize_m2 = NA,
                      #SpeciesGroup1 = NA,
                      pid = Sys.getpid())]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    # qs_dat
    
  } else if(str_detect(file, "All_NEKTON_Parameters")){
    
    # dat <- fread(file, sep = "|", na.strings = nas)
    dat <- spec_dat
    
    # dat[EffortCorrection_100m2 > 0, ResultValue := ResultValue/EffortCorrection_100m2] #Effort correction not necessary - need to show Program editors values they can find in their raw data
    # dat[, `:=` (ParameterName = "Count/100m2 (effort corrected)")]  #Effort correction not necessary - need to show Program editors values they can find in their raw data
    dat[.id == "nekton" & SpeciesGroup1 == "", SpeciesGroup1 := NA]
    dat[.id == "nekton" & CommonIdentifier == "Ophiothrix angulata", SpeciesGroup1 := "Grazers and reef dependent species"]
    
    for(par in dat[.id == "nekton", unique(ParameterName)]){
      if(par %in% parstoskip) next
      
      dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & str_detect(SpeciesGroup2, "Nekton"), .(parameter = unique(ParameterName),
                                                                                type = NA_character_,
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
      
      dat_par[, `:=` (habitat = "Water Column (Nekton)",
                      indicator = "All Nekton",
                      QuadSize_m2 = NA,
                      #SpeciesGroup1 = NA,
                      pid = Sys.getpid())]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
      dat_par2 <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & SpeciesGroup1 %in% c("Grazers and reef dependent species", "Reef Fish"), .(parameter = paste0(unique(ParameterName), " - Grazers and reef dependent species"),
                                                                                                                                                           type = NA_character_,
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
      
      dat_par2[, `:=` (habitat = "Coral Reef (Nekton)",
                       indicator = "Grazers and Reef Dependent Species",
                       QuadSize_m2 = NA,
                       #SpeciesGroup1 = NA,
                       pid = Sys.getpid())]
      
      qs_dat <- rbind(qs_dat, dat_par2)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    # qs_dat
    
  } else if(str_detect(file, "All_Oyster_Parameters")){
    
    dat <- fread(file, sep = "|", na.strings = nas)
    dat[QuadSize_m2 == 0.06, QuadSize_m2 := 0.0625]
    dat[ProgramID == 4042 & is.na(QuadSize_m2), QuadSize_m2 := fcase(SampleDate == as_date("2014-06-11"), 1,
                                                                     SampleDate >= as_date("2014-11-11") & SampleDate <= as_date("2015-01-22"), 0.33,
                                                                     SampleDate >= as_date("2015-03-04"), 0.0625)]
    dat[ProgramID == 5035, QuadSize_m2 := NA]
    
    # dat[ParameterName != "Density" & ParameterName != "Reef Height" & ParameterName != "Percent Live", ParameterName := paste0(ParameterName, "/", QuadSize_m2, "m2")]
    
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
      if(par %in% c("Density", "Reef Height", "Percent Live")){
        
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, .(parameter = unique(ParameterName),
                                                                                  type = NA_character_,
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
        
        dat_par[, `:=` (habitat = "Oyster Reef",
                        indicator = fcase(par == "Density", "Density",
                                          par == "Percent Live", "Percent Live",
                                          par == "Reef Height", "Additional Indicators"),
                        QuadSize_m2 = NA,
                        #SpeciesGroup1 = NA,
                        pid = Sys.getpid(),
                        export = file_short)]

      } else{
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, .(parameter = unique(ParameterName),
                                                                                  type = NA_character_,
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
        
        dat_par[, `:=` (habitat = "Oyster Reef",
                        indicator = fcase(par == "Shell Height", "Size Class",
                                          default = "Additional Indicators"),
                        #SpeciesGroup1 = NA,
                        pid = Sys.getpid(),
                        export = file_short)]
      }

      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    # qs_dat
    
  } else {
    
    # dat <- fread(file, sep = "|", na.strings = nas)
    dat <- spec_dat
    dat[.id == "sav" & ParameterName != "Shoot Count", type := ifelse(CommonIdentifier %in% c("Total_SAV", "Total SAV", "Total seagrass"), "Total", "By species")]
    
    for(par in dat[.id == "sav", unique(ParameterName)]){
      if(par %in% parstoskip) next
      
      if(str_detect(par, "Presence|Blanquet")){
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & SpeciesGroup1 %in% c("Seagrass", "Macroalgae") & type == "By species", .(parameter = unique(ParameterName),
                                                                                                                                   type = "By species",
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
        
      } else{
        dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & SpeciesGroup1 %in% c("Seagrass", "Macroalgae"), .(parameter = unique(ParameterName),
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
                                                                                                                                   n_sdn_high = nrow(dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & ResultValue > dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, mean(ResultValue) + (num_sds * sd(ResultValue))], ])), by = type]
        
      }
      
      dat_par[, `:=` (habitat = "Submerged Aquatic Vegetation",
                      indicator = fcase(par == "Shoot Count", "Additional Indicators",
                                        default = "Percent Cover"),
                      QuadSize_m2 = NA,
                      #SpeciesGroup1 = NA,
                      pid = Sys.getpid())]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    # qs_dat
  }
}

# qs2 <- rbindlist(qs)
qs2 <- copy(qs)
nums <- colnames(qs2[, .SD, .SDcols = is.numeric])
for(n in nums){
  if(n == "QuadSize_m2") next
  qs2[, (n) := plyr::round_any(eval(as.name(n)), 0.001)]
}
qs2[, parameter := as.character(parameter)]
setcolorder(qs2, c("habitat", "indicator", "parameter", "type", "QuadSize_m2", "median", "iqr", "qval_low", "qval_high", "q_low", "q_high", "mean", "sd", 
                   "num_sds", "sdn_low", "sdn_high", "n_tot", "n_q_low", "n_q_high", "n_sdn_low", "n_sdn_high", "pid", "export"))
setorder(qs2, habitat, indicator, parameter, type, QuadSize_m2)

# fwrite(qs2, here::here(paste0("IndicatorQuantiles_", Sys.Date(), ".csv")))
hs <- openxlsx::createStyle(textDecoration = "BOLD")
openxlsx::write.xlsx(qs2, here::here(paste0("IndicatorQuantiles_", Sys.Date(), ".xlsx")), colNames = TRUE, headerStyle = hs, colWidths = "auto")
openxlsx::write.xlsx(qs2, here::here(paste0("IndicatorQuantiles.xlsx")), colNames = TRUE, headerStyle = hs, colWidths = "auto")


ref <- list.files(here::here(), pattern = "Ref_Parameters_Thresholds.xlsx")
ref_info <- file.info(here::here(ref))
print(paste0("Note: The supplied ref. file (", ref, ") was created ", ref_info$ctime, " and last modified ", ref_info$mtime, ". Proceed if you are sure this is the most up-to-date version."))

refdat <- openxlsx::read.xlsx(here::here(ref), startRow = 4)
setDT(refdat)
refdat[, ActionNeededDate := as_date(ActionNeededDate, origin = "1899-12-30")]

#crosswalk the ParameterNames
qs2[, parameter2 := parameter]
qs2[str_detect(parameter, "Composition|Grazer"), parameter2 := fcase(str_detect(parameter, "Dependent"), str_replace(parameter, " - Grazers and Reef Dependent Species", ""),
                                                                     str_detect(parameter, "dependent"), str_replace(parameter, " - Grazers and reef dependent species", ""),
                                                                     str_detect(parameter, "Composition"), str_replace(parameter, " - Species Composition", ""))]

qs2[parameter2 %in% setdiff(sort(unique(qs2$parameter2)), sort(unique(refdat$ParameterName))), parameter2 := fcase(str_detect(parameter, "Tissue"), "Percent Live Tissue", 
                                                                                                                   str_detect(parameter, "Ammonia"), "Ammonia, Un-ionized (NH3)",
                                                                                                                   str_detect(parameter, "corrected"), "Chlorophyll a, Corrected for Pheophytin",
                                                                                                                   str_detect(parameter, "uncorrected"), "Chlorophyll a, Uncorrected for Pheophytin",
                                                                                                                   str_detect(parameter, "Colony"), "Density",
                                                                                                                   str_detect(parameter, "Colored"), "Colored Dissolved Organic Matter",
                                                                                                                   str_detect(parameter, "^Diameter"), "Colony Diameter",
                                                                                                                   str_detect(parameter, "^Height"), "Colony Height",
                                                                                                                   str_detect(parameter, "^Length"), "Colony Length",
                                                                                                                   str_detect(parameter, "NH4"), "Ammonium, Filtered (NH4)",
                                                                                                                   str_detect(parameter, "Nitrate"), "Nitrate (NO3)",
                                                                                                                   str_detect(parameter, "Nitrite"), "Nitrite (NO2)",
                                                                                                                   str_detect(parameter, "Nitrogen, organic"), "Nitrogen, Organic",
                                                                                                                   str_detect(parameter, "NO2"), "NO2+3, Filtered",
                                                                                                                   str_detect(parameter, "PO4"), "Phosphate, Filtered (PO4)",
                                                                                                                   str_detect(parameter, "Presence"), "Presence/Absence",
                                                                                                                   str_detect(parameter, "Kjeldahl"), "Total Kjeldahl Nitrogen",
                                                                                                                   str_detect(parameter, "TSS"), "Total Suspended Solids")]
qs2[habitat == "Coral Reef" & parameter == "Width", parameter2 := "Colony Width"]

gitcommit <- system("git rev-parse HEAD", intern=TRUE)
nchanges <- 0

#Update the quantile values for any parameter for which they have changed since the previous version of the reference workbook
for(r in seq(1, nrow(qs2))){
  
  if(str_detect(qs2[r, habitat], "Continuous")){
    refrow <- refdat[HabitatName == str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Continuous.")[1]) & IndicatorName == paste0(qs2[r, indicator], " - Continuous") & ParameterName == qs2[r, parameter2], ]
    rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
    
  } else if(str_detect(qs2[r, habitat], "Discrete")){
    if(is.na(qs2[r, type])){
      refrow <- refdat[HabitatName == str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Discrete.")[1]) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2], ]
      rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
      
    } else if(qs2[r, type] == "All"){
      refrow <- refdat[HabitatName == str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Discrete.")[1]) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2] & str_detect(Comments, "calculated"), ]
      rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName & str_detect(refdat$Comments, "calculated"))
      
    } else{
      refrow <- refdat[HabitatName == str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Discrete.")[1]) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2] & str_detect(Comments, "calculated", negate = TRUE), ]
      rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName & str_detect(refdat$Comments, "calculated", negate = TRUE))
      
    }
    
  } else if(str_detect(qs2[r, habitat], "Nekton")){
    if(qs2[r, indicator] == "All Nekton"){
      refrow <- refdat[str_detect(HabitatName, str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Nekton.")[1])) & IndicatorName == str_sub(qs2[r, indicator], str_locate(qs2[r, indicator], "All .")[2], -1) & ParameterName == qs2[r, parameter2], ]
      rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
      
    } else{
      refrow <- refdat[str_detect(HabitatName, str_sub(qs2[r, habitat], 1, str_locate(qs2[r, habitat], ". .Nekton.")[1])) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2], ]
      rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
      
    }
    
  } else if(qs2[r, indicator] == "Structural Community Composition"){
    refrow <- refdat[str_detect(HabitatName, qs2[r, habitat]) & IndicatorName == str_sub(qs2[r, indicator], str_locate(qs2[r, indicator], "Structural C")[2], -1) & ParameterName == qs2[r, parameter2], ]
    rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
    
  } else if(!is.na(qs2[r, QuadSize_m2])){
    refrow <- refdat[str_detect(HabitatName, qs2[r, habitat]) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2] & QuadSize_m2 == qs2[r, QuadSize_m2], ]
    rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName & refdat$QuadSize_m2 == refrow$QuadSize_m2)
    
  } else if(!is.na(qs2[r, type])){
    if(qs2[r, type] == "By species"){
      refrow <- refdat[str_detect(HabitatName, qs2[r, habitat]) & IndicatorName == paste0(qs2[r, indicator], " (by species)") & ParameterName == qs2[r, parameter2], ]
      rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
      
    } else if(qs2[r, type] == "Total"){
      refrow <- refdat[str_detect(HabitatName, qs2[r, habitat]) & IndicatorName == paste0(qs2[r, indicator], " (total)") & ParameterName == qs2[r, parameter2], ]
      rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName)
      
    }
    
  } else{
    refrow <- refdat[str_detect(HabitatName, qs2[r, habitat]) & IndicatorName == qs2[r, indicator] & ParameterName == qs2[r, parameter2] & is.na(QuadSize_m2), ]
    rr_ind <- which(refdat$HabitatName == refrow$HabitatName & refdat$IndicatorName == refrow$IndicatorName & refdat$ParameterName == refrow$ParameterName & is.na(refdat$QuadSize_m2))
    
  }
  
  if(length(rr_ind) > 1) stop("Something went wrong. Multiple reference table rows matched to the provided parameter info.")
  if(length(rr_ind) == 0) stop("Something went wrong. Failed to find reference table row index.")
  
  if(refrow$LowQuantile != qs2[r, q_low]){
    refdat[rr_ind, `:=` (LowQuantile = qs2[r, q_low], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuestionableSource = paste0("IndicatorQuantiles.xlsx, Git Commit ID: ", gitcommit))]
    
    nchanges <- nchanges + 1
    
  }
  
  if(refrow$HighQuantile != qs2[r, q_high]){
    refdat[rr_ind, `:=` (HighQuantile = qs2[r, q_high], ActionNeeded = "U", ActionNeededDate = Sys.Date(), QuestionableSource = paste0("IndicatorQuantiles.xlsx, Git Commit ID: ", gitcommit))]
    
    nchanges <- nchanges + 1
    
  }
  
  cat("\r", r, " / ", nrow(qs2), " parameters checked. ", nchanges, " quantile values updated.")
  
}


#Save updates to the reference sheet if necessary
if(nchanges > 0){
  wb <- loadWorkbook(here::here("Ref_Parameters_Thresholds_20231127.xlsx"))
  writeData(wb, sheet = 1, refdat, startRow = 4)
  saveWorkbook(wb, here::here("Ref_Parameters_Thresholds.xlsx"), overwrite = T)
}




