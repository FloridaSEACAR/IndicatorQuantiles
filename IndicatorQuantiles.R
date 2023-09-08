library(tidyverse)
library(data.table)
library(doFuture)
library(lubridate)
library(stringr)

#Process new data export downloads if needed
downloaddate <- as_date("2023-08-22")
zips <- file.info(list.files("C:/Users/steph/Downloads", full.names = TRUE, pattern="*.zip"))
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
seacardat <- list.files(here::here("SEACARdata"), full.names = TRUE)

#Set which parameters to skip (e.g., those with DEAR thresholds and/or expected values)
seacardat <- subset(seacardat, str_detect(seacardat, "Oxygen|pH|Secchi|Salinity|Conductivity|Temperature|Blanquet|Percent", negate = TRUE))
parstoskip <- c("[PercentCover-SpeciesComposition_%]", 
                "[Total/CanopyPercentCover-SpeciesComposition_%]", 
                "Percent Live",
                "[PercentCover-SpeciesComposition_%]",
                "[%LiveTissue_%]",
                "Presence")

#Remove all but one file for each habitat x parameter
seacardat_forit <- c(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_|Species Richness  - ", negate = TRUE)), subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_"))[1]) #, subset(seacardat, str_detect(seacardat, "Species Richness  - "))[1])

#Set quantile thresholds for flagging "questionable" values
quant_low <- 0.001
quant_high <- 0.999
num_sds <- 3

#What are the strings that need to be interpreted as NA values?
nas <- c("NULL")

plan(multisession, workers = 10)

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

# qs <- foreach(file = seacardat_forit) %dofuture% {
for(file in seacardat_forit){
  file_short <- str_sub(file, 61, -1)
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
  
  #skip all continuous WQ datasets because they all have low/high thresholds already
  if(str_detect(file, "Combined_WQ_WC_NUT_cont_")) next #{
    
  #  cont_dat <- lapply(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_")), function(x){
  #     assign(paste0("cont_", which(str_detect(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_")), x))),
  #            fread(x, sep = "|", na.strings = nas))
  #   })
  # 
  #   cont_dat <- rbindlist(cont_dat)
  # 
  #   for(par in unique(cont_dat$ParameterName)){
  #     if(par %in% parstoskip) next
  # 
  #     cont_dat_par <- cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, .(parameter = unique(ParameterName),
  #                                                                                                        tn = NA,
  #                                                                                                        median = median(ResultValue),
  #                                                                                                        iqr = IQR(ResultValue),
  #                                                                                                        qval_low = quant_low,
  #                                                                                                        qval_high = quant_high,
  #                                                                                                        q_low = quantile(ResultValue, probs = quant_low),
  #                                                                                                        q_high = quantile(ResultValue, probs = quant_high),
  #                                                                                                        mean = mean(ResultValue),
  #                                                                                                        sd = sd(ResultValue),
  #                                                                                                        num_sds = num_sds,
  #                                                                                                        sdn_low = mean(ResultValue) - (num_sds * sd(ResultValue)),
  #                                                                                                        sdn_high = mean(ResultValue) + (num_sds * sd(ResultValue)),
  #                                                                                                        n_tot = length(ResultValue),
  #                                                                                                        n_q_low = nrow(cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue < cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, quantile(ResultValue, probs = quant_low)], ]),
  #                                                                                                        n_q_high = nrow(cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue > cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, quantile(ResultValue, probs = quant_high)], ]),
  #                                                                                                        n_sdn_low = nrow(cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue < cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, mean(ResultValue) - (num_sds * sd(ResultValue))], ]),
  #                                                                                                        n_sdn_high = nrow(cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1 & ResultValue > cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, mean(ResultValue) + (num_sds * sd(ResultValue))], ]))]
  # 
  #     cont_dat_par[, `:=` (habitat = "Water Column (Continuous)",
  #                          pid = Sys.getpid(),
  #                          filename = paste0("example: ", file_short))]
  # 
  #     qs_dat <- rbind(qs_dat, cont_dat_par)
  #   }
  #   
  # } else 
  if(str_detect(file, "Combined_WQ_WC_NUT_")){
    
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
      
      dat_par[, `:=` (habitat = "Water Column (Discrete)",
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    
    
  } else if(str_detect(file, "All_CW_Parameters")){
    
    dat <- fread(file, sep = "|", na.string = nas)
    
    # #Only needed for old-style wide-format exports
    # dat <- melt(dat, 
    #             measure.vars = c("[PercentCover-SpeciesComposition_%]", "[StemDensity_#/m2]", "[Total/CanopyPercentCover-SpeciesComposition_%]", "[BasalArea_m2/ha]"),
    #             variable.name = "ParameterName",
    #             value.name = "ResultValue")
    # dat[, ResultValue := as.numeric(ResultValue)]
    
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
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
      
      dat_par[, `:=` (habitat = "Coastal Wetlands",
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    

  } else if(str_detect(file, "All_CORAL_Parameters")){
    
    # #Only needed when coral exports were speparated by region
    # coral_dat <- lapply(subset(seacardat, str_detect(seacardat, "Species Richness  - ")), function(x){
    #   assign(paste0("coral_", which(str_detect(subset(seacardat, str_detect(seacardat, "Species Richness  - ")), x))),
    #          fread(x))
    # })
    #
    # coral_dat <- rbindlist(coral_dat)
    
    dat <- fread(file, sep = "|", na.strings = nas)
    
    # #Only needed for old-style wide formatted exports
    # dat <- melt(coral_dat, 
    #             measure.vars = c("[PercentCover-SpeciesComposition_%]", "[%LiveTissue_%]", "Height_cm", "Width_cm", "Diameter_cm"),
    #             variable.name = "ParameterName",
    #             value.name = "ResultValue")
    # dat[, ResultValue := as.numeric(ResultValue)]
    
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
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
      
      dat_par[, `:=` (habitat = "Coral Reef",
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    
    
  } else if(str_detect(file, "All_NEKTON_Parameters")){
    
    dat <- fread(file, sep = "|", na.strings = nas)
    
    dat[EffortCorrection_100m2 > 0, ResultValue := ResultValue/EffortCorrection_100m2]
    dat[, `:=` (ParameterName = "Count/100m2 (effort corrected)")]
    dat[SpeciesGroup1 == "", SpeciesGroup1 := NA]
    dat[CommonIdentifier == "Ophiothrix angulata", SpeciesGroup1 := "Grazers and reef dependent species"]
    
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
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
      
      dat_par[, `:=` (habitat = "Water Column (Nekton)",
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
      
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
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    
    
  } else if(str_detect(file, "All_Oyster_Parameters")){
    
    dat <- fread(file, sep = "|", na.strings = nas)
    
    dat[ParameterName != "Density" & ParameterName != "Reef Height" & ParameterName != "Percent Live", ParameterName := paste0(ParameterName, "/", QuadSize_m2, "m2")]
    
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
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
      
      dat_par[, `:=` (habitat = "Oyster Reef",
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    
  } else {
    
    dat <- fread(file, sep = "|", na.strings = nas)
    
    for(par in unique(dat$ParameterName)){
      if(par %in% parstoskip) next
      
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
      
      dat_par[, `:=` (habitat = "Submerged Aquatic Vegetation",
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
    qs <- rbind(qs, qs_dat)
    print(paste0(file_short, " done"))
    
  }
}

qs2 <- rbindlist(qs)
qs2 <- qs
nums <- colnames(qs2[, .SD, .SDcols = is.numeric])
for(n in nums){
  qs2[, (n) := plyr::round_any(eval(as.name(n)), 0.001)]
}
qs2[, parameter := as.character(parameter)]
setorder(qs2, habitat, parameter)

# fwrite(qs2, here::here(paste0("IndicatorQuantiles_", Sys.Date(), ".csv")))
hs <- openxlsx::createStyle(textDecoration = "BOLD")
openxlsx::write.xlsx(qs2, here::here(paste0("IndicatorQuantiles_", Sys.Date(), ".xlsx")), colNames = TRUE, headerStyle = hs, colWidths = "auto")


