library(tidyverse)
library(data.table)
library(doFuture)

seacardat <- list.files(here::here("SEACARdata"), full.names = TRUE)
seacardat <- subset(seacardat, str_detect(seacardat, "NH4|NO2|PO4|Kjeldahl", negate = TRUE))
seacardat_forit <- c(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_", negate = TRUE)), subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_"))[1])

#Set quantile thresholds for flagging "questionable" values
quant_low <- 0.005
quant_high <- 0.999
num_sds <- 3

plan(multisession, workers = 12)

qs <- data.table(habitat = character(), 
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

qs <- foreach(file = seacardat_forit) %dofuture% {
  file_short <- str_sub(file, 61, -1)
  dat <- fread(file)
  qs_dat <- data.table(habitat = character(), 
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
  
  if(str_detect(file, "Combined_WQ_WC_NUT_cont_")){
    
   cont_dat <- lapply(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_")), function(x){
      assign(paste0("cont_", subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_"))[which(str_detect(subset(seacardat, str_detect(seacardat, "Combined_WQ_WC_NUT_cont_")), x))]),
             fread(x, sep = "|"))
    })
    
    cont_dat <- rbindlist(cont_dat)
    
    for(par in unique(cont_dat$ParameterName)){
      cont_dat_par <- cont_dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, .(parameter = unique(ParameterName),
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
                           pid = Sys.getpid(),
                           filename = paste0("example: ", file_short))]
      
      qs_dat <- rbind(qs_dat, cont_dat_par)
    }
    
  } else if(str_detect(file, "Combined_WQ_WC_NUT_")){
    for(par in unique(dat$ParameterName)){
      dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1 & Include == 1, .(parameter = unique(ParameterName),
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
      
      dat_par[, `:=` (habitat = "Water Column (Discrete)",
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
    
  } else if(str_detect(file, "All Parameters but Hecatres-2021-Jul-26.csv")){
    dat <- melt(dat, 
                measure.vars = c("[PercentCover-SpeciesComposition_%]", "[StemDensity_#/m2]", "[Total/CanopyPercentCover-SpeciesComposition_%]", "[BasalArea_m2/ha]"),
                variable.name = "ParameterName",
                value.name = "ResultValue")
    dat[, ResultValue := as.numeric(ResultValue)]
    
    for(par in unique(dat$ParameterName)){
      dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, .(parameter = unique(ParameterName),
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
    
  } else {
    for(par in unique(dat$ParameterName)){
      dat_par <- dat[ParameterName == par & !is.na(ResultValue) & MADup == 1, .(parameter = unique(ParameterName),
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
      
      dat_par[, `:=` (habitat = fcase(str_detect(file, "Oyster"), "Oyster Reef",
                                      str_detect(file, "SAV"), "Submerged Aquatic Vegetation",
                                      str_detect(file, "Nekton"), "Water Column (Nekton)"),
                      pid = Sys.getpid(),
                      filename = file_short)]
      
      qs_dat <- rbind(qs_dat, dat_par)
    }
  }
  
  # print(paste0(file_short, " done"))
  rbind(qs, qs_dat)
}

qs2 <- rbindlist(qs)
nums <- colnames(qs2[, .SD, .SDcols = is.numeric])
for(n in nums){
  qs2[, (n) := plyr::round_any(eval(as.name(n)), 0.001)]
}

fwrite(qs2, here::here(paste0("IndicatorQuantiles_", Sys.Date(), ".csv")))
