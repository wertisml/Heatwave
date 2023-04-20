library(data.table)
library(dplyr)
library(arrow)
library(humidity)

setwd("~/Heatwave/Data/Zip")

#==============================================================================#
#Calculate the 85th percentile EHF
#==============================================================================#

Heatwave <- left_join(open_dataset("NC_Heatwave.parquet") %>%
                        mutate(Severe_occurance = ifelse(EHF > 0, 1, 0),
                               daynum = 1) %>%
                        collect() %>%
                        drop_na(.),
                      open_dataset("NC_Heatwave.parquet") %>% 
                        select(Zip, EHF) %>%
                        group_by(Zip) %>% 
                        collect() %>%
                        filter(EHF > 0) %>%
                        do(data.frame(t(quantile(.$EHF, probs = c(0.85), na.rm=T)))) %>%
                        rename(EHF85 = X85.),
                      by = c("Zip"))

#==============================================================================#
#Calculate the daily information 
#==============================================================================#

Heat_Calc <- Heatwave %>%
  group_by(Zip) %>%
  mutate(Severe_Heatwaves = (ifelse(EHF >= EHF85, 1, 0)),
         Extreme_Heatwaves = (ifelse(EHF >= (3 * EHF85), 1, 0)),
         low_intensity = (ifelse(EHF > 0 & EHF < 1, 1, 0)),
         moderate_intensity = (ifelse(EHF >= 1 & EHF < 2, 1, 0)),  
         high_intensity = (ifelse(EHF >= 2, 1, 0)),
         Above_95th = fifelse(TAVG > H95, 1, 0),
         Above_97th  = fifelse(TAVG > H97, 1, 0),
         Above_99th = fifelse(TAVG > H99, 1, 0),
         RH = round(RH(TAVG, Tdmean, isK = FALSE), 2),
         Heatwave = if_else(low_intensity > 0 | moderate_intensity > 0 | high_intensity > 0, 1, 0)) %>%
  select(Date, Zip, TAVG, TMIN, TMAX, RH, EHF, EHI_sig, EHI_accl, 
         Severe_Heatwaves, Extreme_Heatwaves, Above_95th, Above_97th, Above_99th, Heatwave,
         low_intensity, moderate_intensity, high_intensity)

#==============================================================================#
#write to .parquet
#==============================================================================#

write_parquet(Heat_Calc, "Heatwave_Metrics.parquet")

#==============================================================================#
#Coldwaves
#==============================================================================#

Coldwave_NC <- fread("C:\\Users\\owner\\Documents\\GRAM\\HeatWave\\Data\\NC_Cold_Index.csv")
Coldwave <- fread("C:\\Users\\owner\\Documents\\GRAM\\HeatWave\\Data\\SC_Cold_Index.csv")

#==============================================================================#
#Combine .csv and clean the data
#==============================================================================#

#Coldwave <- rbind(Coldwave_NC, Coldwave_SC)

Coldwave$region_code <- as.character(Coldwave$region_code)
Coldwave$year <- as.character(Coldwave$year)
Cold <-Coldwave
Cold$ECF[Cold$ECF <= 0] <- NA

#==============================================================================#
#Calculate the 85th percentile EHF
#==============================================================================#

ECF85 <- Cold %>% 
  group_by(region_code) %>% 
  do(data.frame(t(quantile(.$ECF, probs = c(0.85), na.rm=T))))
colnames(ECF85) <- c('region_code','ECF85')
Coldwave <- merge(Coldwave, ECF85)
Coldwave$daynum <-1

#==============================================================================#
#Calculate the yearly data
#==============================================================================#

Cold_Yearly_Data <- Coldwave %>%
  group_by(year)%>%
  summarize(No_Coldwave = sum(daynum - Coldwave_condition),
            Coldwave_days = sum(Coldwave_condition),
            Severe_Coldwaves = sum(ifelse(ECF >= ECF85, 1, 0)),
            Extreme_Coldwaves = sum(ifelse(ECF >= (3 * ECF85), 1, 0)),
            mean_daily_Coldwaves = mean(Coldwave_condition)*365, #Average number of heatwaves per county per year
            sd_daily_Coldwaves = sd(Coldwave_condition),
            mean_ECF = mean(ECF), #Per county per year
            sd_ECF = sd(ECF),
            Positive_ECF_occurance = sum(ECF > 0)) 

#==============================================================================#
#Calculate the county level data
#==============================================================================#

Cold_County_Data <- Coldwave %>%
  group_by(region_code)%>%
  summarize(No_Coldwave = sum(daynum - Coldwave_condition),
            mean_No_Coldwave = sum(daynum - Coldwave_condition)/31,
            Total_Coldwaves = sum(Coldwave_condition),
            Severe_Coldwaves = sum(ifelse(ECF >= ECF85, 1, 0)),
            Extreme_Coldwaves = sum(ifelse(ECF >= (3 * ECF85), 1, 0)),
            mean_Coldwaves_yearly = mean(Coldwave_condition)*365, 
            sd_Coldwaves_yearly = sd(Coldwave_condition),
            mean_ECF = mean(ECF),
            sd_ECF = sd(ECF),
            ECF_yearly_occurance = sum(ECF > 0)/31)

#==============================================================================#
#Calculate the daily data
#==============================================================================#

Cold_Year <- Coldwave %>%
  tibble() %>%
  group_by(region_code)%>%
  mutate(No_Coldwave = (daynum - Coldwave_condition),
            Coldwave_days = (Coldwave_condition),
            Severe_Coldwaves = (ifelse(ECF >= ECF85, 1, 0)),
            Extreme_Coldwaves = (ifelse(ECF >= (3 * ECF85), 1, 0)),
            Below_5th = B5,
            Below_3rd = B3,
            Below_1st = B1,
            low_intensity = (ifelse(ECF > 0 & ECF < 1, 1, 0)),
            moderate_intensity = (ifelse(ECF >= 1 & ECF < 2, 1, 0)),  
            high_intensity = (ifelse(ECF >= 2, 1, 0)),)%>%
  dplyr::select(Date, region_code, 
         No_Coldwave, Coldwave_days, Severe_Coldwaves, Extreme_Coldwaves, 
         Below_5th, Below_3rd, Below_1st, TAVG, TMIN, TMAX,
         low_intensity, moderate_intensity, high_intensity)

#==============================================================================#
#write to .csv
#==============================================================================#

fwrite(Cold_Yearly_Data, "Cold_Yearly_Data.csv", verbose = TRUE)    
fwrite(Cold_County_Data, "Cold_County_Data.csv", verbose = TRUE) 
fwrite(Cold_Year, "Coldwave.csv")















