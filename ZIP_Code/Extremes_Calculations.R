library(data.table)
library(dplyr)
setwd("~/GRAM/HeatWave/Data")

Heatwave_NC <- fread("C:\\Users\\owner\\Documents\\GRAM\\HeatWave\\Data\\NC_Heat_Index.csv")
Heatwave <- fread("C:\\Users\\owner\\Documents\\GRAM\\HeatWave\\Data\\SC_Heat_Index.csv")
#==============================================================================#
#Combine .csv and clean the data
#==============================================================================#

#Heatwave <- rbind(Heatwave_NC, Heatwave_SC)

Heatwave$region_code <- as.character(Heatwave$region_code)
Heatwave$year <- as.character(Heatwave$year)
Heatwave$EHF[Heatwave$EHF <= 0] <- NA

#==============================================================================#
#Calculate the 85th percentile EHF
#==============================================================================#

EHF85 <- Heatwave %>% 
  group_by(region_code) %>% 
  do(data.frame(t(quantile(.$EHF, probs = c(0.85), na.rm=T))))
colnames(EHF85) <- c('region_code','EHF85')

Heatwave <- merge(Heatwave, EHF85)
Heatwave$Severe_occurance <- ifelse(Heatwave$EHF > 0, 1, 0)
Heatwave$daynum <- 1
Heatwave[is.na(Heatwave)] <- 0

#==============================================================================#
#Calculate the yearly data
#==============================================================================#

Hot_Yearly_Data <- Heatwave %>%
  group_by(year)%>%
  summarize(No_Heatwave = sum(daynum - Heatwave_condition),
            Heatwave_days = sum(Heatwave_condition),
            Severe_Heatwaves = sum(ifelse(EHF >= EHF85, 1, 0)),
            Extreme_Heatwaves = sum(ifelse(EHF >= (3 * EHF85), 1, 0)),
            mean_daily_Heatwaves = mean(Heatwave_condition)* 365, #Average number of heatwaves per county per year
            sd_daily_Heatwaves = sd(Heatwave_condition),
            mean_EHF = mean(EHF), #Per county per year
            sd_EHF = sd(EHF),
            Positive_EHF_occurance = sum(EHF > 0))  
  
#==============================================================================#
#Calculate the county data
#==============================================================================#

Hot_County_Data <- Heatwave %>%
            group_by(region_code)%>%
            summarize(No_Heatwave = sum(daynum - Heatwave_condition),
                      mean_No_Heatwave = sum(daynum - Heatwave_condition)/31,
                      Total_Heatwaves = sum(Heatwave_condition),
                      Severe_Heatwaves = sum(ifelse(EHF >= EHF85, 1, 0)),
                      Extreme_Heatwaves = sum(ifelse(EHF >= (3 * EHF85), 1, 0)),
                      mean_Heatwaves_yearly = mean(Heatwave_condition)*365, 
                      sd_Heatwaves_yearly = sd(Heatwave_condition),
                      mean_EHF = mean(EHF),
                      sd_EHF = sd(EHF),
                      EHF_yearly_occurance = sum(EHF > 0)/31)

#==============================================================================#
#Calculate the daily information 
#==============================================================================#

Heat_Year <- Heatwave %>%
  tibble() %>%
  group_by(region_code) %>%
  mutate(No_Heatwave = (daynum - Heatwave_condition),
         Heatwave_days = (Heatwave_condition),
         Severe_Heatwaves = (ifelse(EHF >= EHF85, 1, 0)),
         Extreme_Heatwaves = (ifelse(EHF >= (3 * EHF85), 1, 0)),
         low_intensity = (ifelse(EHF > 0 & EHF < 1, 1, 0)),
         moderate_intensity = (ifelse(EHF >= 1 & EHF < 2, 1, 0)),  
         high_intensity = (ifelse(EHF >= 2, 1, 0)),
         Above_95th = A95,
         Above_97th = A97,
         Above_99th = A99)
         #TAGVLag1 = lag(TAVG, order_by = date),
         #TAGVLag2 = lag(TAVG, n = 2, order_by = date),
         #TAGVLag3 = lag(TAVG, n = 3, order_by = date),
         #TMINLag1 = lag(TMIN, order_by = date),
         #TMINLag2 = lag(TMIN, n = 2, order_by = date),
         #TMINLag3 = lag(TMIN, n = 3, order_by = date),
         #TMAXLag1 = lag(TMAX, order_by = date),
         #TMAXLag2 = lag(TMAX, n = 2, order_by = date),
         #TMAXLag3 = lag(TMAX, n = 3, order_by = date),) %>%
  select(date, region_code, No_Heatwave, Heatwave_days, Severe_Heatwaves, Extreme_Heatwaves, Above_95th, Above_97th, Above_99th,
         low_intensity, moderate_intensity, high_intensity,TAVG, TMIN, TMAX) 
         #TAGVLag1, TAGVLag2,TAGVLag3, TMINLag1, TMINLag2, TMINLag3, TMAXLag1, TMAXLag2, TMAXLag3)

#==============================================================================#
#write to .csv
#==============================================================================#

fwrite(Hot_Yearly_Data, "Hot_Yearly_Data.csv", verbose = TRUE)    
fwrite(Hot_County_Data, "Hot_County_Data.csv", verbose = TRUE) 
fwrite(Heat_Year, "Heatwave.csv")

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
  dplyr::select(date, region_code, 
         No_Coldwave, Coldwave_days, Severe_Coldwaves, Extreme_Coldwaves, 
         Below_5th, Below_3rd, Below_1st, TAVG, TMIN, TMAX,
         low_intensity, moderate_intensity, high_intensity)

#==============================================================================#
#write to .csv
#==============================================================================#

fwrite(Cold_Yearly_Data, "Cold_Yearly_Data.csv", verbose = TRUE)    
fwrite(Cold_County_Data, "Cold_County_Data.csv", verbose = TRUE) 
fwrite(Cold_Year, "Coldwave.csv")















