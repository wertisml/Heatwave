# Heatwave


How variables are calculated in scripts 4 and 5

EHF or Exessive Heat Factor is calculated using; 
  
  First, the Exessive Heat Index (EHI) is determined, which is the value of the 3 day temperature average subracted from the 95th percentile of heat.
       EHI <- (3 Day temeprature average) - 95th temperature percentile
       
  EHI_accl is the value of the 3 day average subrtacted from the previous 30 day average temperature     
       EHI_accl <- (3 Day temeprature average) - (previous 30 day temperatre average)
       
  EHI_sig is when the EHI is positive     
       EHI_sig <- fifelse(EHI < 0, 0, EHI)

  EHF is the product of a positive EHI_sig multiplied by which ever is greater, 1, or the EHI_accl 
       EHF <- EHI_sig * max(1, EHI_accl)

No_Heatwave = (daynum - Heatwave_condition)
- Daynum is a value of 1 and is subtracted from the a column indicating if there was a heatwave or not, meaning a value of 1 is a day of no heatwave 

Heatwave_days = (Heatwave_condition)
- This is a renaming of the column Heatwave_condition, which receives a value of 1 for that day when the 3 day average that that day is within is above the 95th percentile for temperature, as well as the previous 30 day average being less then the 3 day average.

Severe_Heatwaves = (ifelse(EHF >= EHF85, 1, 0))
- This receives a value of 1 if the EHF for that day is equal to or greater than the 85th percentile of all EHF for that area.
       
Extreme_Heatwaves = (ifelse(EHF >= (3 * EHF85), 1, 0))
- This receives a value of 1 if the EHF for that day is equal to or greater than 3 times the 85th percentile of all EHF for that area.

low_intensity = (ifelse(EHF > 0 & EHF < 1, 1, 0))
- This receives a value of 1 if the EHF for that day is greater than 0 and less than 1.

moderate_intensity = (ifelse(EHF >= 1 & EHF < 2, 1, 0)) 
- This receives a value of 1 if the EHF for that day is greater than or equal to 1 and less than 2.

high_intensity = (ifelse(EHF >= 2, 1, 0)),
- This receives a value of 2 if the EHF is greater than or equal to 2.

 



