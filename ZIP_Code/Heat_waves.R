library(EpiNOAA)
library(dplyr)
library(data.table)
library(tictoc)
library(dplyr)
library(weathermetrics)
library(humidity)
setwd("~/GRAM/HeatWave/Data")
#Make sure its the correct one

#data_2020 <- read_nclimgrid_epinoaa(beginning_date = '2020-03-21', end_date = '2020-12-21', workers = 10)
#data_2021 <- read_nclimgrid_epinoaa(beginning_date = '2021-01-01', end_date = '2021-12-31', workers = 5)
data_2021_NC <- read_nclimgrid_epinoaa(beginning_date="2021-01-01",end_date="2021-12-31",
                                       spatial_res="cty",states="NC", workers = 5)
data_2021_NC <- fread(file.choose())
#data_2021_NC$PRCP <- NULL
data_2021_NC$TMIN <- as.numeric(data_2021_NC$TMIN)
data_2021_NC$TMAX <- as.numeric(data_2021_NC$TMAX)
data_2021_NC$TAVG <- as.numeric(data_2021_NC$TAVG)
data_2021_NC$region_code <- as.numeric(data_2021_NC$region_code)

HeatWave <- data_2021_NC %>%
  group_by(date)%>%
  arrange(region_code)
#Sum of the total days in a year
number_of_days <- aggregate(TAVG~region_code,HeatWave,length)
names(number_of_days)[2] <- 'number_of_days'
#Total TAVG value for a year
total_temp <- aggregate(TAVG~region_code,HeatWave,sum)
names(total_temp)[2] <- 'total_temp'
#calculating extremes
Extremes <- HeatWave %>% group_by(region_code) %>% 
  do(data.frame(t(quantile(.$TAVG, probs = c(0.95, 0.97, 0.99, 0.01, 0.03, 0.05)))))
#Rename columns
colnames(Extremes) <- c('region_code','H95', 'H97', 'H99','C1', 'C3', 'C5')
#merge in columns
temps <- merge(number_of_days,total_temp)
#Calculate the average temp for the year by FIPS/County
temps$yearly_average <- temps$total_temp / temps$number_of_days
#Merge in the average yearly temp with the original data
trying <- merge(HeatWave, temps, by = "region_code", all.x = TRUE)
#Merge in the extremes with the other temperature data
temperature <- merge(trying, Extremes, by = "region_code", all.x = TRUE)
#Turn region_code and day from a character to numeric
temperature$region_code <- as.numeric(temperature$region_code)
temperature$day <- as.numeric(temperature$day)
#Compare any day to the 95th percentile
temperature$daily_diff <- (temperature$TMAX - temperature$TMIN)
#number the region_codes from 1 to end
temperature$number <- cumsum(c(1,as.numeric(diff(temperature$region_code))!=0))
#
Weather <- temperature %>%
  mutate(A95 = fifelse(TAVG > H95, 1, 0),
         A97 = fifelse(TAVG > H97, 1, 0),
         A99 = fifelse(TAVG > H99, 1, 0),
         B5 = fifelse(TAVG < C5, 1, 0),
         B3 = fifelse(TAVG < C3, 1, 0),
         B1 = fifelse(TAVG < C1, 1, 0))
###################################################################################################################
###################################################################################################################
###################################################################################################################
#Calculate Heat Wave

Above_95th <- NULL
Above_95th <- data.frame(Above_95th)
Above_Surrounding <- NULL
Above_Surrounding <- data.frame(Above_Surrounding)
Data <- NULL
Data <- data.frame(Data)
data <- NULL
data <- data.frame(data)
Heatwave_condition <- 0
Heatwave_condition <- data.frame(Heatwave_condition)

tic()
n = 1
for (i in Weather$number) {
  if (n == i){
    mydata2 <- Weather%>%
      filter(number == i)
    x = 1
    k = 31
    Above_Surrounding <- NULL
    Above_95th <- NULL
    Heatwave_condition <- NULL
    mydata2$Heatwave_condition <- 0
    print(n)
    for (j in mydata2$TAVG){
      #mydata2$Heatwave_condition <- 0
      region_code <- mydata2$region_code
      year <- mydata2$year
      month <- mydata2$month
      date <- mydata2$date
      A95 <- mydata2$A95
      A97 <- mydata2$A97
      A99 <- mydata2$A99
      TAVG <- mydata2$TAVG
      TMIN <- mydata2$TMIN
      TMAX <- mydata2$TMAX
      daily_diff <- mydata2$daily_diff
      b <- mydata2$number
      y <- fifelse(sum(mydata2$TAVG[x:(x+2)])/3 > sum(mydata2$H95[x:(x+2)])/3, 1, 0)
      z <- fifelse(sum(mydata2$TAVG[k:(k+2)])/3 > sum(mydata2$TAVG[(k-1):(k-30)])/30, 1, 0)
      if(x <= 11321){
        if(y > 0){
          mydata2$Heatwave_condition[x:(x+2)] <- 1
        }else{
          mydata2$Heatwave_condition[x:(x+2)] <- mydata2$Heatwave_condition[x:(x+2)]
        }
      }
      Heatwave_condition <- mydata2[, c("Heatwave_condition")]
      Above_Surrounding <- rbind(Above_Surrounding, z)
      Above_95th <- rbind(Above_95th, y)
      x = x + 1
      k = k + 1
    }
    #Heatwave_condition <- head(Heatwave_condition, -2)
    data <- data.frame(n, Above_95th, Above_Surrounding, region_code, year, month, date, daily_diff,
                       A95, A97, A99, Heatwave_condition, TAVG, TMAX, TMIN)
    Data <- rbind(Data, data)
    n = n + 1
  }
}
toc()
Data3 <- na.omit(Data)

#calculate EHI_accl, EHI_sig, and EHF

Data1 <- NULL
data1 <- NULL
Data1 <- data.frame(Data1)
data1 <- data.frame(data1)
Heatwave_condition <- 0
Heatwave_condition <- data.frame(Heatwave_condition)
EHI_accl  <- vector(mode="numeric")
EHI_sig  <- vector(mode="numeric")

tic()
n = 1
for (i in Weather$number) {
  if (n == i){
    mydata2 <- Weather%>%
      filter(number == i)
    x = 1
    k = 31
    Heatwave_condition <- NULL
    EHI_accl <-NULL
    EHI_sig <- NULL
    mydata2$Heatwave_condition <- 0
    print(n)
    for (j in mydata2$TAVG){
      if(x <= 11321){
        year <- mydata2$year
        b <- mydata2$number
        y <- (sum(mydata2$TAVG[x:(x+2)])/3 - sum(mydata2$H95[x:(x+2)])/3)
        z <- (sum(mydata2$TAVG[k:(k+2)])/3 - sum(mydata2$TAVG[(k-1):(k-30)])/30)
        if(y > 0){
          mydata2$Heatwave_condition[x:(x+2)] <- 1
        }else{
          mydata2$Heatwave_condition[x:(x+2)] <- mydata2$Heatwave_condition[x:(x+2)]
        }
      }
      Heatwave_condition <- mydata2[, c("Heatwave_condition")]
      EHI_accl <- append(EHI_accl, z)
      EHI_sig <- append(EHI_sig, y)
      x = x + 1
      k = k + 1
    }
    data1 <- data.frame(n, EHI_sig, EHI_accl, year, Heatwave_condition)
    Data1 <- rbind(Data1, data1)
    n = n + 1
  }
}
toc()

Data2 <- na.omit(Data1)

Data2$EHI_sig <- fifelse(Data2$EHI_sig < 0, 0, Data2$EHI_sig)
Data2$EHI_accl <- fifelse(Data2$EHI_accl < 1, 1, Data2$EHI_accl)

#Data2$EHF <- with(Data2, EHI_sig * max(1, EHI_accl))
Data2$EHF <- with(Data2, EHI_sig * EHI_accl)

Data2$Heatwave_condition <- NULL
Data2$n <- NULL
Data2$year <- NULL

Heat_Index <- cbind(Data3, Data2)
fwrite(Heat_Index, "SC_Heat_Index.csv")
###################################################################################################################
###################################################################################################################
###################################################################################################################
#Calculate Cold Wave
#NEED TO DOWNLOAD 2 EXTRA DAYS OF DATA OR ELSE THE x<=363 wont get the last few days of the year
Below_5th <- NULL
Below_5th <- data.frame(Below_5th)
Below_Surrounding <- NULL
Below_Surrounding <- data.frame(Below_Surrounding)
Datar <- NULL
datar <- NULL
Datar <- data.frame(Datar)
datar <- data.frame(datar)
Coldwave_condition <- 0
Coldwave_condition <- data.frame(Coldwave_condition)


tic()
n = 1
for (i in Weather$number) {
  if (n == i){
    mydata <- Weather%>%
      filter(number == i)
    x = 1
    k = 31
    Below_Surrounding <- NULL
    Below_5th <- NULL
    Coldwave_condition <- NULL
    mydata$Coldwave_condition <- 0
    print(n)
    for (j in mydata$TAVG){
      region_code <- mydata$region_code
      year <- mydata$year
      month <- mydata$month
      date <- mydata$date
      b <- mydata$number
      daily_diff <- mydata$daily_diff
      Relative_humidity <- mydata$Relative_humidity
      B1 <- mydata$B1
      B3 <- mydata$B3
      B5 <- mydata$B5
      TAVG <- mydata$TAVG
      TMAX <- mydata$TMAX
      TMIN <- mydata$TMIN
      y <- fifelse(sum(mydata$TAVG[x:(x+2)])/3 < sum(mydata$C5[x:(x+2)])/3, 1, 0)
      z <- fifelse(sum(mydata$TAVG[k:(k+2)])/3 < sum(mydata$TAVG[(k-1):(k-30)])/30, 1, 0)
      if(x <= 11321){
        if(y > 0){
          mydata$Coldwave_condition[x:(x+2)] <- 1
        }else{
          mydata$Coldwave_condition[x:(x+2)] <- mydata$Coldwave_condition[x:(x+2)]
        }
      }
      Coldwave_condition <- mydata[, c("Coldwave_condition")]
      Below_Surrounding <- rbind(Below_Surrounding, z)
      Below_5th <- rbind(Below_5th, y)
      x = x + 1
      k = k + 1
    }
    datar <- data.frame(n, Below_5th, Below_Surrounding, region_code, year, month, date, daily_diff,
                        B1, B3, B5, Coldwave_condition, TAVG, TMAX, TMIN)
    Datar <- rbind(Datar, datar)
    n = n + 1
  }
}
toc()

Datar3 <- na.omit(Datar)

#calculate EHI_accl, EHI_sig, and EHF, but COLD

Datar1 <- NULL
datar1 <- NULL
Datar1 <- data.frame(Datar1)
datar1 <- data.frame(datar1)
Coldwave_condition <- 0
Coldwave_condition <- data.frame(Coldwave_condition)
EHI_accl  <- vector(mode="numeric")
EHI_sig  <- vector(mode="numeric")

tic()
n = 1
for (i in Weather$number) {
  if (n == i){
    mydata <- Weather%>%
      filter(number == i)
    x = 1
    k = 31
    Coldwave_condition <- NULL
    EHI_accl <-NULL
    EHI_sig <- NULL
    mydata$Coldwave_condition <- 0
    print(n)
    for (j in mydata$TAVG){
      if(x <= 11321){
        region_code <- mydata$region_code
        year <- mydata$year
        b <- mydata$number
        y <- (sum(mydata$TAVG[x:(x+2)])/3 - sum(mydata$C5[x:(x+2)])/3) #we want our DMT3 to be smaller then Cold
        z <- (sum(mydata$TAVG[k:(k+2)])/3 - sum(mydata$TAVG[(k-1):(k-30)])/30) #we want our DMT3 to be smaller then DMT30
        if(y < 0){
          mydata$Coldwave_condition[x:(x+2)] <- 1
        }else{
          mydata$Coldwave_condition[x:(x+2)] <- mydata$Coldwave_condition[x:(x+2)]
        }
      }
      Coldwave_condition <- mydata[, c("Coldwave_condition")]
      EHI_accl <- append(EHI_accl, z)
      EHI_sig <- append(EHI_sig, y)
      x = x + 1
      k = k + 1
    }
    datar1 <- data.frame(n, EHI_sig, EHI_accl, Coldwave_condition)
    Datar1 <- rbind(Datar1, datar1)
    n = n + 1
  }
}
toc()

Datar2 <- na.omit(Datar1)

Datar2$EHI_sig <- fifelse(Datar2$EHI_sig > 0, 0, Datar2$EHI_sig)
Datar2$EHI_accl <- fifelse(Datar2$EHI_accl < -1, Datar2$EHI_accl, -1)

Datar2$ECF <- with(Datar2, EHI_sig * EHI_accl)

Datar2$Coldwave_condition <- NULL
Datar2$n <- NULL

Cold_Index <- cbind(Datar3, Datar2)
fwrite(Cold_Index, "SC_Cold_Index.csv")

