library(data.table)
library(dplyr)

dat.files  <- list.files(path="~/Heatwave/Data/Practice/Test",
                         recursive=T,
                         pattern="\\.csv$",
                         full.names=T,
                         ignore.case=F,
                         no.. = TRUE)

data <- NULL
data <- data.table(data)

for(i in dat.files){
  temps <- fread(i)

  comb <- temps %>%
    mutate(Date = substring(.$Date, 1),
           Date = as.Date(gsub("\\.", "-", Date)))
           
  
  year <- data.frame(comb)
  data <- rbind(data, year)
  
}
 
setwd("E:/PRISM_Data/Final_Products")
fwrite(data, "tmean_1998_2020.csv")

dat.files  <- list.files(path="E:/PRISM_Data/Final_Products",
                         recursive=T,
                         pattern="\\.csv$",
                         full.names=T,
                         ignore.case=F,
                         no.. = TRUE)


tmin <- fread("E:/PRISM_Data/Final_Products/tmin_1998_2020.csv")
tmax <- fread("E:/PRISM_Data/Final_Products/tmax_1998_2020.csv")
tmean <- fread("E:/PRISM_Data/Final_Products/tmean_1998_2020.csv")
tdmean <- fread("E:/PRISM_Data/Final_Products/tdmean_1998_2020.csv")

tmin_max <- left_join(tmin, tmax, by = c("Date" = "Date", "Zip" = "Zip"))  
tmin_max <- left_join(tmin_max, tmean, by = c("Date" = "Date", "Zip" = "Zip"))  
tmin_max <- left_join(tmin_max, tdmean, by = c("Date" = "Date", "Zip" = "Zip"))  

temp <- na.omit(tmin_max)

temperature <- temp[, c(3,2,1,4,5,6)]

fwrite(temperature, "SC_PRISM.csv")
 













