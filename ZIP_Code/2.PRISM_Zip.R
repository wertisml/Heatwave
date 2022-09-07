library(data.table)
library(dplyr)
library(rgdal)
library(sf)
library(raster)
library(tictoc)
library(purrr)
library(tidyverse)
library(prism)

#==============================================================================#
#Pre-process
#==============================================================================#

ZipShape <- st_read( "E:\\PRISM_Data\\SC_Shapefile\\SC_Shapefile\\South_Carolina.shp")

dat.files  <- list.files(path="E:/PRISM_Data/DewPoint",
                         recursive=T,
                         pattern="\\_bil.bil$",
                         full.names=T,
                         ignore.case=F,
                         no.. = TRUE)

#Change the date to be the start of the raster, length.out is the number of rasters in the list
tempo <- seq(as.Date("2018/01/01"), by = "day", length.out = 1096) 

#==============================================================================#
#Brick the rasters and cut up the rasters
#==============================================================================#

prism_set_dl_dir("E:/PRISM_Data/Tmax")

#combine all rasters into one
tic()
mystack <- pd_stack(prism_archive_subset(
  "tmax",
  temp_period = "daily",
  minDate = "2018-01-01", 
  maxDate = "2020-12-31"))
toc()
#Change the file names
r <- setNames(mystack, tempo)
#make the shapefile have the same spatial extent as the raster
ZipShape <- st_transform(ZipShape, st_crs(r))
#alter raster to shapefile shape
r.crop <- crop(r, extent(ZipShape))
#get the values for everyhting in the brick
r.mean = raster::extract(r.crop, ZipShape, method = "simple", fun = mean, sp = T)
# Write results
temps <- as.data.frame(r.mean)

temps$OBJECTID <- seq(1, nrow(temps))
temps <- temps %>%
  dplyr::select(OBJECTID, everything())

#==============================================================================#
#For loop to transpose the data and create a new file
#==============================================================================#
#temps <- fread("C:\\Users\\owner\\Documents\\GRAM\\HeatWave\\Data\\PRISM_DewPoint\\A_Final_Product\\tdmean_2017.csv")

data = NULL
data <- data.table(data)
n = 1
for(i in temps$OBJECTID){
  if(i == n){
    
    first <- temps[i,]
    
    tran <- cbind(t(first[,11:ncol(temps)]), colnames(first[,11:ncol(temps)]))
    
    combined <- data.table(cbind(tran, first[,2]))
    
    colnames(combined) <- c("tmax", "Date", "Zip")
    
    comb <- combined %>%
      mutate(Date = str_sub(Date, 2, -1),
             Date = as.Date(gsub("\\.", "-", Date)),
             tmax = round(as.numeric(tmax), digits = 2))
    n = n+1
    print(n)
    data <- rbind(data, comb)
  }
}

setwd("E:/PRISM_Data/Tmax/A_Final_Product")
fwrite(data, "tdmean_2018_2020.csv")

