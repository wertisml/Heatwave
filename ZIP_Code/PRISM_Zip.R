library(data.table)
library(dplyr)
library(rgdal)
library(sf)
library(raster)
library(tictoc)
library(purrr)
library(tidyverse)
library(prism)
library(exactextractr)

#==============================================================================#
#Pre-process
#==============================================================================#

ZipShape <- st_read("C:\\Users\\owner\\Documents\\GRAM\\HeatWave\\Data\\US_Shapefile_Zip\\North_Carolina\\ZIP_Code_Tabulation_Areas.shp")

dat.files  <- list.files(path="~/GRAM/HeatWave/Data/PRISM_TAVG",
                         recursive=T,
                         pattern="\\_bil.bil$",
                         full.names=T,
                         ignore.case=F,
                         no.. = TRUE)

#Change the date to be the start of the raster, length.out is the number of rasters in the list
tempo <- seq(as.Date("2020/01/01"), by = "day", length.out = 366) 
#Change the file names

#==============================================================================#
#Brick the rasters and cut up the rasters
#==============================================================================#

#set path for PRISM files
prism_set_dl_dir("~/GRAM/HeatWave/Data/PRISM_TAVG")

#combine all rasters into one
tic()
mystack <- pd_stack(prism_archive_subset(
  "tmean",
  temp_period = "daily",
  minDate = "2020-01-01", 
  maxDate = "2020-12-31"))
toc()
  
#Change the file names
r <- setNames(mystack, tempo)
#make the shapefile have the same spatial extent as the raster
ZipShape <- st_transform(ZipShape, st_crs(r))

#alter raster to shapefile shape
tic()
r.crop <- raster::crop(r, extent(ZipShape))
toc()

#get the values for everything in the brick
r.mean = raster::extract(r.crop, ZipShape, method = "simple", fun = mean, sp = T)

# Write results
temps <- as.data.frame(r.mean)

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
    
    tran <- cbind(t(first[,9:ncol(temps)]), colnames(first[,9:ncol(temps)]))
    
    combined <- data.table(cbind(tran, first[,2]))
    
    colnames(combined) <- c("tmin", "Date", "Zip")
    
    comb <- combined %>%
      mutate(Date = str_sub(Date, 2, -1),
             Date = as.Date(gsub("\\.", "-", Date)),
             tmin = round(as.numeric(tmin), digits = 2))
    n = n+1
    print(n)
    data <- rbind(data, comb)
  }
}

setwd("~/GRAM/HeatWave/Data/PRISM_TMIN/A_Final_Product")
fwrite(data, "tmin_2017.csv")

