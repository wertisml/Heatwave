library(data.table)
library(dplyr)
library(rgdal)
library(sf)
library(raster)
library(purrr)
library(tidyverse)
library(prism)

#==============================================================================#
#Pre-process
#==============================================================================#

ZipShape <- st_read("~/Heatwave/ZIP_Code/NC_Shapefile/ZIP_Code_Tabulation_Areas.shp")

dat.files  <- list.files(path="~/Heatwave/Data/Practice_Rasters/Tmean",
                         recursive=T,
                         pattern="\\_bil.bil$",
                         full.names=T,
                         ignore.case=F,
                         no.. = TRUE)

#Change the date to be the start of the raster, length.out is the number of rasters in the list
tempo <- seq(as.Date("2018/01/01"), by = "day", length.out = length(dat.files)) 

#==============================================================================#
#Brick the rasters and cut up the rasters
#==============================================================================#

#combine all rasters into one
mystack <- stack(dat.files)

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

data = NULL
data <- data.table(data)
n = 1
for(i in temps$OBJECTID){
  if(i == n){
    
    first <- temps[i,]
    
    tran <- cbind(t(first[,11:ncol(temps)]), colnames(first[,11:ncol(temps)]))
    
    combined <- data.table(cbind(tran, first[,2]))
    
    colnames(combined) <- c("tmean", "Date", "Zip")
    
    comb <- combined %>%
      mutate(Date = str_sub(Date, 2, -1),
             Date = as.Date(gsub("\\.", "-", Date)),
             tmean = round(as.numeric(tmean), digits = 2))
             
    n = n+1
    print(n)
    data <- rbind(data, comb)
  }
}

write_parquet(data, "tmean_2020.parquet")

