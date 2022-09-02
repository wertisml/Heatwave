library(prism)
library(raster)
#Can only download a temperature type twice a day from an ip

#==============================================================================#
#PRISM TAVG
#==============================================================================#

prism_set_dl_dir("~/GRAM/HeatWave/Data/PRISM_TAVG")

get_prism_dailys(
  type = "tmean", 
  minDate = "2020-01-01", 
  maxDate = "2020-08-20", 
  keepZip = FALSE)

#pd_to_file(prism_archive_ls())

#==============================================================================#
#PRISM TMAX
#==============================================================================#

prism_set_dl_dir("~/GRAM/HeatWave/Data/PRISM_TMAX")

get_prism_dailys(
  type = "tmax", 
  minDate = "2020-01-01", 
  maxDate = "2020-12-31", 
  keepZip = FALSE)

#pd_to_file(prism_archive_ls())

#==============================================================================#
#PRISM TMIN
#==============================================================================#

prism_set_dl_dir("~/GRAM/HeatWave/Data/PRISM_TMIN")

get_prism_dailys(
  type = "tmin", 
  minDate = "2017-01-01", 
  maxDate = "2017-12-31", 
  keepZip = FALSE)

#pd_to_file(prism_archive_ls())

#==============================================================================#
#PRISM DewPoint
#==============================================================================#

prism_set_dl_dir("~/GRAM/HeatWave/Data/PRISM_DewPoint")

get_prism_dailys(
  type = "tdmean", 
  minDate = "2020-03-09", 
  maxDate = "2020-12-31", 
  keepZip = FALSE)

#pd_to_file(prism_archive_ls())

#==============================================================================#
#30-year normals
#==============================================================================#

prism_set_dl_dir("~/GRAM/HeatWave/Data/Normals")

get_prism_normals(type = "tdmean",
                  resolution = "800m",
                  mon = NULL, 
                  annual = TRUE,
                  keepZip = FALSE)

#pd_to_file(prism_archive_ls())

#==============================================================================#
#When select few files load empty
#==============================================================================#

prism_set_dl_dir("~/GRAM/HeatWave/Data/PRISM_TMIN")

get_prism_dailys(type="tmin", 
                 dates = "2020-09-23", 
                 keepZip=FALSE)

#pd_to_file(prism_archive_ls())














