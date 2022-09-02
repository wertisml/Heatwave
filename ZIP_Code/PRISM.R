library(prism)
library(raster)

#==============================================================================#
# PRISM TAVG
#==============================================================================#

prism_set_dl_dir("E:/PRISM_Data/Tmean")

get_prism_dailys(
  type = "tmean", 
  minDate = "2008-01-01", 
  maxDate = "2020-12-31", 
  keepZip = FALSE)

pd_to_file(prism_archive_ls())

#==============================================================================#
# PRISM TMAX
#==============================================================================#

prism_set_dl_dir("E:/PRISM_Data/Tmax")

get_prism_dailys(
  type = "tmax", 
  minDate = "1999-08-27", 
  maxDate = "1999-12-31", 
  keepZip = FALSE)

pd_to_file(prism_archive_ls())

#==============================================================================#
# PRISM TMIN
#==============================================================================#

prism_set_dl_dir("E:/PRISM_Data/Tmin")

get_prism_dailys(
  type = "tmin", 
  minDate = "2016-01-01", 
  maxDate = "2020-12-31", 
  keepZip = FALSE)

pd_to_file(prism_archive_ls())

#==============================================================================#
# PRISM DewPoint
#==============================================================================#

prism_set_dl_dir("E:/PRISM_Data/DewPoint")

get_prism_dailys(
  type = "tdmean", 
  minDate = "2017-01-01", 
  maxDate = "2020-12-31", 
  keepZip = FALSE)

pd_to_file(prism_archive_ls())

#==============================================================================#
# 30-year normals
#==============================================================================#

prism_set_dl_dir("~/Heat_Waves/South_Carolina/Normals")

get_prism_dailys(type = "tmean", 
                 resolution = "800m",
                 mon = NULL,
                 keepZip = FALSE)

pd_to_file(prism_archive_ls())




