library(future)
library(furrr)
library(tidyverse)
library(arrow)

setwd("~/Heatwave/Data/Zip")

#==============================================================================#
# Read in and set up data
#==============================================================================#

Temperature <- left_join(open_dataset("NC_PRISM_ZIP_2008_2021.parquet") %>% # Create a df of the temeprature data and rename the columns
                           mutate(month = month(Date)) %>%
                           collect(),
                         open_dataset("NC_PRISM_ZIP_2008_2021.parquet") %>% # Create a df for extreme percentile of temperatures
                           mutate(month = month(Date)) %>%
                           collect() %>%
                           group_by(Zip, month) %>%
                           do(data.frame(t(quantile(.$tmean, probs = c(0.95, 0.97, 0.99))))) %>%
                           rename(H95 = X95.,
                                  H97 = X97.,
                                  H99 = X99.),
                         by = c("Zip", "month")) %>%
  arrange(Date) %>%
  rename(Tdmean = tdmean,
         TMIN = tmin,
         TMAX = tmax,
         TAVG = tmean) %>%
  na.omit(.) # Remove any rows that contain missing values

#==============================================================================#
# Create Heatwave Functions
#==============================================================================#

# Calculate the EHI significance
EHI_sig <- function(Data){
  
  # Set the window size
  window_size <- 3
  
  # Initialize the start and end indices of the window
  start_index <- 1
  end_index <- window_size
  
  # Initialize a new column called is_larger
  Data[, "EHI_sig"] <- 0
  
  # Loop through the dataset row by row with a 3 row window
  while (end_index <= nrow(Data)) {
    
    # Get the current window of rows
    current_window <- Data[start_index:end_index, ]
    
    # Check if the current window is larger than the mean value of the whole column
    Data$EHI_sig[end_index] <- mean(current_window$TAVG) - mean(current_window$H95) 
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    end_index <- end_index + 1
  }
  return(Data)
}

# Calculate the EHI acclimation 
EHI_accl <- function(Data){
  
  # Set the window size
  window_size <- 3
  
  # Initialize the start and end indices of the window
  start_index <- 31
  end_index <- window_size
  
  # Initialize a new column called is_larger
  Data[, "EHI_accl"] <- 0
  
  # Loop through the dataset row by row with a 3 row window
  while ((start_index+2) <= nrow(Data)) {
    
    # Get the current window of rows
    current_window <- Data[(start_index-2):(start_index), ]
    
    # Get the mean value of the current window
    current_window_mean <- mean(current_window$TAVG)
    
    # Get the previous 30 rows
    prev_30_rows <- Data[(start_index-30):(start_index-1), ]
    
    # Get the mean value of the previous 30 rows
    prev_30_rows_mean <- mean(prev_30_rows$TAVG)
    
    # Check if the current window mean value is greater than the previous 30 rows mean value
    Data$EHI_accl[start_index] <- current_window_mean - prev_30_rows_mean 
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    end_index <- end_index + 1
  }
  return(Data)
}

#==============================================================================#
# Calculate Heat Wave
#==============================================================================#

EHF_pipeline <- function(dataset) {
  
  Test <- dataset %>%
    select(Zip, Date, TAVG, H95) %>%
    nest(data = c(-Zip)) %>%
    mutate(EHI_sig = future_map(data, EHI_sig),
           EHI_accl = future_map(data, EHI_accl)) %>%
    select(-data) %>% 
    unnest(cols = c(EHI_sig, EHI_accl), names_repair = "minimal") %>% #unnests the nested data
    subset(., select = which(!duplicated(names(.)))) %>% # Remove the duplicated column names 
     mutate(EHI_sig = if_else(EHI_sig < 0, 0, EHI_sig),
            EHI_accl = if_else(EHI_accl < 1, 1, EHI_accl),
            EHF = EHI_sig * EHI_accl)
}

# Run in parallel
plan(multisession, workers = (availableCores() - 1))
start <- now()
Temperature <- EHF_pipeline(Temperature)
end <- now()
end-start

Temperature <- left_join(Temperature, EHF_parallel_analysis %>% select(-TAVG, -H95), by = c("Zip", "Date")) 

write_parquet(Temperature, "NC_Heatwave.parquet")

