library(future)
library(furrr)
library(tidyverse)
library(arrow)
library(data.table)

setwd("~/Heatwave/Data/Zip")

#==============================================================================#
# Read in and set up data
#==============================================================================#

Temperature <- left_join(open_dataset("NC_PRISM_ZIP_2008_2021.parquet") %>% # Create a df of the temeprature data and rename the columns
                           rename(Tdmean = tdmean,
                                  TMIN = tmin,
                                  TMAX = tmax,
                                  TAVG = tmean) %>%
                           mutate(month = month(Date)) %>%
                           collect(),
                         open_dataset("NC_PRISM_ZIP_2008_2021.parquet") %>% # Create a df for extreme percentile of temperatures
                           rename(Tdmean = tdmean,
                                  TMIN = tmin,
                                  TMAX = tmax,
                                  TAVG = tmean) %>%
                           mutate(month = month(Date)) %>%
                           collect() %>%
                           group_by(Zip, month) %>%
                           do(data.frame(t(quantile(.$TAVG, probs = c(0.01, 0.03, 0.05))))) %>%
                           rename(C1 = X1.,
                                  C3 = X3.,
                                  C5 = X5.),
                         by = c("Zip", "month")) %>%
  arrange(Date) %>%
  na.omit(.) # Remove any rows that contain missing values

#==============================================================================#
# Create Coldwave Functions
#==============================================================================#

# Calculate if the 3 day average is above the 95th percentile
Below_5th <- function(Data){
  
  # Set the window size
  window_size <- 3
  
  # Initialize the start and end indices of the window
  start_index <- 1
  end_index <- window_size
  
  # Initialize a new column called is_larger
  Data[, "Below_5th"] <- 0
  
  # Loop through the dataset row by row with a 3 row window
  while (end_index <= nrow(Data)) {
    # Get the current window of rows
    current_window <- Data[start_index:end_index, ]
    
    # Check if the current window is larger than the mean value of the whole column
    if (mean(current_window$TAVG) < mean(current_window$C5)) {
      # Set the value of is_larger to 1
      Data[, "Below_5th"][end_index] <- 1
    }
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    end_index <- end_index + 1
  }
  return(Data)
}

# Calculate if the 3 day average temperature is above the previous 30 day temperature
Below_Surrounding <- function(Data){
  
  # Set the window size
  window_size <- 3
  
  # Initialize the start and end indices of the window
  start_index <- 31
  end_index <- window_size
  
  # Initialize a new column called is_larger
  Data[, "Below_Surrounding"] <- 0
  
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
    if (current_window_mean < prev_30_rows_mean) {
      # Set the value of is_larger to 1
      Data[, "Below_Surrounding"][start_index] <- 1
    } 
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    end_index <- end_index + 1
  }
  return(Data)
}

# Calculate the EHI significance
ECI_sig <- function(Data){
  
  # Set the window size
  window_size <- 3
  
  # Initialize the start and end indices of the window
  start_index <- 1
  end_index <- window_size
  
  # Initialize a new column called is_larger
  Data[, "ECI_sig"] <- 0
  
  # Loop through the dataset row by row with a 3 row window
  while (end_index <= nrow(Data)) {
    # Get the current window of rows
    current_window <- Data[start_index:end_index, ]
    
    # Check if the current window is larger than the mean value of the whole column
    Data$ECI_sig[end_index] <- mean(current_window$TAVG) - mean(current_window$C5) 
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    end_index <- end_index + 1
  }
  return(Data)
}

# Calculate the EHI acclimation 
ECI_accl <- function(Data){
  
  # Set the window size
  window_size <- 3
  
  # Initialize the start and end indices of the window
  start_index <- 31
  end_index <- window_size
  
  # Initialize a new column called is_larger
  Data[, "ECI_accl"] <- 0
  
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
    
    Data$ECI_accl[start_index] <- current_window_mean - prev_30_rows_mean 
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    end_index <- end_index + 1
  }
  return(Data)
}

#==============================================================================#
# Calculate Heat Wave
#==============================================================================#


Below_pipeline <- function(dataset) {
  
  Test <- dataset %>%
    nest(data = c(-Zip)) %>%
    mutate(Below_5 = future_map(data, Below_5th),
           Below_surrounding = future_map(data, Below_Surrounding)) %>%
    select(-data) %>% 
    unnest(cols = c(Below_5, Below_surrounding), names_repair = "minimal") %>% #unnests the nested data
    subset(., select = which(!duplicated(names(.)))) # Remove the duplicated column names 
  
}

ECF_pipeline <- function(dataset) {
  
  Test <- dataset %>%
    nest(data = c(-Zip)) %>%
    mutate(ECI_sig = future_map(data, ECI_sig),
           ECI_accl = future_map(data, ECI_accl)) %>%
    select(-data) %>% 
    unnest(cols = c(ECI_sig, ECI_accl), names_repair = "minimal") %>% #unnests the nested data
    subset(., select = which(!duplicated(names(.)))) %>% # Remove the duplicated column names 
    mutate(ECI_sig = fifelse(ECI_sig > 0, 0, ECI_sig),
           ECI_accl = fifelse(ECI_accl < -1, ECI_accl, -1),
           ECF = ECI_sig * ECI_accl)
  
}

# Run in parallel
plan(multisession, workers = (availableCores() - 1))
start <- now()
#Below_parallel_analysis <- Below_pipeline(Temperature)
ECF_parallel_analysis <- ECF_pipeline(Temperature)
end <- now()
end-start

write_parquet(ECF_parallel_analysis, "NC_Coldwave.parquet")


