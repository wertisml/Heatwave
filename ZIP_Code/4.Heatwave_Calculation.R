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
                           do(data.frame(t(quantile(.$TAVG, probs = c(0.95, 0.97, 0.99, 0.01, 0.03, 0.05))))) %>%
                           rename(H95 = X95.,
                                  H97 = X97.,
                                  H99 = X99.,
                                  C1 = X1.,
                                  C3 = X3.,
                                  C5 = X5.),
                         by = c("Zip", "month")) %>%
  arrange(Date) %>%
  na.omit(.) # Remove any rows that contain missing values

#==============================================================================#
# Create Heatwave Functions
#==============================================================================#

# Calculate if the 3 day average is above the 95th percentile
Above_95th <- function(Data){
  
  # Set the window size
  window_size <- 3
  
  # Initialize the start and end indices of the window
  start_index <- 1
  end_index <- window_size
  
  # Initialize a new column called is_larger
  Data[, "Above_95th"] <- 0
  
  # Loop through the dataset row by row with a 3 row window
  while (end_index <= nrow(Data)) {
    # Get the current window of rows
    current_window <- Data[start_index:end_index, ]
    
    # Check if the current window is larger than the mean value of the whole column
    if (mean(current_window$TAVG) > mean(current_window$H95)) {
      # Set the value of is_larger to 1
      Data[, "Above_95th"][end_index] <- 1
    }
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    end_index <- end_index + 1
  }
  return(Data)
}

# Calculate if the 3 day average temperature is above the previous 30 day temperature
Above_Surrounding <- function(Data){
  
  # Set the window size
  window_size <- 3
  
  # Initialize the start and end indices of the window
  start_index <- 31
  end_index <- window_size
  
  # Initialize a new column called is_larger
  Data[, "Above_Surrounding"] <- 0
  
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
    if (current_window_mean > prev_30_rows_mean) {
      # Set the value of is_larger to 1
      Data[, "Above_Surrounding"][start_index] <- 1
    } 
    
    # Increment the start and end indices of the window
    start_index <- start_index + 1
    end_index <- end_index + 1
  }
  return(Data)
}

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
#Calculate Heat Wave
#==============================================================================#


Above_pipeline <- function(dataset) {

Test <- dataset %>%
  nest(data = c(-Zip)) %>%
  mutate(Above_95 = future_map(data, Above_95th),
         Above_surrounding = future_map(data, Above_Surrounding)) %>%
  select(-data) %>% 
  unnest(cols = c(Above_95, Above_surrounding), names_repair = "minimal") %>% #unnests the nested data
  subset(., select = which(!duplicated(names(.)))) # Remove the duplicated column names 

}

EHF_pipeline <- function(dataset) {
  
  Test <- dataset %>%
    nest(data = c(-Zip)) %>%
    mutate(EHI_sig = future_map(data, EHI_sig),
           EHI_accl = future_map(data, EHI_accl)) %>%
    select(-data) %>% 
    unnest(cols = c(EHI_sig, EHI_accl), names_repair = "minimal") %>% #unnests the nested data
    subset(., select = which(!duplicated(names(.)))) %>% # Remove the duplicated column names 
     mutate(EHI_sig = fifelse(EHI_sig < 0, 0, EHI_sig),
            EHI_accl = fifelse(EHI_accl < 1, 1, EHI_accl),
            EHF = EHI_sig * EHI_accl)
  
}

# Run in parallel
plan(multisession, workers = (availableCores() - 1))
start <- now()
#Above_parallel_analysis <- Above_pipeline(Temperature)
EHF_parallel_analysis <- EHF_pipeline(Temperature)
end <- now()
end-start

write_parquet(EHF_parallel_analysis, "NC_Heatwave.parquet")

