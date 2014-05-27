### ==============================================================================================
### THIS IS AUTOMATED VERSION OF THE PREVIOUS CODE WITH THE SAME/SIMILAR FILE NAME
###
### - This analysis is aimed for getting conversion for starting visits either uisng Category
###   Pages or Product pages.
###   
### - We are using Visit logs and CodeFix logs data   
###
### -  This requires data cleanup upfront such as 
###     - removing internal accounts
###     - removing misumi referers and only keeping third party referers    
### - Then split the clean data into Category and Product based on the initial URL access   
###     - URL with `vona2/detail` referes Product Page    
###     - URL with `vona2/mech` referes to Category Page
###    
### ==============================================================================================

# List the files and removing everything
rm(list = ls())
gc() #it will free memory

# Trying to capture runtime
begTime <- Sys.time()

#Getting the required package
require(data.table)
require(bit64)
require(lubridate)
#======================================== web log part ===================================================

  #Set working directory
  setwd("P:/Data_Analysis/Weblog_Data/")
  
  # Getting file names from command line arguments
  args <- commandArgs(trailingOnly = TRUE)
  
  c.file <- args[1]
  v.file <- args[2]
  
  # To run manually - specify files
#   c.file = "codefix_log_20140401_20140430.txt"
#   v.file = "visit_log_20140401_20140430.txt"
  
  # Reading data
  codeFix_log <- fread(c.file, header = TRUE, sep = "\t", na.strings=c("NA", ''), colClasses="character") 
  visit_log   <- fread(v.file, header = TRUE, sep = "\t", na.strings=c("NA", ''), colClasses="character") 

  # clean up data
  codeFix_log <- codeFix_log [, list (AccessDateTime, CUSTCD, SessionId, Referer) ]

  # Keep only unique records
  codeFix_log <- unique(codeFix_log)
  visit_log <- unique(visit_log)

  # removing blank keywords and convert  keywords to lower case and 
  # removing internal acccounts with CUSTCD = "WOSMUS"
  codeFix_log <- codeFix_log [!(CUSTCD == "WOSMUS")]
  visit_log <- visit_log [!(CUSTCD == "WOSMUS") ]
  
  # Convert AccessDatetime to date format and select data from 09/01/2013 to 02/28/2014
  visit_log$AccessDateTime   <- as.Date(visit_log$AccessDateTime)
  codeFix_log$AccessDateTime <- as.Date(codeFix_log$AccessDateTime)
  
  # Keep only external referes, not misumi
  visit_log <- visit_log[grep("^https?://us.misumi-ec.com/|^https?://www.misumiusa.com|^https?://www.misumi-ec.com", 
                                            Referer, ignore.case=T, invert=T), 
                                       list(AccessDateTime, CUSTCD, SessionId, URL, Referer, SERIES_CODE)]

  # Removing Misumi referer URLs
  codeFix_log <- codeFix_log[grep("^https?://us.misumi-ec.com/|^https?://www.misumiusa.com|^https?://www.misumi-ec.com", 
                                          Referer, ignore.case=T, invert=T)]

#  ------------------------------------------------------------------------

  # Removing blank/NA referers and/or SessionId
  visit_log <- visit_log[ !(is.na(Referer) | is.na(SessionId)) ]
  codeFix_log <- codeFix_log[ !(is.na(Referer) | is.na(SessionId)) ]

  # Splitting data for Category and Product page based on the initial URL access
  visit_log_cat  <- visit_log[grep("vona2/mech", URL, ignore.case=T)]
  visit_log_prod <- visit_log[grep("vona2/detail", URL, ignore.case=T)]
  
  # Setting key for faster performance
  setkey(codeFix_log   , SessionId)
  setkey(visit_log_cat , SessionId)
  setkey(visit_log_prod, SessionId)

  #
  # Producing monthly results
  # 
  visit_codeFix_cat <-  codeFix_log[visit_log_cat, nomatch=0, allow.cartesian=T]
  visit_codeFix_prod <- codeFix_log[visit_log_prod, nomatch=0, allow.cartesian=T]

  result_table <- data.table( total_queries_cat  = nrow(visit_log_cat), 
                              total_queries_prod = nrow(visit_log_prod), 
                              total_codefix_cat  = nrow(visit_codeFix_cat), 
                              total_codefix_prod = nrow(visit_codeFix_prod) )
  
  # rounding results to 3 decimal points
  result_table <- result_table[, `:=` ( cat_conversion  = round(total_codefix_cat/total_queries_cat, 3), 
                                        prod_conversion = round(total_codefix_prod/total_queries_prod, 3)
                                       )]

  # Get current month name
  library(lubridate)
  curr_month = as.character(month(Sys.Date(), label=TRUE))
  
  # making file name
  analysis_file = paste0("P:/Data_Analysis/Analysis_Results/15_cat_prod_starting_visits", "_", curr_month, ".csv" )
  
  # Exporting results to csv
  write.csv(result_table, file = analysis_file, na = '', row.names = FALSE)
