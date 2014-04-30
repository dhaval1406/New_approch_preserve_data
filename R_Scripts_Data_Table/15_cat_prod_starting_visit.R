### ==============================================================================================
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
  
  # s.file <- args[1]
  # c.file <- args[2]
  # cad.file <- args[3]
  
  # To run manually - specify files
  # s.file = "search_log_20130901_20140228.txt"
  c.file = "codefix_log_20130901_20140228.txt"
  v.file = "visit_log_20130501_20140301.txt"
  v1.file = "visit_log_20140302_20140331.txt"
  
  #Importing data
  codeFix_log_aug_sept <- fread("codefix_log_20130801_20130901.txt", header = TRUE, sep = "\t", na.strings=c("NA", '')) 
  codeFix_log_sept_feb <- fread(c.file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 
  codeFix_log_feb_march <- fread("codefix_log_20140301_20140331.txt", header = TRUE, sep = "\t", na.strings=c("NA", '')) 
  
  codeFix_log <- rbind(codeFix_log_aug_sept, codeFix_log_sept_feb, codeFix_log_feb_march)

  visit_log <- fread(v.file, header = TRUE, sep = "\t", na.strings=c("NA", ''), colClasses="character") 
  visit_log.1 <- fread(v1.file, header = TRUE, sep = "\t", na.strings=c("NA", ''), colClasses="character") 
  
  # clean up data
  codeFix_log <- codeFix_log [, list (AccessDateTime, CUSTCD, SessionId, Referer, SERIES_CODE) ]

  # Formatting Series_Code - fread assigns it to class 'integer64'
  codeFix_log$SERIES_CODE = as.character(codeFix_log$SERIES_CODE)
  
  # Keep only unique records
  codeFix_log <- unique(codeFix_log)
  visit_log <- unique(visit_log)
  
  # removing blank keywords and convert  keywords to lower case and 
  # removing internal acccounts with CUSTCD = "WOSMUS"
  # search_log <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1) ] [, Q := tolower(Q)]
  codeFix_log <- codeFix_log [!(CUSTCD == "WOSMUS")]
  visit_log <- visit_log [!(CUSTCD == "WOSMUS") ]
  visit_log.1 <- visit_log.1 [!(CUSTCD == "WOSMUS") ]
  
  # Convert AccessDatetime to date format and select data from 09/01/2013 to 02/28/2014
  visit_log$AccessDateTime <- as.Date(visit_log$AccessDateTime)
  visit_log.1$AccessDateTime <- as.Date(visit_log.1$AccessDateTime)
  
  codeFix_log$AccessDateTime <- as.Date(codeFix_log$AccessDateTime)
  
  # Data between August and March
  visit_logFromAug <- visit_log[AccessDateTime >='2013-08-01' & AccessDateTime <'2014-03-01']
  
  # Combine with data from March
  visit_logFromAug <- rbind(visit_logFromAug, visit_log.1)

  # Keep only external referes, not misumi
  visit_logFromAug <- visit_logFromAug[grep("^https?://us.misumi-ec.com/|^https?://www.misumiusa.com|^https?://www.misumi-ec.com", 
                                            Referer, ignore.case=T, invert=T), 
                                       list(AccessDateTime, CUSTCD, SessionId, URL, Referer, SERIES_CODE)]


  # Removing blank/NA referers
  visit_logFromAug <- visit_logFromAug[!is.na(Referer)]
  
  # Splitting data for Category and Product page based on the initial URL access
  visit_logFromAug_cat  <- visit_logFromAug[grep("vona2/mech", URL, ignore.case=T)]
  visit_logFromAug_prod <- visit_logFromAug[grep("vona2/detail", URL, ignore.case=T)]

  # Removing blank session id
  codeFix_log <- codeFix_log[!is.na(SessionId)]
  
  setkey(codeFix_log          , SessionId)
  setkey(visit_logFromAug_cat , SessionId)
  setkey(visit_logFromAug_prod, SessionId)

  #
  # Figure out the date logic to split the result for each month
  #
  from_dates <- c('2013-08-01', '2013-09-01', '2013-10-01', '2013-11-01', '2013-12-01', '2014-01-01', '2014-02-01', '2014-03-01', '2014-04-01')
  to_dates   <- c('2013-09-01', '2013-10-01', '2013-11-01', '2013-12-01', '2014-01-01', '2014-02-01', '2014-03-01', '2014-04-01', '2014-05-01')

  # Producing monthly results
  for(i in 1:length(from_dates)){

      codeFix_log_monthly <- codeFix_log[AccessDateTime >= from_dates[i] & AccessDateTime < to_dates[i]]
      visit_logFromAug_cat_montly <- visit_logFromAug_cat[AccessDateTime >= from_dates[i] & AccessDateTime < to_dates[i]]
      visit_logFromAug_prod_montly <- visit_logFromAug_prod[AccessDateTime >= from_dates[i] & AccessDateTime < to_dates[i]]
    
      visit_codeFix_cat_monthly  <- codeFix_log_monthly[visit_logFromAug_cat_montly,  nomatch=0]
      visit_codeFix_prod_monthly <- codeFix_log_monthly[visit_logFromAug_prod_montly, nomatch=0]
    
      result_table <- data.table( total_queries_cat  = nrow(visit_logFromAug_cat_montly), 
                                  total_queries_prod = nrow(visit_logFromAug_prod_montly), 
                                  total_codefix_cat = nrow(visit_codeFix_cat_monthly), 
                                  total_codefix_prod = nrow(visit_codeFix_prod_monthly) )
      
      result_table <- result_table[, `:=` ( cat_conversion  = round(total_codefix_cat/total_queries_cat, 3), 
                                            prod_conversion = round(total_codefix_prod/total_queries_prod, 3)
                                           )]
  
      analysis_file = paste0("P:/Data_Analysis/Analysis_Results/15_cat_prod_starting_visits", "_", month(from_dates[i], label=TRUE), ".csv" )
      
      # Exporting results to csv
      write.csv(result_table, file = analysis_file, na = '', row.names = FALSE)
  }
