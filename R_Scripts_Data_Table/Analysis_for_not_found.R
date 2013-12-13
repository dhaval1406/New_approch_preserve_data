# This Data analysis is similar to Analysis 7 that uses below data from different servers
# Search_log, codeFix_log  -> web server logs
# SO data - from SQL server's SO table

### Changes compare to previous analysis
# SEARCH_TYPE == 1 for seach_log
# CUSTCD is used to match Customer_Code from SO table
# Logic for from.date and to.date
# Summarizing on category_2

# List the files and removing everything
rm(list = ls())
gc() #it will free memory

# Trying to capture runtime
begTime <- Sys.time()

#Getting the required package
require(plyr)
require(data.table)
require(bit64)
#======================================== web log part ===================================================
#Set working directory
setwd("P:/Data_Analysis/Weblog_Data/")

#Files
s.file = c('search_log_20130701_20130801.txt',
          'search_log_20130801_20130901.txt',
          'search_log_20130901_20131001.txt',
          'search_log_20131001_20131101.txt')
 

#Extract multiple files from an archive with the target weblog directory
sapply(s.file, function(x) {
  system(paste0("7z x -y P:/Data_Analysis/Archive/archive_2013.7z ", x , " -o", getwd()))
})

# #Extract a specific file from an archive with the target weblog directory
# system(paste0("7z x -y P:/Data_Analysis/Archive/archive_2013.7z ", s.file, " -o", getwd()))

#Importing data
# search_log <- fread(s.file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 

##### Import all monthly data
      fileNames <- list.files(path = getwd(), pattern = "search_log_*")
      search_log <- rbindlist(lapply(fileNames, function(x) {
        xx <- fread(x, header = TRUE, sep = "\t", na.strings=c("NA", ''))
      }))
      
      search_log <- unique(search_log)

############################################################################

#clean up data
search_log[, c("USER_CODE",  "CookieId", "BRD_CODE", "IP", "UA", "LogFileName", "UpdateDate", "V17", "URL") := NULL]

# removing blank keywords and convert  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
# keeping results only with SEARCH_TYPE == 1 and status= not found
search_log_clean <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & 
                           (SEARCH_TYPE == 1) & (Status=='NotFound')] [, Q := tolower(Q)]

# Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
# search_log_clean$Q <- gsub("(.+[^s])s$", "\\1", search_log_clean$Q)

### summarize total not found keywords
setkey(search_log_clean, Q)

search_log_clean_sum <- search_log_clean[, list(total_not_found = sum(Status=='NotFound')), by=Q] [order(-total_not_found)]

# Exporting results to csv
write.csv(search_log_clean_sum, file = "P:/Data_Analysis/Analysis_Results/Not_found_kwrds_0701_1101.csv", na = '', row.names = FALSE)

# Delete files after processing data
# unlink(c(s.file, c.file))

runTime <- Sys.time()-begTime 
runTime

#================================ TEST ================================================
