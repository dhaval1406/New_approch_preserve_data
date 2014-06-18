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
require(xlsx)
#======================================== web log part ===================================================
#Set working directory
setwd("P:/Data_Analysis/Weblog_Data/")

#Files from 12/01/2013 to 04/30/2014
fileNames = c(  'search_log_20131201_20140101.txt',
                'search_log_20140101_20140131.txt',
                'search_log_20140201_20140228.txt',
                'search_log_20140301_20140331.txt',
                'search_log_20140401_20140430.txt'
              )
 
#Extract multiple files from an archive with the target weblog directory
# Not needed this time sine the data are in weblog directory
# sapply(fileNames, function(x) {
#   system(paste0("7z x -y P:/Data_Analysis/Archive/archive_2013.7z ", x , " -o", getwd()))
# })

# #Extract a specific file from an archive with the target weblog directory
# system(paste0("7z x -y P:/Data_Analysis/Archive/archive_2013.7z ", fileNames, " -o", getwd()))

#Importing data
# search_log <- fread(s.file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 

##### Import all monthly data
      #fileNames <- list.files(path = getwd(), pattern = "search_log_*")

      search_log <- rbindlist(lapply(fileNames, function(x) {
        xx <- fread(x, header = TRUE, sep = "\t", na.strings=c("NA", ''))
      }))
      
      search_log <- unique(search_log)


#  ------------------------------------------------------------------------
# clean up data
search_log[, c("USER_CODE",  "CookieId", "BRD_CODE", "IP", "UA", "LogFileName", "UpdateDate", "V17", "URL") := NULL]

# removing blank keywords and convert  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
# keeping results only with SEARCH_TYPE == 1 and status= not found
search_log_clean <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & 
                           (SEARCH_TYPE == 1) & (Status=='NotFound')] [, Q := tolower(Q)]

search_log_clean_found <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & 
                             (SEARCH_TYPE == 1) & (Status!='NotFound')] [, Q := tolower(Q)]


# Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
# search_log_clean$Q <- gsub("(.+[^s])s$", "\\1", search_log_clean$Q)

### summarize total not found keywords
setnames(search_log_clean, "Q", "keyword")
setnames(search_log_clean_found, "Q", "keyword")

setkey(search_log_clean, keyword)
setkey(search_log_clean_found, keyword)

search_log_clean_sum <- search_log_clean[, list(total_not_found = sum(Status=='NotFound')), by=keyword] [order(-total_not_found)]
search_log_clean_sum_found <- search_log_clean_found[, list(total_found = .N), by=keyword] [order(-total_found)]

# Filtering optimized keywords from search_log ----------------------------
optimized.keywords <- as.data.table( 
    read.xlsx2( "P:/Data_Analysis/Keyword_Analysis_Results_Gordana/phase4_03282014/Real_Work_merge_permutations_03282014/optimized_keywords_05092014.xlsx", 
                sheetIndex=1, stringsAsFactors= F ) 
)

# Keeping only new not found keywords
search_log_clean_sum <- search_log_clean_sum[!keyword %in% optimized.keywords$keyword]
search_log_clean_sum_found <- search_log_clean_sum_found[!keyword %in% optimized.keywords$keyword]

# Exporting results to csv
write.csv(search_log_clean_sum, file = "P:/Data_Analysis/Analysis_Results/Not_found_kwrds_122013_042014.csv", na = '', row.names = FALSE)
write.csv(search_log_clean_sum_found, file = "P:/Data_Analysis/Analysis_Results/found_kwrds_122013_042014.csv", na = '', row.names = FALSE)

# Delete files after processing data
# unlink(c(s.file, c.file))

runTime <- Sys.time()-begTime 
runTime

#================================ TEST ================================================
