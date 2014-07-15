### ==============================================================================================
### Goal: Create keyword table for suggestion features, with formatted URLs based on keyword's 
###       optimization status 
### 
### ==============================================================================================

# List the files and removing everything
  rm(list = ls())
  gc() #it will free memory

# Trying to capture runtime
  begTime <- Sys.time()

#Getting the required package
  require(data.table)
  require(xlsx)
  require(RCurl)
  require(bit64)
  require(plyr)

#Set working directory
  setwd("P:/Data_Analysis/Weblog_Data/")

### Which files to process
  search_file <- "search_log_20131212_20140112.txt"

# Importing data - Reading with encoding UTF-8
  search_log <- as.data.table(read.delim(search_file, header = TRUE, sep = "\t", , na.strings=c("NA", ''), encoding="UTF-8"))

# clean up data
  search_log[, c("USER_CODE",  "CookieId", "BRD_CODE", "IP", "UA", "LogFileName", "UpdateDate", "X") := NULL]

### ================================================================
### Data clean up - reomving unnecessary keywords, part numbers etc.
### ================================================================

# removing blank series codes and conver  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
  search_log <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1)] [, Q := tolower(Q)]

# Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
# Removing commas around the words
# Removing keywords with control characters, and words that only contains punctuation characters
# Removing potential part numbers with more than one dash, or more than 4 digits
  search_log$Q <- gsub("(.*?)'(.+)'", "\\1\\2", search_log$Q)
  search_log$Q <- gsub("(.+[^s])s$|'$", "\\1", search_log$Q)
  search_log <- search_log[c(grep("[[:cntrl:]]|^[[:punct:]]$", search_log$Q, ignore.case=T, invert=T)), ]
  search_log <- search_log[!grep("(.+-.+){2,}|(.*\\d.*){4,}", Q)]

# Seperate keywords with more than 3 digits and less than 2 digits
# Then select 2 of more digit keywords that has a space
  search_log_2_or_more <- search_log[grep("(.*\\d.*){2,}", Q)] 
  search_log_less_than_2 <- search_log[!grep("(.*\\d.*){2,}", Q)]  
  search_log_2_or_more <- search_log_2_or_more[grep("\\s", Q)]

# Combine the results
  search_log <- rbind(search_log_2_or_more, search_log_less_than_2)

# Summarize total keyword searches
  setkey(search_log, Q)
  search_log[, total_search := sum (sum(Status=='NotFound'), (Status=='Hit')), by=Q] 

# Removing NotFound resutls
  search_log <- search_log[!(Status=='NotFound')]

# To get correct unique records, setting key to NULL
  setkey(search_log, NULL)
  total_keyword_search <- unique(search_log[, c("Q", "total_search"), with=FALSE])

### ================================================================
### Create keyword search result URL, based on keyword optimization
### that match keyword.cat.data
###       Optimized keyword: /kwsrc/?kwd=
###       Non: /vona2/result/?Keyword=
### ===============================================================

# Read in previouly submitted files
  submitted_file_cat = "P:/Data_Analysis/Keyword_Analysis_Results_Gordana/Updated_after_phase1_Gordana/submitted_12162013/keyword_category_jp_format_12162013.txt"
  submitted_file_series = "P:/Data_Analysis/Keyword_Analysis_Results_Gordana/Updated_after_phase1_Gordana/submitted_12162013/keyword_series_jp_format.txt"

  keywords_cat <- fread(submitted_file_cat, header = F, sep = "\t", na.strings=c("NA", '')) 
  keywords_series <- fread(submitted_file_series, header = F, sep = "\t", na.strings=c("NA", '')) 

# Combine category and series data to get list of keywords
  submitted_keywords <- rbind(keywords_cat, keywords_series)

# Seperate optimized and non-optimized 
  optimized_keyword_search <- total_keyword_search[total_keyword_search$Q %in% as.character(unique(submitted_keywords$V1)), ]
  non_optimized_keyword_search <- total_keyword_search[!total_keyword_search$Q %in% as.character(unique(submitted_keywords$V1)), ]

# Replace space in the keywords with `+` for URL
  optimized_keyword_search$URL <- gsub(" ", "+", optimized_keyword_search$Q)
  non_optimized_keyword_search$URL <- gsub(" ", "+", non_optimized_keyword_search$Q)

# Create URLs
  optimized_keyword_search$URL <- paste0("/kwsrc/?kwd=", optimized_keyword_search$URL)
  non_optimized_keyword_search$URL <- paste0("/vona2/result/?Keyword=", non_optimized_keyword_search$URL)

# Combine optimized and non-optimized results
  all_keyword_search <- rbind(optimized_keyword_search, non_optimized_keyword_search)
  all_keyword_search <- unique(all_keyword_search[order(-total_search)])

# Exports results to CSVs
  write.csv(all_keyword_search, file = "P:/Data_Analysis/Analysis_Results/keyword_table_suggestion_20131212_20140112_2_or_more.csv", na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime
#=========================================================== Test the results =============================================
