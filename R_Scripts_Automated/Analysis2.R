# List the files and removing everything
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(plyr)

# Getting dates and download type from command line arguments
args <- commandArgs(trailingOnly = TRUE)

from.date <- args[1]
to.date <- args[2]
download_type <- args[3]

#Set working directory
setwd(paste0("P:/Data_Analysis/Analysis_Results/", download_type, '_results/'))

# Load already processed data
image_file_name <- paste0("P:/Data_Analysis/Processed_R_Datasets/", 
                          paste("Data_Load_Prod", from.date, to.date, sep="_" ), ".RData")
load(image_file_name) 

# Trying to capture runtime
begTime <- Sys.time()
#============================================== Main Algorithem =======================================

total_codefix <- ddply(codeFix_log, .(SessionId, Referer, SERIES_CODE), summarise, 
                       codefix_count = length(unique(Referer))
)

# Merge total_keyword_search with search 
keyword_search_merge <- merge(search_log, total_keyword_search, by=c("Q"), sort=FALSE)

# Removing blank series codes
keyword_search_merge <- subset(keyword_search_merge, is.na (SERIES_CODE) == FALSE) 
codefix_merge <- merge(codeFix_log, total_codefix, all.x=TRUE, by=c("SessionId", "Referer", "SERIES_CODE"), sort=FALSE)
detailtab_merge <- merge(detailTab_log, total_detailtab, all.X=TRUE, by=c("SessionId", "SERIES_CODE"), sort=FALSE)

# Adding extra field to capture links by series code
keyword_search_merge$link_series <- 1

# Get unique values before merge
codefix_merge <- unique(codefix_merge[, -4])
detailtab_merge <- unique(detailtab_merge[, c("SessionId", "SERIES_CODE", "tab_count")])

#merging keyword_search_merge and codefix_merge frames
search_codefix_merge_series <- merge(
  keyword_search_merge,
  codefix_merge,all.x=TRUE,
  by.x = c("SessionId", "URL", "SERIES_CODE"), by.y = c("SessionId", "Referer", "SERIES_CODE"),
  sort = FALSE  
)

#merging keyword_search_merge and codefix_merge frames
search_codefix_detailtab_merge<- merge(
  search_codefix_merge_series,
  detailtab_merge,
  by.x = c("SessionId", "SERIES_CODE"), by.y = c("SessionId", "SERIES_CODE"),
  sort = FALSE  
)

# Select unique records - replacing missing values as 0 
# giving multiple results for multiple session ID. So replaced with ddply below
keyword_search_counts <- ddply(search_codefix_detailtab_merge, .(Q, SERIES_CODE, total_search, sum_link, sum_linkCtg), summarise, 
                              total_codefix = sum(codefix_count, na.rm = T), 
                              total_tabcount = sum(tab_count),
                              links_by_series = sum(link_series) 
                              )

# Format csv name
analysis_file <- paste0( paste("2_keyword_webid", from.date, to.date, sep="_" ), ".csv" )

# Exporting results in csv
write.csv(keyword_search_counts, file = analysis_file, na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime

#=========================================================== Test the results =============================================
