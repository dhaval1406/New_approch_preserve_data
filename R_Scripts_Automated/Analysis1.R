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

total_codefix <- ddply(codeFix_log, .(SessionId, Referer), summarise, 
                              codefix_count = length(unique(Referer))
                      )

# Merge total_keyword_search with search 
keyword_search_merge_anl <- merge(search_log, total_keyword_search, all.x=TRUE, by="Q", sort=FALSE)
codefix_merge <- merge(codeFix_log, total_codefix, all.x=TRUE, by=c("SessionId", "Referer"), sort=FALSE)

# Get unique values before merge
keyword_search_merge_anl <- unique(keyword_search_merge_anl[, c("Q", "SessionId", "URL", "sum_link", "sum_linkCtg", "total_search")])
codefix_merge <- unique(codefix_merge[, c("SessionId", "Referer", "codefix_count")])

#merging keyword_search_merge and codefix_merge frames
search_codefix_merge <- merge(
                            keyword_search_merge_anl,
                            codefix_merge,all.x=TRUE,
                            by.x = c("SessionId", "URL"), by.y = c("SessionId", "Referer"),
                            sort = FALSE  
                          )

keyword_search_counts_anl <- ddply(search_codefix_merge, .(Q, total_search, sum_linkCtg, sum_link), summarise, 
                                    total_codefix = sum(codefix_count, na.rm = T)
                                  )

# Sorting based on highest 
keyword_search_counts_anl <- keyword_search_counts_anl[order(-keyword_search_counts_anl$total_search), ]

# Format csv name
analysis_file <- paste0( paste("1_keyword_transition_ratio", from.date, to.date, sep="_" ), ".csv" )

# Exporting results in csv
write.csv(keyword_search_counts_anl, file = analysis_file, na = "0", row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime
#=========================================================== Test the results =============================================

