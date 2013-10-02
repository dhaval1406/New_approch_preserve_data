# List the files and removing everything
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(plyr)

#Set working directory
setwd("P:/Data_Analysis/Analysis_Results/")

# Load already processed data
load("P:/Data_Analysis/Processed_R_Datasets/Data_Load_Prod.RData") 

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
analysis_file = format_name("1_keyword_transition_ratio");

# Exporting results in csv
write.csv(keyword_search_counts_anl, file = analysis_file, na = "0", row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime
#=========================================================== Test the results =============================================

