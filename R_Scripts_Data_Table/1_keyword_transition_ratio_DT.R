# List the files and removing everything
ls ()
rm(list = ls())
gc() #it will free memory

#Getting the required package
# require(plyr)
require(data.table)
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

### Summarize CodeFix counts
  total_codefix <- codeFix_log
  
  # Set the key for faster operations
  setkey(total_codefix, SessionId, Referer)
  
  # Calculate codefix_count summerizing by SessionId and Referer URL
  total_codefix[, codefix_count := length(unique(Referer)), by=list(SessionId, Referer)] 
  
  # Keep unique records with the fields that we need
  setkey(total_codefix, NULL)
  total_codefix <- unique(total_codefix[, c("SessionId", "Referer", "codefix_count"), with=FALSE])

# - ATYARE NATHI JARUR PAN MAY NEED - MAY BE UPAR NU PAN NAHI JOYEEYE

### Merge total_keyword_search with search 
#   keyword_search_merge_anl <- merge(search_log, total_keyword_search, all.x=TRUE, by="Q", sort=FALSE)
#   codefix_merge <- merge(codeFix_log, total_codefix, all.x=TRUE, by=c("SessionId", "Referer"), sort=FALSE)

# Setting key to Null, otherwise it will only show unique results for Q
setkey(search_log, NULL)
keyword_search_merge_anl <- unique(search_log[, c("Q", "SessionId","URL", "sum_link", "sum_linkCtg", "total_search"), with=FALSE])

# Setting keys for merging data tables
setkey(keyword_search_merge_anl, SessionId, URL)
setkey(total_codefix, SessionId, Referer)

#merging keyword_search_merge and codefix_merge frames
# data.table doesn't support by.x by.y yet. So joining by using cbind
# A good example is here - http://stackoverflow.com/questions/14126337/multiple-joins-merges-with-data-tables/14126721#14126721
search_codefix_merge <- cbind( keyword_search_merge_anl, 
                               total_codefix[J(keyword_search_merge_anl$SessionId, 
                                               keyword_search_merge_anl$URL)
                                             ][, list(codefix_count=codefix_count)])

# Sorting based on highest 
  keyword_search_counts_anl <- search_codefix_merge [ ,list(total_codefix = sum(codefix_count, na.rm = T)), 
                                                       by = list(Q, total_search, sum_linkCtg, sum_link)
                                                    ] [order(-total_search)] 
# Format csv name
analysis_file <- paste0( paste("1_keyword_transition_ratio", from.date, to.date, sep="_" ), ".csv" )

# Exporting results in csv
write.csv(keyword_search_counts_anl, file = analysis_file, na = "0", row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime
#=========================================================== Test the results =============================================

#keyword_search_counts <- count(search_codefix_merge, .(Q, sum_link, sum_linkCtg, total_search, codefix_count))
