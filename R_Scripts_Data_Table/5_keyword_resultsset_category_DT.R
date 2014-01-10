# List the files and removing everything
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(data.table)

#Set working directory
setwd("P:/Data_Analysis/Analysis_Results/")

# Load already processed data
load("P:/Data_Analysis/Processed_R_Datasets/Analysis_3.RData")

# Trying to capture runtime
begTime <- Sys.time()

# Pre req : Using R data from Analysis 3. Analysis 3 code must be run uptil here.
#============================================== Main Algorithem =======================================

#Function to take last url part and selecting second column for just match value
URL_parts <- function(x) {
  m <- regexec(".*/(\\w+)/$", x)
  do.call(rbind, lapply(regmatches(x, m), `[`, c(2L)))
}

url_parts_category_id <- URL_parts(as.character(search_log$Link_URL))
colnames(url_parts_category_id) <- c("CategoryId")

# Append URL part values back to original table
search_log_cat_id <- data.table(search_log[, list(Q, Link_URL)], url_parts_category_id)

# Summarizing by Keywords and CategoryId
# Sorting based on highest total_LinkCtg and keywords
search_log_cat_anl <- search_log_cat_id[, list(total_LinkCtg = .N),
                                          by=list(Q, CategoryId)
                                       ] [ order(-total_LinkCtg, Q) ] 
  
  
# Adding an extra column to display the sort order
search_log_cat_anl [, sort_order:= 1:nrow(search_log_cat_anl)]

# Format csv name
analysis_file = format_name("5_keyword_resultsset_category")

# Exporting results in csv
write.csv(search_log_cat_anl, file = analysis_file, na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime

#======================================= Test Area ============================================================#
