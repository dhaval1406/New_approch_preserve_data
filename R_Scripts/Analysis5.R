# List the files and removing everything
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(plyr)

#Set working directory
#  setwd("P:/TempData_06042013/")
setwd("P:/R/New_approch_preserve_data/")
load("Analysis_3.RData")

# Trying to capture runtime
begTime <- Sys.time()

#Function to take last url part and selecting second column for just match value
URL_parts <- function(x) {
  m <- regexec(".*/(\\w+)/$", x)
  do.call(rbind, lapply(regmatches(x, m), `[`, c(2L)))
}

url_parts_category_id <- URL_parts(as.character(search_log$Link_URL))
colnames(url_parts_category_id) <- c("CategoryId")

# Append URL part values back to original tabl
search_log_cat_id <- data.frame(search_log[c("Q", "Link_URL")], url_parts_category_id)

search_log_cat_sum <- ddply (search_log_cat_id, .(Q, CategoryId), summarise, 
                                 total_LinkCtg = length(Q)
                                )

# Sorting based on highest total_LinkCtg and keywords
search_log_cat_anl <- search_log_cat_sum[order(-search_log_cat_sum$total_LinkCtg, search_log_cat_sum$Q), ]

# Adding an extra column to display the sort order
search_log_cat_anl$sort_order = 1:nrow(search_log_cat_anl)

#Export results to csv file
write.csv(search_log_cat_anl, file = "keyword_analysis_5_new.csv", na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 

runTime

#======================================= Test Area ============================================================#
