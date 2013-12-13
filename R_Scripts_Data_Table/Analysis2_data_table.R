# List the files and removing everything
ls ()
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(plyr)

#Set working directory
setwd("P:/Data_Analysis/Analysis_Results/")

# Load already processed data
image_file_name <- paste0("P:/Data_Analysis/Processed_R_Datasets/Data_Load_Prod_", 
                          format(Sys.time(), "%m%d%Y"), ".RData")
load(image_file_name) 

# Trying to capture runtime
begTime <- Sys.time()

# ============================================== Main Algorithem =======================================

### Summarize CodeFix counts
  total_codefix <- codeFix_log
  
  # Set the key for faster operations
   setkey(total_codefix, SessionId, Referer, SERIES_CODE)
  
  # Calculate codefix_count summerizing by SessionId and Referer URL
  total_codefix[, codefix_count := length(unique(Referer)), by=list(SessionId, Referer, SERIES_CODE)] 
  
  # Keep unique records with the fields that we need
  setkey(total_codefix, NULL)
  total_codefix <- unique(total_codefix[, c("SessionId", "Referer","SERIES_CODE", "codefix_count"), with=FALSE])

# Removing blank series codes
keyword_search_merge <- search_log[!is.na(SERIES_CODE)]

# Adding extra field to capture links by series code
keyword_search_merge$link_series <- 1

# Get unique values before merge
setkey(detailTab_log, NULL)
detailtab_merge <- unique(detailTab_log[, c("SessionId", "SERIES_CODE", "tab_count"), with=FALSE])

# Setting keys for merging data tables
keyword_search_merge_anl <- keyword_search_merge

setkey(keyword_search_merge_anl, SessionId, URL, SERIES_CODE)
setkey(total_codefix, SessionId, Referer, SERIES_CODE)

#merging keyword_search_merge and codefix_merge frames
# data.table doesn't support by.x by.y yet. So joining by using cbind
# A good example is here - http://stackoverflow.com/questions/14126337/multiple-joins-merges-with-data-tables/14126721#14126721
search_codefix_merge_series <- cbind( keyword_search_merge_anl, 
                                       total_codefix[J(keyword_search_merge_anl$SessionId, 
                                                       keyword_search_merge_anl$URL,
                                                       keyword_search_merge_anl$SERIES_CODE)
                                                     ])

### merging keyword_search_merge and codefix_merge frames
  setkey(search_codefix_merge_series, SessionId, SERIES_CODE)
  setkey(detailtab_merge, SessionId, SERIES_CODE)
  
  # merge by keys, remove non-matched lines (making it innter join)
  search_codefix_detailtab_merge<-detailtab_merge[search_codefix_merge_series, nomatch=0] 

# Select unique records - replacing missing values as 0 
# giving multiple results for multiple session ID. So replaced with ddply below

  keyword_search_counts <- search_codefix_detailtab_merge [ ,list( total_codefix = sum(codefix_count, na.rm = T),
                                                                   total_tabcount = sum(tab_count, na.rm = T),
                                                                   links_by_series = sum(link_series, na.rm = T) 
                                                                  ), 
                                                             by = list(Q, SERIES_CODE, total_search, sum_link, sum_linkCtg)
                                                          ]  [order(-total_search, Q, SERIES_CODE)] 

# Format csv name
analysis_file = format_name("2_keyword_webid");

# Exporting results in csv
write.csv(keyword_search_counts, file = analysis_file, na = "0", row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime

#=========================================================== Test the results =============================================
# library(sqldf)
# sqldf ("select * from shaft_subset s, codeFix_log t where (s.SessionId == t.SessionId and s.URL == t.Referer and s.SERIES_CODE == t.SERIES_CODE)")
# 
# write.csv(sqldf ("select * from codeFix_log where SessionId in (Select SessionId from search_log where Q == 'shaft')"),
#           , file = "codefix_shaft.csv", na = "0", row.names = FALSE)
 
