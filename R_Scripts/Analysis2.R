# List the files and removing everything
ls ()
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(plyr)

#Set working directory
setwd("P:/R/New_approch_preserve_data/")
load("Data_Load_Prod.RData")

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

# Exporting results in csv
write.csv(keyword_search_counts, file = "keyword_analysis_2_week.csv", na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime

#=========================================================== Test the results =============================================

# Checking internal accounts
# search_log_internal <- subset(search_log, CUSTCD== "WOSMUS")
# codefix_log_internal <- subset(codeFix_log, CUSTCD== "WOSMUS")
# detailTab_log_internal <- subset(detailTab_log, CUSTCD== "WOSMUS")

# subset(total_keyword_search, Q %in% c("shaft", "washer"))

# Taking subsets for testing
# search_log <- subset(search_log, Q == 'plunger')
# codeFix_log <- subset(codeFix_log, SessionId %in% c('7d843f9657b534720ff88f603ac9febb', '61bbb9c516a449f54f6c3548b161fedb', 'b7ed91bf7ec90f5a31a9b74f57bb3884'))
# detailTab_log <- subset(detailTab_log, SessionId %in% c('7d843f9657b534720ff88f603ac9febb', '61bbb9c516a449f54f6c3548b161fedb', 'b7ed91bf7ec90f5a31a9b74f57bb3884'))

#subset(search_codefix_detailtab_merge, Q %in% c('MTSR8-300'))
#subset(keyword_search_counts_1, Q %in% c('MTSR8-300'))

#subset(search_codefix_detailtab_merge, Q %in% c('B6909ZZ', 'lead screw'))
#subset(keyword_search_counts_series, Q == 'B6909ZZ')
#creating a subset, e.g sessionid = something
#sub_codeFix <- subset(codeFix_log, SessionId %in% c('6e64b44adbec3cf7ad6cb820751159c0','7c6e99c7c2c4ac3edf848d86195d923b'), select = c("SessionId", "Referer", "PRODUCT_CODE"))

### creating a function in ddply - may be used later on to create a function
#ddply(sub_search, .(Q), function(x) {
#  x$new.var <- ifelse(x$Status == 'Link', 'My status is link', 'My status is not link')
#  return(x)
#})

# time elapsed by process, cpu time and running time
#system.time(search_log <- read.delim('us_dtuser.txt', header = TRUE, sep = "\t", quote = "", comment.char = ""))

# write.csv(subset(search_log, Q == 'shaft'),
#           , file = "seach_log_shaft.csv", na = "0", row.names = FALSE)
# 
# 
# library(sqldf)
# 
# shaft_subset <- subset(search_log, Q == 'shaft')
# 
# head(shaft_subset)
# 
# sqldf ("select * from shaft_subset s, codeFix_log t where (s.SessionId == t.SessionId and s.URL == t.Referer and s.SERIES_CODE == t.SERIES_CODE)")
# 
# write.csv(sqldf ("select * from codeFix_log where SessionId in (Select SessionId from search_log where Q == 'shaft')"),
#           , file = "codefix_shaft.csv", na = "0", row.names = FALSE)
# 
# 
# head(codeFix_log)