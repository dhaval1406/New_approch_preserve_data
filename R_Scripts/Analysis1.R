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

write.csv(keyword_search_counts_anl, file = "keyword_analysis_1_new.csv", na = "0", row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime
#=========================================================== Test the results =============================================

# Last ddply can be written using data.table - super fast

# library(data.table)
# DT <- data.table::data.table(search_codefix_merge)
# keyword_search_counts_anl2 <- DT [,list(total_codefix = sum(codefix_count, na.rm = T)), by = list(Q, total_search, sum_linkCtg, sum_link)][order(-total_search)] 



#subset(keyword_search_counts_anl1, Q %in% c('fep', 'lead screw', ''))

#blank1 <- subset(search_log, Q %in% c(''))

#blank2 <- search_log[!complete.cases(search_log$Q),]

#merge(x, df1, all.x=TRUE, by.x="Site.1", by.y="Sites", sort=FALSE)

### creating a function in ddply - may be used later on to create a function
#ddply(sub_search, .(Q), function(x) {
#  x$new.var <- ifelse(x$Status == 'Link', 'My status is link', 'My status is not link')
#  return(x)
#})

#keyword_search_counts <- count(search_codefix_merge, .(Q, sum_link, sum_linkCtg, total_search, codefix_count))

#creating a subset, e.g sessionid = something
#sub_codeFix <- subset(codeFix_log, SessionId %in% c('6e64b44adbec3cf7ad6cb820751159c0','7c6e99c7c2c4ac3edf848d86195d923b'), select = c("SessionId", "Referer", "PRODUCT_CODE"))
