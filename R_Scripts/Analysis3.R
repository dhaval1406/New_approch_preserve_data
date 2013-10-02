# List the files and removing everything
ls ()
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(plyr)

#Set working directory
setwd("P:/R/New_approch_preserve_data/")
load("P:/Data_Analysis/Processed_R_Datasets/Data_Load_Prod.RData")

# Trying to capture runtime
begTime <- Sys.time()

#============================================== Main Algorithem =======================================

# removing internal acccounts with CUSTCD = "WOSMUS"
codeFix_log <- subset(codeFix_log, subset = ( !is.na(Referer) ))

total_codefix <- ddply(codeFix_log, .(SessionId, Referer, SERIES_CODE), summarise, 
                              codefix_count = length(unique(PRODUCT_CODE))
                      )

#Keep only records with LinkCtg to save time on merging unnecessary records
search_log <- subset (search_log, Status=='LinkCtg')

# Merge total_keyword_search with search 
keyword_search_merge_anl <- merge(search_log, total_keyword_search, all.x=TRUE, by="Q", sort=FALSE)
codefix_merge <- merge(codeFix_log, total_codefix, all.x=TRUE, by=c("SessionId", "Referer", "SERIES_CODE"), sort=FALSE)

#======================================= new url ==========================================
  #Coverting URLs to character to avoid any error messages
  keyword_search_merge_anl$Link_URL <- as.character(keyword_search_merge_anl$Link_URL)
  # Split URL by / using perl reg ex - it will return a list within list
  url_parts_search <- lapply(keyword_search_merge_anl$Link_URL, strsplit, "/", perl=TRUE)
  # Enhanced version of the regex
  #url_parts <- lapply(search_codefix_merge$Link_URL, strsplit, "http://[^/]+/([^/]+)/[^/]+/?", perl=TRUE)
  #url_parts1 <- data.frame(do.call(rbind.fill.matrix, lapply(url_parts, function(x) do.call(rbind, x))))
  
  # create dateframe from the list of list, since the url part are not same - so it will result in differnt number of columns
  # So, using rbin.fill.matrix to fill with missing values and avoid any error messages. 
  # Join the resultant data frame with original one
  url_parts_search <- data.frame(keyword_search_merge_anl, do.call(rbind.fill.matrix, lapply(url_parts_search, function(x) do.call(rbind, x))))
  
#==========================================================================================

# Get unique values before merge
keyword_search_merge_anl <- ddply(url_parts_search, .(Q, SessionId, Link_URL, sum_link, sum_linkCtg, total_search, X6, X7, X8, X9, X10), summarise,
                                  LinkCtg = length(Q), 
                                  .progress = "text")  
codefix_merge <- unique(codefix_merge[, -4])


#merging keyword_search_merge and codefix_merge frames
search_codefix_merge <- merge(
                            keyword_search_merge_anl,
                            codefix_merge,all.x=TRUE,
                            by.x = c("SessionId", "Link_URL"), by.y = c("SessionId", "Referer"),
                            sort = FALSE                            
                         ) 

# Taking substring to match with each category ids, 3, 5, 7, 9, 11
search_codefix_merge <- transform(search_codefix_merge, 
              cat1_id = substr(search_codefix_merge$X6, 1, 3),
              cat2_id = substr(search_codefix_merge$X7, 1, 5),
              cat3_id = substr(search_codefix_merge$X8, 1, 7),
              cat4_id = substr(search_codefix_merge$X9, 1, 9),
              cat5_id = substr(search_codefix_merge$X10, 1, 11)) 

url_parts_merge <- merge(
  search_codefix_merge,
  category,
  by.x = c("cat1_id", "cat2_id", "cat3_id", "cat4_id", "cat5_id"), by.y = c("cat_id_1", "cat_id_2", "cat_id_3", "cat_id_4", "cat_id_5"),
  sort = FALSE  
)

keyword_search_counts_anl <- ddply(url_parts_merge, .(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, SERIES_CODE, total_search, sum_link, sum_linkCtg), summarise, 
                                   LinkCtg = sum(LinkCtg), 
                                   total_codefix_linkCtg = sum(codefix_count, na.rm = T),
                                   .progress = "text"
                                  )

# Sorting the data based on total search (desc), keywords and number of link categories(desc)
keyword_search_counts_anl <- keyword_search_counts_anl[order(-keyword_search_counts_anl$total_search, 
                                                              keyword_search_counts_anl$Q, 
                                                              -keyword_search_counts_anl$LinkCtg),  ]


write.csv(keyword_search_counts_anl, file = "3_keyword_webid_partnumber.csv", na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime

# save image of R data for further use
save.image("Analysis_3.RData")
# for future saving - less data
#save(search_log, url_parts_merge, file = "Analysis_3.RData")
#=========================================================== Test the results =============================================

# subset(keyword_search_counts_anl1, Q %in% c('fep', 'lead screw', ''))
# 
# blank1 <- subset(search_log, Q %in% c(''))
# 
# blank2 <- search_log[!complete.cases(search_log$Q),]
# 
# #merge(x, df1, all.x=TRUE, by.x="Site.1", by.y="Sites", sort=FALSE)
# 
# ### creating a function in ddply - may be used later on to create a function
# #ddply(sub_search, .(Q), function(x) {
# #  x$new.var <- ifelse(x$Status == 'Link', 'My status is link', 'My status is not link')
# #  return(x)
# #})
# 
# #keyword_search_counts <- count(search_codefix_merge, .(Q, sum_link, sum_linkCtg, total_search, codefix_count))
# 
# 
# #creating a subset, e.g sessionid = something
# #sub_codeFix <- subset(codeFix_log, SessionId %in% c('6e64b44adbec3cf7ad6cb820751159c0','7c6e99c7c2c4ac3edf848d86195d923b'), select = c("SessionId", "Referer", "PRODUCT_CODE"))
# 
# #search_codefix_merge[pmatch(search_codefix_merge$Link_URL, search_codefix_merge$Referer), ]
# 
# #     Experiment for getting a partial match on Link_URL and Referer
# #     x <- pmatch(search_codefix_merge$Link_URL, search_codefix_merge$Referer)
# #     search_codefix_merge <- search_codefix_merge[x[!is.na(x)], ]
# #     x1 <- pmatch(search_codefix_merge$Link_URL, search_codefix_merge$Referer, dup = TRUE)
# #     search_codefix_merge <- search_codefix_merge[x[!is.na(x)], ]
# #     pmatch(search_codefix_merge[17,("Link_URL")], search_codefix_merge[16, c("Referer")])
# #     str_detect(search_codefix_merge[17,("Link_URL")], search_codefix_merge[c("Referer")])
# #     grepl
# #str_detect("http://us.misumi-ec.com/vona2/press/P0200000000/P0202000000/P0202070000/P0202070500/", "http://us.misumi-ec.com/vona2/press/P0200000000/P0202000000/")
# 
# # May use in future as it reduces the number of variables
# # category_1 <- unique(category[, c("cat_id_1", "cat_name_1")]);
# # category_2 <- unique(category[, c("cat_id_2", "cat_name_2")]);
# # category_3 <- unique(category[, c("cat_id_3", "cat_name_3")]);
# # category_4 <- unique(category[, c("cat_id_4", "cat_name_4")]);
# # category_5 <- unique(category[, c("cat_id_5", "cat_name_5")]);
# 
# 
# keyword_search_merge_anl <- subset(keyword_search_merge_anl, Q == 'shaft')
# 
# library(sqldf)
# 
# search_log_subset <- subset(search_log, Q == 'straight punch')
# 
# codeFix_subset <- sqldf ("select * from search_log_subset s, codeFix_log t where (s.SessionId == t.SessionId and s.Link_URL == t.Referer )")
