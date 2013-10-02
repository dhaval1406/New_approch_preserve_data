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
  
  # Fix 07232013 - Adding a coulumn X10 with blank values in case if category 5 is not present.
  #                Otherwise below ddply statement would fail
  if (is.na(names(url_parts_search)[21])) url_parts_search$X10 = NA
#==========================================================================================

# Get unique values before merge
keyword_search_merge_anl <- ddply(url_parts_search, .(Q, SessionId, Link_URL, sum_link, sum_linkCtg, total_search, X6, X7, X8, X9, X10), summarise,
                                  LinkCtg = length(Q)
                                  )  
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
                                   total_codefix_linkCtg = sum(codefix_count, na.rm = T)
                                  )

# Sorting the data based on total search (desc), keywords and number of link categories(desc)
keyword_search_counts_anl <- keyword_search_counts_anl[order(-keyword_search_counts_anl$total_search, 
                                                              keyword_search_counts_anl$Q, 
                                                              -keyword_search_counts_anl$LinkCtg),  ]
# Format csv name
analysis_file = format_name("keyword_analysis_3");

# Exporting results in csv
write.csv(keyword_search_counts_anl, file = analysis_file, na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime

# save image of R data for further use in Analysis #4 and #5
save.image("P:/Data_Analysis/Processed_R_Datasets/Analysis_3.RData")
# for future saving - less data
#save(search_log, url_parts_merge, file = "Analysis_3.RData")
#=========================================================== Test the results =============================================
