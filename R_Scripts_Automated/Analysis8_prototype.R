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
  #url_parts <- lapply(search_codefix_merge$Link_URL, strsplit, "[^/]+/([^/]+)/[^/]+/?", perl=TRUE)
  #url_parts1 <- data.frame(do.call(rbind.fill.matrix, lapply(url_parts, function(x) do.call(rbind, x))))
  
  # create dateframe from the list of list, since the url part are not same - so it will result in differnt number of columns
  # So, using rbin.fill.matrix to fill with missing values and avoid any error messages. 
  # Join the resultant data frame with original one
  url_parts_search <- data.frame(keyword_search_merge_anl, do.call(rbind.fill.matrix, lapply(url_parts_search, function(x) do.call(rbind, x))))
  
  # Fix 07232013 - Adding a coulumn X10 with blank values in case if category 5 is not present.
  #                Otherwise below ddply statement would fail
  if (is.na(names(url_parts_search)[21])) url_parts_search$X10 = NA

#=================================== Partial Match =======================================================
# Using data.table
library(data.table)

# Get unique values before merge
url_parts_search_dt = data.table(url_parts_search)
codefix_merge_dt = data.table(codefix_merge, key = 'SessionId') [, PRODUCT_CODE:=NULL]

keyword_search_merge_anl_dt = unique( url_parts_search_dt[, LinkCtg := .N, 
                                                    by=list(Q, SessionId, Link_URL, sum_link, sum_linkCtg, total_search, X6, X7, X8, X9, X10)] [,
                                                                                                                                                
                                                    list(Q, SessionId, Link_URL, sum_link, sum_linkCtg, total_search, X6, X7, X8, X9, X10, LinkCtg)]
                                    )

# Setkey for faster join
setkey(keyword_search_merge_anl_dt, SessionId)

# Partial merge of `codefix_merge_dt` and `keyword_search_merge_anl_dt` - using Link_URL as pattern, and Referer as table
# Also wrap grep result with `mapply` to apply grep function to each element of each argument
# Also transforming/taking substring to match with each category ids, 3, 5, 7, 9, 11
search_codefix_merge_dt = codefix_merge_dt [ keyword_search_merge_anl_dt, nomatch = 0, allow.cartesian=TRUE ]  [ 
                                             
                                             mapply(grep, Link_URL, Referer)==1 ]  [,
                                             
                                             `:=` (cat_id_1 = as.factor(substr(X6, 1, 3)),
                                                   cat_id_2 = as.factor(substr(X7, 1, 5)),
                                                   cat_id_3 = as.factor(substr(X8, 1, 7)),
                                                   cat_id_4 = as.factor(substr(X9, 1, 9)),
                                                   cat_id_5 = as.factor(substr(X10, 1, 11)))]

# Reset the key to get the unique values correctly
search_codefix_merge_dt = setkey(search_codefix_merge_dt, NULL)
search_codefix_merge_dt = unique(search_codefix_merge_dt)

# Converting it back to data.frame to use merge in a traditional way
# Since merge for data.table sucks
search_codefix_merge_df <- data.frame(search_codefix_merge_dt)

url_parts_merge = merge(
  search_codefix_merge_df,
  category,
  by = c("cat_id_1", "cat_id_2", "cat_id_3", "cat_id_4", "cat_id_5"),
  sort = FALSE
)

# Converting data.frame back to data.table to harness the power of data.table
url_parts_merge_dt = data.table(url_parts_merge)

# Compound query: Summarizing based on `by` variable
#                 Keeping only selected varialbes
#                 Sorting based on total search (desc), keywords and number of link categories(desc)
keyword_search_counts_anl_dt = url_parts_merge_dt[, `:=` (LinkCtg = sum(LinkCtg), total_codefix_linkCtg = sum(codefix_count, na.rm = T)), 
                                                            by =list(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, 
                                                                total_search, sum_link, sum_linkCtg)] [,
                                                   
                                                            list(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, 
                                                                 total_search, sum_link, sum_linkCtg, LinkCtg, total_codefix_linkCtg, Link_URL)] [
                                                            
                                                            order(-total_search, Q, -LinkCtg)  ]
                                

# Reset the key to get the correct value for unique records
keyword_search_counts_anl_dt = unique(setkey(keyword_search_counts_anl_dt, NULL))


# Keeping only columns that are used in the search prototype
keyword_search_counts_anl_dt = keyword_search_counts_anl_dt[, list(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, Link_URL)]

# Getting only category id, that is last part of the Link_URL. 
# Similar regex function is used in Analysis 4 & 5.
# Function to take last url part and selecting second column for just match value
URL_parts <- function(x) {
  m <- regexec(".*/(\\w+)/$", x)
  do.call(rbind, lapply(regmatches(x, m), `[`, c(2L)))
}

category_ids <- URL_parts(as.character(keyword_search_counts_anl_dt$Link_URL))
colnames(category_ids) <- c("Category_Id")

# Append category ids back to original tabl
keyword_search_counts_anl_dt <- data.table(keyword_search_counts_anl_dt, category_ids)

# Adding a sort_order field to preserve the order in the webpage query
keyword_search_counts_anl_dt[, sort_order:=(1:nrow(keyword_search_counts_anl_dt))]

# Format csv name
analysis_file = format_name("8_keyword_webid_partial");

# Exporting results in csv
write.csv(keyword_search_counts_anl_dt, file = analysis_file, na = '', row.names = FALSE)

################################################ ODBC connection to export into MySQL ################################################

  library(RODBC)
  
  misumi_mysql <- odbcConnect("web_log", uid="", pwd="")
  
  # Open an ODBC connection
  odbcGetInfo(misumi_mysql)
  
  # Explort results to MySQL table `keyword_url`
  sqlSave(misumi_mysql, keyword_search_counts_anl_dt, tablename='keyword_url', safer=TRUE, append=TRUE, colnames=FALSE, rownames=FALSE)

######################################################################################################################################

runTime <- Sys.time()-begTime 
runTime

#=========================================================== Test the results =============================================
# Will be useful to create dictionary from keywords
# write.table(unique(keyword_search_counts_anl_dt[,Q]), file='test.txt', sep = " ", eol=" ", qmethod="escape", col.names=FALSE, row.names=FALSE, quote=FALSE)
