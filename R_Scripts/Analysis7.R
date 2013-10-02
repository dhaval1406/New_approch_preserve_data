# This Data analysis uses below data from different servers
# Search_log, codeFix_log  -> web server logs
# SO data - from SQL server's SO table

### Changes compare to previous analysis
# SEARCH_TYPE == 1 for seach_log
# CUSTCD is used to match Customer_Code from SO table
# Logic for from.date

# List the files and removing everything
rm(list = ls())
gc() #it will free memory

# Trying to capture runtime
begTime <- Sys.time()

#Getting the required package
require(plyr)
library(RODBC)
#======================================== web log part ===================================================
#Set working directory
setwd("P:/Data_Analysis/Weblog_Data/")

#Importing data
search_log <- read.delim('search_log_20130715_20130722.txt', header = TRUE, sep = "\t", quote = "", comment.char = "", na.strings=c("NA", ''))
codeFix_log <- read.delim('codefix_log_20130715_20130722.txt', header = TRUE, sep = "\t", quote = "", comment.char = "", na.strings=c("NA", ''))
category <- read.csv('category.csv', header = FALSE, sep = ",", quote = "\"", comment.char = "", na.strings=c("NA", ''))

#clean up data
search_log <- search_log[c("AccessDateTime", "SessionId", "Q", "SEARCH_TYPE", "Status", "Link_URL", "SERIES_CODE", "CUSTCD")]
codeFix_log <- codeFix_log[c("SessionId","PRODUCT_CODE", "SERIES_CODE","Referer", "CUSTCD")]
# first two columns are not used
category <- category[, - c(1:2, 13)]
# Assigning column names
colnames(category) <- c("cat_id_1", "cat_name_1","cat_id_2", "cat_name_2","cat_id_3", "cat_name_3","cat_id_4", "cat_name_4","cat_id_5", "cat_name_5")

# removing blank keywords and convert  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
# keeping results only with SEARCH_TYPE == 1
search_log <- subset(search_log, subset = ( !is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1) ))
search_log$Q <- tolower(search_log$Q)

codeFix_log <- subset(codeFix_log, subset = (!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))))

### summarize total keyword searches - search_log[1:100,]
total_keyword_search <- ddply(search_log, .(Q), summarise, 
                              sum_link = sum(Status=='Link'), 
                              sum_linkCtg = sum(Status=='LinkCtg'), 
                              total_search = sum (sum(Status=='NotFound'), (Status=='Hit'))
)

total_codefix <- ddply(codeFix_log, .(SessionId, Referer, SERIES_CODE), summarise, 
                       codefix_count = length(unique(PRODUCT_CODE))
)

#Keep only records with Link and remove blank CUSTCD
search_log <- subset (search_log, (!is.na(CUSTCD) & (Status=='LinkCtg')))
codeFix_log <- subset(codeFix_log, !is.na(CUSTCD))

# Merge total_keyword_search with search 
keyword_search_merge_anl <- merge(search_log, total_keyword_search, all.x=TRUE, by="Q", sort=FALSE)
codefix_merge <- merge(codeFix_log, total_codefix, all.x=TRUE, by=c("SessionId", "Referer", "SERIES_CODE"), sort=FALSE)

#=========== new url ==============
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

#==================================
# Get unique values before merge
keyword_search_merge_anl <- ddply(url_parts_search, .(Q, SessionId, Link_URL, CUSTCD, sum_link, sum_linkCtg, total_search, X6, X7, X8, X9, X10), summarise,
                                  LinkCtg = length(Q)
                                  )  
#codefix_merge <- unique(codefix_merge[, -4])

#merging keyword_search_merge and codefix_merge frames
search_codefix_merge <- merge(
  keyword_search_merge_anl,
  codefix_merge,all.x=TRUE,
  by.x = c("SessionId", "Link_URL", "CUSTCD"), by.y = c("SessionId", "Referer", "CUSTCD"),
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
#======================================= SO data - ODBC part =================================================
# Connect to SQL servers ODBC connction
misumi_sql <- odbcConnect("misumi_sql", uid="AccessDB", pwd="")

# Format date
from.date = format(Sys.Date()-11, "%m%d%Y")

# Get data from SQL server that are greater or equal to from date
# Also remove samples with Product_Total_Amount > 0.00
so_data <- sqlQuery(misumi_sql, paste("select SO_Date, Customer_Code, Product_Code, Product_Name, Product_Total_Amount, BusinessUnit,",
                                       "Classify_Code, InnerCode, MediaCodeQT from SO", 
                                       "where Product_Total_Amount > 0.00 and SO_Date >=", from.date))

#merging keyword_search_merge and codefix_merge frames
search_codefix_so_merge <- merge(
  url_parts_merge,
  so_data, 
  by.x = c("CUSTCD", "PRODUCT_CODE"), by.y = c("Customer_Code", "Product_Code"),
  sort = FALSE  
)

analysis_7_results <- ddply(search_codefix_so_merge, .(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, SERIES_CODE, Product_Name, 
                                                       BusinessUnit, Classify_Code, InnerCode, MediaCodeQT, total_search, sum_link, sum_linkCtg), 
                            summarise, 
                            LinkCtg = sum(LinkCtg), 
                            sum_product_total_amount = sum(Product_Total_Amount, na.rm = T), 
                            total_codefix = sum(codefix_count, na.rm = T)
                           )

# Sorting the data based on product total ammount(desc), total search (desc), keywords and number of link categories(desc)
analysis_7_results <- analysis_7_results[order( -analysis_7_results$sum_product_total_amount 
                                                -analysis_7_results$total_search, 
                                                 analysis_7_results$Q, 
                                                 -analysis_7_results$LinkCtg),  ]

write.csv(analysis_7_results, file = "P:/Data_Analysis/Analysis_Results/keyword_analysis_7.csv", na = '', row.names = FALSE)