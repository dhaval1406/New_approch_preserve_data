# List the files and removing everything
rm(list = ls())
gc() #it will free memory

# Trying to capture runtime
begTime <- Sys.time()

# Getting file names and dates from command line arguments
args <- commandArgs(trailingOnly = TRUE)

search_log_file <- args[1]
detailtab_log_file <- args[2]
codefix_log_file <- args[3]

from.date <- args[4]
to.date <- args[5]

#Getting the required package
require(plyr)
require(data.table)
require(bit64)
#Set working directory
setwd("P:/Data_Analysis/Weblog_Data/")

#Importing data
search_log <- fread(search_log_file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 
codeFix_log <- fread(codefix_log_file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 
detailTab_log <- fread(detailtab_log_file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 

category <- fread('category.csv', header = FALSE, sep = ",", na.strings=c("NA", ''))

#clean up data
search_log[, c("USER_CODE",  "CookieId", "BRD_CODE", "IP", "UA", "LogFileName", "UpdateDate", "V17") := NULL]

codeFix_log[, c("AccessDateTime",  "USER_CODE", "CookieId", "BRD_CODE", 
                "SHUNSAKU", "IP", "UA", "LogFileName", "UpdateDate", "V15") := NULL]

detailTab_log[, c("AccessDateTime",  "USER_CODE", "CookieId", "BRD_CODE", "URL"
                , "IP", "UA", "LogFileName", "UpdateDate", "V14") := NULL]

# first two columns are not used
category[, c(1:2, 13) := NULL]

# Assigning column names, setnames is similar-but faster- as columnname
setnames(category, c("cat_id_1", "cat_name_1","cat_id_2", "cat_name_2","cat_id_3", "cat_name_3","cat_id_4", "cat_name_4","cat_id_5", "cat_name_5"))

# removing blank series codes and conver  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
# 01/13/14 - Added SEARCH_TYPE == 1 to keep only "keyword search" and not suggestions
search_log <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1)] [, Q := tolower(Q)]
codeFix_log <- codeFix_log[!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))] [, CUSTCD := NULL]

# For analysis # 3, non blank Referer is used
#codeFix_log <- subset(codeFix_log, subset = ( !is.na(Referer) & (!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))) ), select = -CUSTCD)
detailTab_log <- detailTab_log[!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))] [, CUSTCD := NULL]
                              
# Formatting Series_Code - fread assigns it to class 'integer64'
search_log$SERIES_CODE = as.character(search_log$SERIES_CODE)
codeFix_log$SERIES_CODE = as.character(codeFix_log$SERIES_CODE)
detailTab_log$SERIES_CODE = as.character(detailTab_log$SERIES_CODE)

# Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
search_log$Q <- gsub("(.+[^s])s$", "\\1", search_log$Q)

### summarize total keyword searches
setkey(search_log, Q)

search_log[, `:=` (sum_link = sum(Status=='Link'), 
                   sum_linkCtg = sum(Status=='LinkCtg'), 
                   total_search = sum (sum(Status=='NotFound'), (Status=='Hit'))),
            by=Q] 

# To get correct unique records, setting key to NULL
setkey(search_log, NULL)
total_keyword_search <- unique(search_log[, c("Q", "sum_link", "sum_linkCtg", "total_search"), with=FALSE])

# total_codefix <- ddply(codeFix_log, .(SessionId, Referer), summarise, 
#                        #codefix_count = sum(Referer != ''), 
#                        #codefix_count = length(unique(c(SessionId)))
#                        codefix_count = length(unique(Referer))
# 					            )
# 
# # for Analysis # 3	
# total_codefix_anl3 <- ddply(codeFix_log, .(SessionId, Referer, SERIES_CODE), summarise, 
#             					 codefix_count = length(unique(PRODUCT_CODE))
#                       )

setkey(detailTab_log, SessionId, SERIES_CODE)

detailTab_log[, tab_count := length(unique(Tab_Name)), by=list(SessionId, SERIES_CODE)] 

# Kadach upar nu kadhi ne niche nu lakhavanu
# total_detailtab <- detailTab_log[, list(tab_count = length(unique(Tab_Name))), by=list(SessionId, SERIES_CODE)] 

# Merge total_keyword_search with search 
# keyword_search_merge <- merge(search_log, total_keyword_search, all.x=TRUE, by="Q", sort=FALSE)
# codefix_merge <- merge(codeFix_log, total_codefix, all.x=TRUE, by=c("SessionId", "Referer"), sort=FALSE)

# Function to format resultant CSV - generated during different analysis
format_name <- function(x){
  time.stamp = format(Sys.time(), "%m%d%Y")
  file.name = paste(x, sprintf("%s.csv", time.stamp), sep="_")
  return(file.name)
}

### Save image of R data for further use
image_file_name <- paste0("P:/Data_Analysis/Processed_R_Datasets/", 
                          paste("Data_Load_Prod", from.date, to.date, sep="_" ), ".RData")

save.image(image_file_name)

runTime <- Sys.time()-begTime 
runTime