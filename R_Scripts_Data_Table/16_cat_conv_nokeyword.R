### ==============================================================================================
### Initially this analysis was done using Japan's weekly weblog data
### Paul then asked to perform similar analysis using US' monthly weblog data   
###   
### Goal: To analyse the keyword search results before and after optimizations. 
### To measure: calculate click and conversion ratio for first round or keyword mappings 
###             and compare results before and after optimization for keywords 

### Its mix of 3 different analysis: 1_keyword_transition_ratio, 3_keyword_webid_partnumber and
###                                  4_keyword_category  
### 
### Click ratio      =  Number of clicks (sum_link+sum_Ctg)/Total queries(total_searches)
### Conversion ratio = Number of codefix (LinkCtg)/Total queries(total_searches)
###
### Example to run:
###   cmd /k cd /d p:\Data_Analysis\Weblog_Data && RTerm --vanilla --args search_log_20140421_20140427.txt codefix_log_20140421_20140427.txt 20140421 20140427 < P:/R/New_approch_preserve_data/R_Scripts_Data_Table/16_cat_conv_nokeyword.R 
### ==============================================================================================

# List the files and removing everything
rm(list = ls())
gc() #it will free memory

# Trying to capture runtime
begTime <- Sys.time()

# Getting file names and dates from command line arguments
args <- commandArgs(trailingOnly = TRUE)

# search_log_file <- "search_log_20140331_20140406.txt"
# codefix_log_file <- "codefix_log_20140331_20140406.txt"

search_log_file <- args[1]
codefix_log_file <- args[2]

from.date <- args[3]
to.date <- args[4]

#Getting the required package
require(data.table)
require(xlsx)
require(RCurl)
require(bit64)
require(plyr)

#Setting directories

# Uncomment below if using Japan's data
setwd("P:/Data_Analysis/Weblog_Data/jp_data/")
results_dir <- "P:/Data_Analysis/Analysis_Results/jp_results/"

# Uncomment below if using USA data
# setwd("P:/Data_Analysis/Weblog_Data/")
# results_dir <- "P:/Data_Analysis/Analysis_Results/"

### =====================================================================
### Part of Data_Load_Prod mixed with 1_keyword_transition_ratio
### =====================================================================

#Importing data
search_log <- fread(search_log_file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 
codeFix_log <- fread(codefix_log_file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 

#clean up data
search_log[, c("AccessDateTime", "TOKUCD",  "CookieId", "BRD_CODE", "IP", 
               "UA", "LogFileName", "UpdateDate", "V17") := NULL]
codeFix_log[, c("AccessDateTime",  "TOKUCD", "CookieId", "BRD_CODE", 
                "SHUNSAKU", "IP", "UA", "LogFileName", "UpdateDate", "V15") := NULL]

# removing internal acccounts with CUSTCD = "WOSMUS"
# 01/13/14 - Added SEARCH_TYPE == 1 to keep only "keyword search" and not suggestions
search_log <- search_log [Status=='LinkCtg']
search_log <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1)] [, Q := tolower(Q)]

codeFix_log <- codeFix_log[ !CUSTCD=="WOSMUS" | is.na(CUSTCD) ] [, CUSTCD := NULL]

  # Above line is equvalent to below 2 lines. Similar for code fix
  # search_log <- search_log[ SEARCH_TYPE == 1] 
  # search_log <- search_log[ !CUSTCD == "WOSMUS" ] 

# Formatting Link_URL
search_log[, `:=` ( SERIES_CODE = as.character(SERIES_CODE), 
                    Link_URL    = as.character(Link_URL)
                   )]
codeFix_log[, SERIES_CODE := as.character(SERIES_CODE)]

# Exctract (right most) category id from the Link_URL
search_log$last_cat  <- gsub(".*/(\\w+)/$", "\\1", search_log$Link_URL) 

#
# summarize total keyword searches. 
# Since we already filter Status==LinkCtg, both below number will be same
#
search_log[, `:=` (sum_linkCtg = sum(Status=='LinkCtg'), 
                   tot_last_cat = .N),  
            by=last_cat] 

# To get correct unique records, setting key to NULL
setkey(search_log, NULL)
total_keyword_search <- unique(search_log[, c("last_cat", "sum_linkCtg", "tot_last_cat"), with=FALSE])

keyword_search_merge_anl <- search_log

### =====================================================================
### Part of Analysis # 3_keyword_webid_partnumber
### =====================================================================

  ### Summarize CodeFix counts
  total_codefix <- codeFix_log
  
  # removing internal acccounts with CUSTCD = "WOSMUS"
  total_codefix <- total_codefix [!is.na(Referer)]

  # Calculate codefix_count summerizing by SessionId, Referer and Series Code
  setkey(total_codefix, SessionId, Referer, SERIES_CODE)
  total_codefix[, codefix_count := length(unique(PRODUCT_CODE)), by=key(total_codefix)] 
  
  # Keep unique records with the fields that we need
  setkey(total_codefix, NULL)
  total_codefix <- unique(total_codefix[, c("SessionId", "Referer","SERIES_CODE", "codefix_count"), with=FALSE])

  # Keep only records with LinkCtg to save time on merging unnecessary records
#   keyword_search_merge_anl <- search_log [Status=='LinkCtg']

#==========================================================================================
# Get unique values before merge
keyword_search_merge_anl <- keyword_search_merge_anl[, list(LinkCtg=.N), 
                                                     by=list(last_cat, SessionId, Link_URL, sum_linkCtg, tot_last_cat)]

keyword_search_merge_anl <- unique(keyword_search_merge_anl)

# Setkey for faster join
setkey(total_codefix, SessionId)
setkey(keyword_search_merge_anl, SessionId)

# Partial match/merge of `total_codefix` and `keyword_search_merge_anl` - using Link_URL as pattern, and Referer as table
# Also wrap grep result with `mapply` to apply grep function to each element of each argument
search_codefix_merge = total_codefix [ keyword_search_merge_anl, nomatch = 0, allow.cartesian = TRUE ][ 
                                         mapply(grep, Link_URL, Referer)==1 ] 

# Reset the key to get the unique values correctly
search_codefix_merge = setkey(search_codefix_merge, NULL)
search_codefix_merge = unique(search_codefix_merge)

# to keep consistancy with previous analysis
url_parts_merge <- search_codefix_merge

### ===========================================================
### Part of Analysis # 4_keyword_category
### ===========================================================

# url_parts_merge$last_cat  <- gsub(".*/(\\w+)/$", "\\1", url_parts_merge$Link_URL) 

# Summarize based on Link Categories
# Finding coversion rate, total_codefix_linkCtg / total_search
# Sorting the data based on total search (desc), keywords and number of link categories(desc)
# 02122014 - Link_URL added to obtain the URL and last category code
keyword_search_counts_anl <- url_parts_merge[, list(LinkCtg=sum(LinkCtg), 
                                                    #tot_codefix_count = sum(codefix_count, na.rm = T)),
                                                 by=list(last_cat, sum_linkCtg, Link_URL)
                                               ] [, ConvRate := round(LinkCtg/sum_linkCtg, 3) ] [ order(-sum_linkCtg,-ConvRate, last_cat) ]

# Format csv name
analysis_file <- paste0( results_dir, paste("16_cat_conv_nokeyword", from.date, to.date, sep="_" ), ".csv" )

# Exporting results in csv
write.csv(keyword_search_counts_anl, file = analysis_file, na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime
#=========================================================== Test the results =============================================



