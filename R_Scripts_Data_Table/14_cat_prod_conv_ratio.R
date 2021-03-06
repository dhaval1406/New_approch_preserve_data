### ==============================================================================================
### Goal: To analyse the keyword search results before and after optimizations. 
### To measure: calculate click and conversion ratio for first round or keyword mappings 
###             and compare results before and after optimization for keywords 
###
### Its mix of 3 different analysis: 1_keyword_transition_ratio, 3_keyword_webid_partnumber and
###                                  4_keyword_category  
### 
### Click ratio =  Number of clicks (sum_link+sum_Ctg)/Total queries(total_searches)
### Conversion ratio = Number of codefix (LinkCtg)/Total queries(total_searches)
###             
### ==============================================================================================

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
require(data.table)
require(xlsx)
require(bit64)
require(plyr)

#Setting directories
setwd("P:/Data_Analysis/Weblog_Data/")
results_dir <- "P:/Data_Analysis/Analysis_Results/"
# results_dir_q <- "Q:/marketingshared/Web Reports/Data_Analysis/click_conversion_ratio/"

### =====================================================================
### Part of Data_Load_Prod mixed with 1_keyword_transition_ratio
### =====================================================================
# Importing data
search_log <- fread(search_log_file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 
codeFix_log <- fread(codefix_log_file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 

category <- fread('category.csv', header = FALSE, sep = ",", na.strings=c("NA", ''))

# Get rid of MIke's crawler ip
search_log <- search_log[IP != "69.47.201.40"]
codeFix_log <- codeFix_log[IP != "69.47.201.40"]

#clean up data
search_log[, c("USER_CODE",  "CookieId", "BRD_CODE", "IP", "UA", "LogFileName", "UpdateDate", "V17") := NULL]

codeFix_log[, c("AccessDateTime",  "USER_CODE", "CookieId", "BRD_CODE", 
                "SHUNSAKU", "IP", "UA", "LogFileName", "UpdateDate", "V15") := NULL]

# first two columns are not used
category[, c(1:2, 13) := NULL]

# Assigning column names, setnames is similar-but faster- as columnname
setnames(category, c("cat_id_1", "cat_name_1","cat_id_2", "cat_name_2","cat_id_3", "cat_name_3","cat_id_4", "cat_name_4","cat_id_5", "cat_name_5"))

# removing blank series codes and conver  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
# 01/13/14 - Added SEARCH_TYPE == 1 to keep only "keyword search" and not suggestions
search_log <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1)] [, Q := tolower(Q)]
codeFix_log <- codeFix_log[!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))] [, CUSTCD := NULL]

# Formatting Series_Code - fread assigns it to class 'integer64'
search_log$SERIES_CODE = as.character(search_log$SERIES_CODE)
codeFix_log$SERIES_CODE = as.character(codeFix_log$SERIES_CODE)

# Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
search_log$Q <- gsub("(.+[^s])s$", "\\1", search_log$Q)

### summarize total keyword searches
setkey(search_log, Q)

search_log[, `:=` (sum_link = sum(Status=='Link'), 
                   sum_linkCtg = sum(Status=='LinkCtg'), 
                   total_search = sum (sum(Status=='NotFound'), (Status=='Hit')),
                   total_clicks = sum (sum(Status=='Link'), (Status=='LinkCtg')),
                   click_ratio = round ( (sum (sum(Status=='Link'), 
                                               (Status=='LinkCtg')) ) / 
                                         ( sum (sum(Status=='NotFound'),
                                                (Status=='Hit'))), 3 
                                       ) 
                   ),  
            by=Q] 

# To get correct unique records, setting key to NULL
setkey(search_log, NULL)
total_keyword_search <- unique(search_log[, c("Q", "sum_link", "sum_linkCtg", "total_search", "total_clicks", "click_ratio"), with=FALSE])

### =====================================================================
### Part of Analysis # 3_keyword_webid_partnumber
### =====================================================================
### Summarize CodeFix counts
  total_codefix <- codeFix_log
  
  # removing internal acccounts with CUSTCD = "WOSMUS"
  total_codefix <- total_codefix [!is.na(Referer)]

  # Set the key for faster operations
   setkey(total_codefix, SessionId, Referer, SERIES_CODE)
  
  # Calculate codefix_count summerizing by SessionId, Referer and Series Code
  total_codefix[, codefix_count := length(unique(PRODUCT_CODE)), by=list(SessionId, Referer, SERIES_CODE)] 
  
  # Keep unique records with the fields that we need
  setkey(total_codefix, NULL)
  total_codefix <- unique(total_codefix[, c("SessionId", "Referer","SERIES_CODE", "codefix_count"), with=FALSE])

  # Keep only link to count codefix for links
  search_log_link <- search_log [Status=='Link']

  # Keep only records with LinkCtg to save time on merging unnecessary records
  search_log <- search_log [Status=='LinkCtg']

  # Assign to keyword_search_merge_anl to make it consistent
  keyword_search_merge_anl <- search_log

#======================================= new url ==========================================
  # Coverting URLs to character to avoid any error messages
  keyword_search_merge_anl$Link_URL <- as.character(keyword_search_merge_anl$Link_URL)

  # Split URL by / using perl reg ex - it will return a list within list
  url_parts_search <- lapply(keyword_search_merge_anl$Link_URL, strsplit, "/", perl=TRUE)
  
  # create dateframe from the list of list, since the url part are not same - so it will result in differnt number of columns
  # So, using rbin.fill.matrix to fill with missing values and avoid any error messages. 
  # Join the resultant data frame with original one
  url_parts_search <- data.frame(keyword_search_merge_anl, do.call(rbind.fill.matrix, lapply(url_parts_search, function(x) do.call(rbind, x))))

  # Fix 07232013 - Adding a coulumn X10 with blank values in case if category 5 is not present.
  #                Otherwise below ddply statement would fail
  if (is.na(names(url_parts_search)[21])) url_parts_search$X10 = NA

  # Convert data.frame to data.table
  url_parts_search <- as.data.table(url_parts_search)  
#==========================================================================================
# Get unique values before merge
keyword_search_merge_anl <- url_parts_search[, list(LinkCtg=.N), 
                                             by=list(Q, SessionId, Link_URL, sum_link, sum_linkCtg, total_search, X6, X7, X8, X9, X10)]

# Setkey for faster join
setkey(total_codefix, SessionId)
setkey(keyword_search_merge_anl, SessionId)

# Partial merge of `total_codefix` and `keyword_search_merge_anl` - using Link_URL as pattern, and Referer as table
# Also wrap grep result with `mapply` to apply grep function to each element of each argument
# Also transforming/taking substring to match with each category ids, 3, 5, 7, 9, 11
search_codefix_merge = total_codefix [ keyword_search_merge_anl, nomatch = 0, allow.cartesian = TRUE ]  [ 
                                             
                                         mapply(grep, Link_URL, Referer)==1 ]  [,
                                         
                                         `:=` (cat_id_1 = as.factor(substr(X6, 1, 3)),
                                               cat_id_2 = as.factor(substr(X7, 1, 5)),
                                               cat_id_3 = as.factor(substr(X8, 1, 7)),
                                               cat_id_4 = as.factor(substr(X9, 1, 9)),
                                               cat_id_5 = as.factor(substr(X10, 1, 11)))]

# Reset the key to get the unique values correctly
search_codefix_merge = setkey(search_codefix_merge, NULL)
search_codefix_merge = unique(search_codefix_merge)

# Converting it back to data.frame to use merge in a traditional way
# Since merge for data.table sucks
  search_codefix_merge <- as.data.frame(search_codefix_merge)
  category <- as.data.frame(category)
  
  url_parts_merge <- merge(
    search_codefix_merge,
    category,
    by = c("cat_id_1", "cat_id_2", "cat_id_3", "cat_id_4", "cat_id_5"),
    sort = FALSE
  )
  
  # Converting data.frame back to data.table to harness the power of data.table
  url_parts_merge <- as.data.table(url_parts_merge)

### ===========================================================
### Count number codefix for Status==Link
### ===========================================================
  total_codefix_link <- codeFix_log

  # Set the key for faster operations
   setkey(total_codefix_link, SessionId, SERIES_CODE)
  
  # Calculate codefix_count_link summerizing by SessionId, Referer and Series Code
  total_codefix_link[, codefix_count_link := length(PRODUCT_CODE), by=list(SessionId, SERIES_CODE)] 
  
  # Keep unique records with the fields that we need
  setkey(total_codefix_link, NULL)
  total_codefix_link <- unique(total_codefix_link[, c("SessionId", "SERIES_CODE", "codefix_count_link"), with=FALSE])

  # Setting Key for faster merge  
  setkey(search_log_link, SessionId, SERIES_CODE)
  setkey(total_codefix_link, SessionId, SERIES_CODE)


  search_codefix_merge_link = total_codefix_link [ search_log_link, nomatch = 0, allow.cartesian = TRUE ]

  # 02052014 - total_search added
  search_codefix_merge_link_sum <- search_codefix_merge_link[, list(codefix_link = .N), by=list(Q, total_search)]

### ===========================================================
### Part of Analysis # 4_keyword_category
### ===========================================================
# Summarize based on Link Categories
# Finding coversion rate, total_codefix_linkCtg / total_search
# Sorting the data based on total search (desc), keywords and number of link categories(desc)
# 02122014 - Link_URL added to obtain the URL and last category code
  keyword_search_counts_anl <- url_parts_merge[, list(LinkCtg=sum(LinkCtg), 
                                                      total_codefix_linkCtg = sum(codefix_count, na.rm = T)),
                                                 by=list(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, total_search, sum_link, sum_linkCtg, Link_URL)
                                               
                                               ] [, ConvRate := round(total_codefix_linkCtg/total_search, 3) ] [ order(-total_search, Q, -LinkCtg) ]

# Merge to with codefix_link results - 02052014 - changed all.x to all, and total_search added in by
  keyword_search_counts_anl <- merge(keyword_search_counts_anl,
                                     search_codefix_merge_link_sum,
                                     by=c("Q","total_search"),
                                     all =TRUE,
                                     sort = FALSE)

### ===========================================================
### Calculate total conversion by category and product
### ===========================================================
# Setkey for faster join
setkey(total_keyword_search, Q)
setkey(keyword_search_counts_anl, Q)

# Merge to get the match
click_ratio_cat_data <- total_keyword_search [order(-total_search, -total_clicks, Q)]
conv_ratio_cat_data  <- keyword_search_counts_anl  [order(-total_search, Q)]

# Keeping unique records
click_ratio_cat_data <- unique(click_ratio_cat_data)
conv_ratio_cat_data  <- unique(conv_ratio_cat_data)

# Summarize conversion rate and total_codfix 
# 02062014 - changed logic for total_ConvRate. Included codefix_link in the count as well
conv_ratio_cat_data_sum <- conv_ratio_cat_data[, list(total_ConvRate = round( (sum (sum (total_codefix_linkCtg, na.rm = T), 
                                                                                    codefix_link, 
                                                                                    na.rm = T)) / total_search, 3), 
                                                      total_codeFix_cat = sum (total_codefix_linkCtg, na.rm = T)), 
                                                 by = list(Q, total_search, codefix_link)]

# Keeping unique records and adding row values
conv_ratio_cat_data_sum  <- unique(conv_ratio_cat_data_sum)
conv_ratio_cat_data_sum <- conv_ratio_cat_data_sum[,list( total_search      = sum(total_search,      na.rm=T), 
                                                          codefix_link      = sum(codefix_link,      na.rm=T), 
                                                          total_codeFix_cat = sum(total_codeFix_cat, na.rm=T)
                                                         )]


# Calculate convertion by both Category and Product
conv_ratio_cat_data_sum <- conv_ratio_cat_data_sum[, `:=` ( cat_conversion = round(total_codeFix_cat/total_search, 3), 
                                                            prd_conversion = round(codefix_link/total_search, 3)
                                                           )]

# Keeping only records that has valid Link_URL and so as new_url
# conv_ratio_cat_data <- conv_ratio_cat_data[!is.na(conv_ratio_cat_data$Link_URL)]
# conv_ratio_cat_data$Link_URL <- NULL

#=================================================================== 
### Format csv names
#===================================================================
# for (dir_name in c(results_dir, results_dir_q)){

#   click_ratio_file      <- paste0( dir_name, paste("click_ratio", from.date, to.date, sep="_" ), ".csv" )
#   conv_ratio_file       <- paste0( dir_name, paste("conv_ratio", from.date, to.date, sep="_" ), ".csv" )
#   total_conv_ratio_file <- paste0( dir_name, paste("total_conv_ratio", from.date, to.date, sep="_" ), ".csv" )
  
  total_conv_ratio_file <- paste0( results_dir, paste("14_cat_prod_conv_ratio", from.date, to.date, sep="_" ), ".csv" )
  
  ### Exports both results to CSVs
  #===================================================================
#   write.csv(click_ratio_cat_data, file = click_ratio_file, na = "0", row.names = FALSE)
#   write.csv(conv_ratio_cat_data, file = conv_ratio_file, na = '', row.names = FALSE)
  write.csv(conv_ratio_cat_data_sum, file = total_conv_ratio_file, na = "0", row.names = FALSE)
# }  


runTime <- Sys.time()-begTime 
runTime

#=========================================================== Test the results =============================================
# cmd /k cd /d p:\Data_Analysis\Weblog_Data && RTerm --vanilla --args search_log_20130801_20130901.txt none codefix_log_20130801_20130901.txt 20130801 20130901 < P:/R/New_approch_preserve_data/R_Scripts_Data_Table/14_codefix_conv_ratio.R 
# cmd /k cd /d p:\Data_Analysis\Weblog_Data && RTerm --vanilla --args search_log_20130901_20131001.txt none codefix_log_20130901_20131001.txt 20130901 20131001 < P:/R/New_approch_preserve_data/R_Scripts_Data_Table/14_codefix_conv_ratio.R 
# cmd /k cd /d p:\Data_Analysis\Weblog_Data && RTerm --vanilla --args search_log_20131001_20131101.txt none codefix_log_20131001_20131101.txt 20131001 20131101 < P:/R/New_approch_preserve_data/R_Scripts_Data_Table/14_codefix_conv_ratio.R 
# cmd /k cd /d p:\Data_Analysis\Weblog_Data && RTerm --vanilla --args search_log_20131101_20131201.txt none codefix_log_20131101_20131201.txt 20131101 20131201 < P:/R/New_approch_preserve_data/R_Scripts_Data_Table/14_codefix_conv_ratio.R 
# cmd /k cd /d p:\Data_Analysis\Weblog_Data && RTerm --vanilla --args search_log_20131201_20140101.txt none codefix_log_20131201_20140101.txt 20131201 20140101 < P:/R/New_approch_preserve_data/R_Scripts_Data_Table/14_codefix_conv_ratio.R 
# cmd /k cd /d p:\Data_Analysis\Weblog_Data && RTerm --vanilla --args search_log_20140101_20140131.txt none codefix_log_20140101_20140131.txt 20140101 20140131 < P:/R/New_approch_preserve_data/R_Scripts_Data_Table/14_codefix_conv_ratio.R 
# cmd /k cd /d p:\Data_Analysis\Weblog_Data && RTerm --vanilla --args search_log_20140201_20140228.txt none codefix_log_20140201_20140228.txt 20140201 20140228 < P:/R/New_approch_preserve_data/R_Scripts_Data_Table/14_codefix_conv_ratio.R 
# cmd /k cd /d p:\Data_Analysis\Weblog_Data && RTerm --vanilla --args search_log_20140301_20140331.txt none codefix_log_20140301_20140331.txt 20140301 20140331 < P:/R/New_approch_preserve_data/R_Scripts_Data_Table/14_codefix_conv_ratio.R 