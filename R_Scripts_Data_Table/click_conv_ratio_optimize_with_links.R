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
results_dir <- "P:/Data_Analysis/Analysis_Results/shiny_data/"
results_dir_q <- "Q:/marketingshared/Web Reports/Data_Analysis/click_conversion_ratio/"

# Using keyword_category_121113.xls 
# keyword.cat.data <- as.data.table(read.xlsx("keyword_category_022714.xls", sheetIndex=1, stringsAsFactors= F, encoding='UTF-8'))
keyword.cat.data <- as.data.table(read.xlsx("optimized_keywords_05092014.xlsx", sheetIndex=1, stringsAsFactors= F, encoding='UTF-8'))

### =====================================================================
### Part of Data_Load_Prod mixed with 1_keyword_transition_ratio
### =====================================================================
# Importing data

# search_log <- as.data.table(read.delim(search_log_file, header = TRUE, sep = "\t", , na.strings=c("NA", ''), encoding="UTF-8"))
# codeFix_log <- as.data.table(read.delim(codefix_log_file, header = TRUE, sep = "\t", , na.strings=c("NA", ''), encoding="UTF-8"))

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

### ============================================================
###   Create link/url  for conversion rate by categoreis
####  Using last category Id from the Link_URL
### ===========================================================
keyword_search_counts_anl$new_url  <- paste0 ("<a href=", keyword_search_counts_anl$Link_URL, 
                                              " target=_blank>", gsub(".*/(\\w+)/$", "\\1", keyword_search_counts_anl$Link_URL), 
                                              "</a>")  
### ===========================================================
### Keep only keywords that match keyword.cat.data
### ===========================================================
# Setkey for faster join
setkey(total_keyword_search, Q)
setkey(keyword_search_counts_anl, Q)
setkey(keyword.cat.data, keyword)

# Merge to get the match
click_ratio_cat_data <- total_keyword_search [ keyword.cat.data[, keyword], nomatch = 0 ] [order(-total_search, -total_clicks, Q)]
conv_ratio_cat_data  <- keyword_search_counts_anl [ keyword.cat.data[, keyword], nomatch = 0 ] [order(-total_search, Q)]

# Keeping unique records
click_ratio_cat_data <- unique(click_ratio_cat_data)
conv_ratio_cat_data  <- unique(conv_ratio_cat_data)

# Summarize conversion rate and total_codfix 
# 02062014 - changed logic for total_ConvRate. Included codefix_link in the count as well
conv_ratio_cat_data_sum <- conv_ratio_cat_data[, list(total_ConvRate = round( (sum (sum (total_codefix_linkCtg, na.rm = T), 
                                                                                    codefix_link, 
                                                                                    na.rm = T)) / total_search, 3), 
                                                      total_codeFix_sum = sum (total_codefix_linkCtg, na.rm = T)), 
                                                 by = list(Q, total_search, codefix_link)]
# Keeping unique records
conv_ratio_cat_data_sum  <- unique(conv_ratio_cat_data_sum)

# Keeping only records that has valid Link_URL and so as new_url
conv_ratio_cat_data <- conv_ratio_cat_data[!is.na(conv_ratio_cat_data$Link_URL)]
conv_ratio_cat_data$Link_URL <- NULL

#=================================================================== 
### Non optimized keywords only - little opposite from the above
#===================================================================
# Merge to get the match
click_ratio_cat_data_non <- total_keyword_search [ !keyword.cat.data[, keyword]] [order(-total_search, -total_clicks, Q)]
conv_ratio_cat_data_non  <- keyword_search_counts_anl [ !keyword.cat.data[, keyword]] [order(-total_search, Q)]

# Keeping unique records
click_ratio_cat_data_non <- unique(click_ratio_cat_data_non)
conv_ratio_cat_data_non  <- unique(conv_ratio_cat_data_non)

# Summarize conversion rate and total_codfix
# 02062014 - changed logic for total_ConvRate. Included codefix_link in the count as well
conv_ratio_cat_data_sum_non <- conv_ratio_cat_data_non[, list(total_ConvRate = round( (sum (sum (total_codefix_linkCtg, na.rm = T), 
                                                                                    codefix_link, 
                                                                                    na.rm = T)) / total_search, 3), 
                                                              total_codeFix_sum = sum (total_codefix_linkCtg, na.rm = T)), 
                                                       by = list(Q, total_search, codefix_link)]
# Keeping unique records
conv_ratio_cat_data_sum_non <- unique(conv_ratio_cat_data_sum_non)

# Keeping only records that has valid Link_URL and so as new_url
conv_ratio_cat_data_non <- conv_ratio_cat_data_non[!is.na(conv_ratio_cat_data_non$Link_URL)]
conv_ratio_cat_data_non$Link_URL <- NULL
#=================================================================== 
### Format csv names
#===================================================================
for (dir_name in c(results_dir, results_dir_q)){

  click_ratio_file      <- paste0( dir_name, paste("click_ratio", from.date, to.date, sep="_" ), ".csv" )
  conv_ratio_file       <- paste0( dir_name, paste("conv_ratio", from.date, to.date, sep="_" ), ".csv" )
  total_conv_ratio_file <- paste0( dir_name, paste("total_conv_ratio", from.date, to.date, sep="_" ), ".csv" )
  
  click_ratio_file_non      <- paste0( dir_name, paste("non_click_ratio", from.date, to.date, sep="_" ), ".csv" )
  conv_ratio_file_non       <- paste0( dir_name, paste("non_conv_ratio", from.date, to.date, sep="_" ), ".csv" )
  total_conv_ratio_file_non <- paste0( dir_name, paste("non_total_conv_ratio", from.date, to.date, sep="_" ), ".csv" )
  
  #=================================================================== 
  ### Exports both results to CSVs
  #===================================================================
  write.csv(click_ratio_cat_data, file = click_ratio_file, na = "0", row.names = FALSE)
  write.csv(conv_ratio_cat_data, file = conv_ratio_file, na = '', row.names = FALSE)
  write.csv(conv_ratio_cat_data_sum, file = total_conv_ratio_file, na = "0", row.names = FALSE)
  
  write.csv(click_ratio_cat_data_non, file = click_ratio_file_non, na = "0", row.names = FALSE)
  write.csv(conv_ratio_cat_data_non, file = conv_ratio_file_non, na = '', row.names = FALSE)
  write.csv(conv_ratio_cat_data_sum_non, file = total_conv_ratio_file_non, na = "0", row.names = FALSE)

}  

#=================================================================== 
### Appending the files names to from_to_dates - that is being used 
### by shiny webapp
#===================================================================
append_data <- data.frame("from_date" = format(as.Date(from.date,'%m%d%Y'), '%m/%d/%Y'), 
                          "to.date"   = format(as.Date(to.date,'%m%d%Y'), '%m/%d/%Y'), 
                          "file_name" = c(paste0(paste("click_ratio", from.date, to.date, sep="_" ), ".csv"), 
                                          paste0(paste("non_click_ratio", from.date, to.date, sep="_" ), ".csv"), 
                                          paste0(paste("total_conv_ratio", from.date, to.date, sep="_" ), ".csv"), 
                                          paste0(paste("conv_ratio", from.date, to.date, sep="_" ), ".csv"), 
                                          paste0(paste("non_conv_ratio", from.date, to.date, sep="_" ), ".csv"), 
                                          paste0(paste("non_total_conv_ratio", from.date, to.date, sep="_" ), ".csv")))

write.table(append_data, file = paste0(results_dir, "from_to_dates.csv"), append = TRUE, sep = ",", na = "0", row.names = FALSE, col.names = FALSE)

runTime <- Sys.time()-begTime 
runTime

#=========================================================== Test the results =============================================
#<a href=http://www.misumiusa.com target=_blank>shaft</a>
