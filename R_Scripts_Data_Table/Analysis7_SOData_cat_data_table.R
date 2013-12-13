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
require(data.table)
require(bit64)
require(RODBC)
#======================================== web log part ===================================================
#Set working directory
setwd("P:/Data_Analysis/Weblog_Data/")

# Getting file names from command line arguments
args <- commandArgs(trailingOnly = TRUE)

s.file <- args[1]
c.file <- args[2]

# To run manually - specify files
# s.file = 'search_log_20130901_20131001.txt'
# c.file = 'codefix_log_20130901_20131001.txt'

#Extract a specific files from an archive with the target weblog directory
system(paste0("7z x -y P:/Data_Analysis/Archive/archive_2013.7z ", s.file, " -o", getwd()))
system(paste0("7z x -y P:/Data_Analysis/Archive/archive_2013.7z ", c.file, " -o", getwd()))

#Importing data
search_log <- fread(s.file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 
codeFix_log <- fread(c.file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 

category <- fread('category.csv', header = FALSE, sep = ",", na.strings=c("NA", ''))

#clean up data
search_log[, c("USER_CODE",  "CookieId", "BRD_CODE", "IP", "UA", "LogFileName", "UpdateDate", "V17", "URL") := NULL]

codeFix_log[, c("AccessDateTime",  "USER_CODE", "CookieId", "BRD_CODE", 
                "SHUNSAKU", "IP", "UA", "LogFileName", "UpdateDate", "V15") := NULL]

# First two columns are not used
category[, c(1:2, 13) := NULL]

# Assigning column names, setnames is similar-but faster- as columnname
setnames(category, c("cat_id_1", "cat_name_1","cat_id_2", "cat_name_2","cat_id_3", "cat_name_3","cat_id_4", "cat_name_4","cat_id_5", "cat_name_5"))

# removing blank keywords and convert  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
# keeping results only with SEARCH_TYPE == 1
search_log <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1) ] [, Q := tolower(Q)]
codeFix_log <- codeFix_log [!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))]

# Formatting Series_Code - fread assigns it to class 'integer64'
search_log$SERIES_CODE = as.character(search_log$SERIES_CODE)
codeFix_log$SERIES_CODE = as.character(codeFix_log$SERIES_CODE)

# Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
search_log$Q <- gsub("(.+[^s])s$", "\\1", search_log$Q)

### summarize total keyword searches
setkey(search_log, Q)

search_log[, `:=` (sum_link = sum(Status=='Link'), 
                   sum_linkCtg = sum(Status=='LinkCtg'), 
                   total_search = sum (sum(Status=='NotFound'), (Status=='Hit'))),
            by=Q] 

### Summarize CodeFix counts
  total_codefix <- codeFix_log
  
  # Set the key for faster operations
  setkey(total_codefix, SessionId, Referer, SERIES_CODE)
  
  # Calculate codefix_count summerizing by SessionId and Referer URL
  total_codefix[, codefix_count := length(unique(PRODUCT_CODE)), by=list(SessionId, Referer, SERIES_CODE)] 
  
  # Remove blank CUSTCD
  total_codefix <- total_codefix[!is.na(CUSTCD)]
  
  # Keep unique records with the fields that we need
  setkey(total_codefix, NULL)
  total_codefix <- unique(total_codefix[, with=FALSE])

#Keep only records with Link and remove blank CUSTCD
search_log <- search_log[!is.na(CUSTCD) & (Status=='LinkCtg')]

# Assign to keyword_search_merge_anl to make it consistent
keyword_search_merge_anl <- search_log

#======================================= new url ==========================================
  #Coverting URLs to character to avoid any error messages
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
                                             by=list(Q, SessionId, Link_URL, CUSTCD, sum_link, sum_linkCtg, total_search, X6, X7, X8, X9, X10)]
  

# Setting keys for merging data tables
setkey(keyword_search_merge_anl, SessionId, CUSTCD)
setkey(total_codefix, SessionId, CUSTCD)

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

#======================================= SO data - ODBC part =================================================
  # Connect to SQL servers ODBC connction
  misumi_sql <- odbcConnect("misumi_sql", uid="AccessDB", pwd="")
  
  # Format date
  m <- regexec("_.*?_(.*?)_(.*?).txt", s.file)
  file.frm.date <- regmatches(s.file, m)[[1]][2]
  file.to.date  <- regmatches(s.file, m)[[1]][3]
  
  from.date = as.Date(file.frm.date, "%Y%m%d")
  to.date  = as.Date(file.to.date, "%Y%m%d")

  from.date = format(from.date, "%m%d%Y")
  to.date = format(to.date+30, "%m%d%Y")  
  
  # Get data from SQL server that are greater or equal to from date
  # Also remove samples with Product_Total_Amount > 0.00
  so_data <- sqlQuery(misumi_sql, paste("select SO_Date, Customer_Code, Product_Code, Product_Name, Product_Total_Amount, BusinessUnit,",
                                         "Classify_Code, InnerCode, MediaCodeQT from SO", 
                                         "where Product_Total_Amount > 0.00 and SO_Date >=", from.date, "and SO_Date <=", to.date))

#======================================= Merge/summarize weblog data with Sales Order====================================================================
# Create a keyed data.table from so_data
so_data <- data.table(so_data, key=c("Customer_Code", "Product_Code"))

# Setting keys for merging data tables
setkey(url_parts_merge, CUSTCD, PRODUCT_CODE)

# Merge so_data with serach_codefix_merge
search_codefix_so_merge <-so_data[url_parts_merge, nomatch=0]

# Sorting the data based on product total ammount(desc), total search (desc), keywords and number of link categories(desc)
analysis_7_results <- search_codefix_so_merge[, list(LinkCtg = sum(LinkCtg),
                                                     sum_product_total_amount = sum(Product_Total_Amount, na.rm = T),
                                                     total_codefix = sum(codefix_count, na.rm = T)), 
                                                by=list(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, SERIES_CODE, Product_Name,  
                                                       BusinessUnit, Classify_Code, InnerCode, MediaCodeQT, total_search, sum_link, sum_linkCtg)
                                             ] [ order(-sum_product_total_amount, -total_search, Q, -LinkCtg) ]

analysis_file = paste0("P:/Data_Analysis/Analysis_Results/7_keyword_category_sales", "_", file.frm.date, "_", file.to.date, ".csv" )

# Exporting results to csv
# write.csv(analysis_7_results, file = "P:/Data_Analysis/Analysis_Results/keyword_analysis_6.csv", na = '', row.names = FALSE)
write.csv(analysis_7_results, file = analysis_file, na = '', row.names = FALSE)

# Remove file copies after processing
unlink(c(s.file, c.file))

# save image of R data for further use
image_file = paste0("P:/Data_Analysis/Processed_R_Datasets/Processed_Data_7", "_", file.frm.date, "_", file.to.date, ".RData" )
save.image(image_file)

runTime <- Sys.time()-begTime 
runTime

#================================ TEST ================================================
