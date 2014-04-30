# This Data analysis is derived from 6_keyword_product_sales_DT.R. 
# It calculates total number of visits, Part Number generated, CAD downloads and Sales Amount
# It uses below data from different servers
# Search_log, codeFix_log, caddownload_log -> web server logs
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
require(data.table)
require(RODBC)
require(bit64)
#======================================== web log part ===================================================
#Set working directory
setwd("P:/Data_Analysis/Weblog_Data/")

# Getting file names from command line arguments
args <- commandArgs(trailingOnly = TRUE)

# s.file <- args[1]
# c.file <- args[2]
# cad.file <- args[3]

# To run manually - specify files
# s.file = "search_log_20130901_20140228.txt"
c.file = "codefix_log_20130901_20140228.txt"
cad.file = "caddownload_log_20130901_20140228.txt"
v.file = "visit_log_20130501_20140301.txt"

#Extract a specific files from an archive with the target weblog directory
# system(paste0("7z x -y P:/Data_Analysis/Archive/archive_2013.7z ", s.file, " -o", getwd()))
# system(paste0("7z x -y P:/Data_Analysis/Archive/archive_2013.7z ", c.file, " -o", getwd()))

#Importing data
# search_log <- fread(s.file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 
codeFix_log <- fread(c.file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 
cadFile_log <- fread(cad.file, header = TRUE, sep = "\t", na.strings=c("NA", '')) 
visit_log <- fread(v.file, header = TRUE, sep = "\t", na.strings=c("NA", ''), colClasses="character") 

#clean up data
# search_log[, c("USER_CODE",  "CookieId", "BRD_CODE", "IP", "UA", "LogFileName", "UpdateDate", "V17", "Link_URL") := NULL]

# codeFix_log[, c("AccessDateTime",  "USER_CODE", "CookieId", "BRD_CODE", 
#                 "SHUNSAKU", "IP", "UA", "LogFileName", "UpdateDate", "V15") := NULL]

codeFix_log <- codeFix_log [, list (CUSTCD, PRODUCT_CODE, SERIES_CODE) ]
cadFile_log <- cadFile_log [, list (CUSTCD, PRODUCT_CODE, SERIES_CODE) ]

# removing blank keywords and convert  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
# keeping results only with SEARCH_TYPE == 1
# search_log <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1) ] [, Q := tolower(Q)]
codeFix_log <- codeFix_log [!(CUSTCD == "WOSMUS") | !(is.na(CUSTCD))]
cadFile_log <- cadFile_log [!(CUSTCD == "WOSMUS") | !(is.na(CUSTCD))]
visit_log <- visit_log [!(CUSTCD == "WOSMUS") ]

# Convert AccessDatetime to date format and select data from 09/01/2013 to 02/28/2014
visit_log$AccessDateTime <- as.Date(visit_log$AccessDateTime)
visit_logFromSept <- visit_log[AccessDateTime >='2013-09-01' & AccessDateTime <='2014-02-28']

# Formatting Series_Code - fread assigns it to class 'integer64'
# search_log$SERIES_CODE = as.character(search_log$SERIES_CODE)
codeFix_log$SERIES_CODE = as.character(codeFix_log$SERIES_CODE)
cadFile_log$SERIES_CODE = as.character(cadFile_log$SERIES_CODE)

# Adding total column
result_table <- data.table(total_codeFix = nrow(codeFix_log), 
                           total_cadDownloads = nrow(cadFile_log))

# Keep only unique records
codeFix_log <- unique(codeFix_log)
cadFile_log <- unique(cadFile_log)

# Merge CodeFix and CAD download data
setkey(codeFix_log, CUSTCD, SERIES_CODE, PRODUCT_CODE)
setkey(cadFile_log, CUSTCD, SERIES_CODE, PRODUCT_CODE)

cad_codeFix_merge <-codeFix_log[cadFile_log, nomatch=0]

#======================================= SO data - ODBC part =================================================
  # Connect to SQL servers ODBC connction
  misumi_sql <- odbcConnect("misumi_sql", uid="AccessDB", pwd="")
  
  # Format date
  m <- regexec("_.*?_(.*?)_(.*?).txt", s.file)
  file.frm.date <- regmatches(s.file, m)[[1]][2]
  file.to.date  <- regmatches(s.file, m)[[1]][3]
  
  from.date = as.Date(file.frm.date, "%Y%m%d")
  to.date  = as.Date(file.to.date, "%Y%m%d") + 30

#   from.date = format(from.date, "%m%d%Y")
#   to.date = format(to.date+30, "%m%d%Y")  
  
  # Get data from SQL server that are greater or equal to from date
  # Also remove samples with Product_Total_Amount > 0.00
#   so_data <- sqlQuery(misumi_sql, paste("select SO_Date, Customer_Code, Product_Code, Product_Name, Product_Total_Amount, BusinessUnit,",
#                                          "Classify_Code, InnerCode, MediaCodeQT from SO", 
#                                          "where Product_Total_Amount > 0.00 and SO_Date >=", from.date, "and SO_Date <=", to.date))
  so_data <- sqlQuery(misumi_sql, paste0("select Customer_Code, Product_Code, Product_Name, Product_Total_Amount, BusinessUnit,",
                                         "Classify_Code, InnerCode, MediaCodeQT from SO ", 
                                         "where Product_Total_Amount > 0.00 and SODate >= '", from.date, "' and SODate < '", to.date,"'"))

#======================================= Merge/summarize weblog data with Sales Order====================================================================
# Create a keyed data.table from so_data
setnames (so_data, "Customer_Code", "CUSTCD")
setnames (so_data, "Product_Code", "PRODUCT_CODE")
so_data <- data.table(so_data, key=c("CUSTCD"))

# Setting keys for merging data tables
setkey(cad_codeFix_merge, CUSTCD)

# Merge so_data with serach_codefix_merge
cad_codeFix_so_merge <- so_data[cad_codeFix_merge, nomatch=0, allow.cartesian=T]

# Keeping only unique records, especially from SO table side.
# So not counting SERIES_CODE and PRODUCT_CODE.1
cad_codeFix_so_merge_unique <- unique(cad_codeFix_so_merge, by=names(cad_codeFix_so_merge)[-c(9:10)])

# Assign total product sales to the result table
result_table$total_product_sales <- sum(cad_codeFix_so_merge_unique$Product_Total_Amount)
result_table$total_sales_orders <- nrow(so_data)
result_table$total_site_visits <- nrow(visit_logFromSept)

analysis_file = paste0("P:/Data_Analysis/Analysis_Results/12_visits_partnum_cad_sales_total", "_", file.frm.date, "_", file.to.date, ".csv" )

# Exporting results to csv
# write.csv(analysis_6_results, file = "P:/Data_Analysis/Analysis_Results/keyword_analysis_6.csv", na = '', row.names = FALSE)
write.csv(result_table, file = analysis_file, na = '', row.names = FALSE)

# Both CUSTCD and PRODUCT_CODE match --------------------------------------
  setkey(cad_codeFix_merge, CUSTCD, PRODUCT_CODE)
  so_data <- data.table(so_data, key=c("CUSTCD", "PRODUCT_CODE"))
  
  # Merge so_data with serach_codefix_merge
  cad_codeFix_so_merge.1 <- so_data[cad_codeFix_merge, nomatch=0, allow.cartesian=T]
  
  # Keeping only unique records, especially from SO table side.
  # So not counting SERIES_CODE and PRODUCT_CODE.1
  cad_codeFix_so_merge_unique.1 <- unique(cad_codeFix_so_merge.1, by=names(cad_codeFix_so_merge.1)[-c(9:10)])
  
  # Assign total product sales to the result table
  result_table.1 <- result_table
  result_table.1$total_product_sales <- sum(cad_codeFix_so_merge_unique.1$Product_Total_Amount)
  result_table.1$total_sales_orders <- nrow(so_data)
  result_table.1$total_site_visits <- nrow(visit_logFromSept)  

  analysis_file.1 = paste0("P:/Data_Analysis/Analysis_Results/12_visits_partnum_cad_sales_total_products", "_", file.frm.date, "_", file.to.date, ".csv" )
  
  # Exporting results to csv
  # write.csv(analysis_6_results, file = "P:/Data_Analysis/Analysis_Results/keyword_analysis_6.csv", na = '', row.names = FALSE)
  write.csv(result_table.1, file = analysis_file.1, na = '', row.names = FALSE)
  
# save image of R data for further use
image_file = paste0("P:/Data_Analysis/Processed_R_Datasets/Processed_Data_12", "_", file.frm.date, "_", file.to.date, ".RData" )
save.image(image_file)

runTime <- Sys.time()-begTime 
runTime


# Counting unique visits for webid - `Hierarchi="detail"` -----------------
  visit.hierarchy.detail <- visit_logFromSept[Hierarchy=="detail"]
   
  names(visit.hierarchy.detail)
  
  page.views.detail <- visit.hierarchy.detail[, list(unique_visits = .N), by=list (SessionId, CUSTCD)]
  
  page.views.detail$total_visits <- sum(page.views.detail$unique_visits)
  
  write.csv(page.views.detail, file = "P:/Data_Analysis/Analysis_Results/12_unique_visits_sept_feb.csv", na = '', row.names = FALSE)

# save.image("P:/Data_Analysis/Weblog_Data/my_analysis_ws_04032014.RData")

#
#  Creating analysis report using long format, including details from visit_log, cadFile_log, so_data and codeFix_log
#           
# 
  # loading previously saved data
  load("P:/Data_Analysis/Weblog_Data/my_analysis_ws_04032014.RData")

  visit.hierarchy.detail <- visit_logFromSept[Hierarchy=="detail"]
   
  names(visit.hierarchy.detail)
  
  pg.views.by.webid <- visit.hierarchy.detail[, list(total_webids = .N), by=list (SessionId, CUSTCD, SERIES_CODE)]
  
  pg.views.by.webid[, total_visits:= sum(total_webids), by=list (SessionId, CUSTCD, SERIES_CODE)]

  page.views.detail$total_visits <- sum(page.views.detail$total_webids)
  
  write.csv(page.views.detail, file = "P:/Data_Analysis/Analysis_Results/12_unique_visits_sept_feb.csv", na = '', row.names = FALSE)








