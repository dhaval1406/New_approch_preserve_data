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
# search_log <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1) ] [, Q := tolower(Q)]
codeFix_log <- codeFix_log [!(CUSTCD == "WOSMUS")]
cadFile_log <- cadFile_log [!(CUSTCD == "WOSMUS")]
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
  m <- regexec("_.*?_(.*?)_(.*?).txt", c.file)
  file.frm.date <- regmatches(c.file, m)[[1]][2]
  file.to.date  <- regmatches(c.file, m)[[1]][3]
  
  from.date = as.Date(file.frm.date, "%Y%m%d")
  to.date  = as.Date(file.to.date, "%Y%m%d") + 45

#   from.date = format(from.date, "%m%d%Y")
#   to.date = format(to.date+30, "%m%d%Y")  
  
  # Get data from SQL server that are greater or equal to from date
  # Also remove samples with Product_Total_Amount > 0.00
#   so_data <- sqlQuery(misumi_sql, paste("select SO_Date, Customer_Code, Product_Code, Product_Name, Product_Total_Amount, BusinessUnit,",
#                                          "Classify_Code, InnerCode, MediaCodeQT from SO", 
#                                          "where Product_Total_Amount > 0.00 and SO_Date >=", from.date, "and SO_Date <=", to.date))
  so_data <- sqlQuery(misumi_sql, paste0("select Customer_Code, Product_Code, Product_Name, Product_Total_Amount, BusinessUnit,",
                                         "Classify_Code, InnerCode, MediaCodeQT from SO_Accumulative ", 
                                         "where Product_Total_Amount > 0.00 and SODate >= '", from.date, "' and SODate < '", to.date,"'"))
  
#======================================= Merge/summarize weblog data with Sales Order====================================================================
  # Both CUSTCD and PRODUCT_CODE match 
  # Create a keyed data.table from so_data
  setnames (so_data, "Customer_Code", "CUSTCD")
  setnames (so_data, "Product_Code", "PRODUCT_CODE")

  setkey(cad_codeFix_merge, CUSTCD)
  so_data <- data.table(so_data, key=c("CUSTCD", "PRODUCT_CODE"))
  
  cust_prod <- unique(cad_codeFix_merge[, list(CUSTCD, PRODUCT_CODE)])
  setkey(cust_prod, CUSTCD)

#
#  Creating analysis report using long format, including details from visit_log, cadFile_log, so_data and codeFix_log
#           
# 
  cu <- sqlQuery(misumi_sql, "select CUSTOMER_CODE, ACOLL_CUSTOMER_CODE,  CUSTOMER_NAME from CU ")

#   cu.1 <- sqlQuery(misumi_sql, "select * from CU WHERE ACOLL_CUSTOMER_CODE='062843'")

  cu <- data.table(cu, key=c("CUSTOMER_CODE"))
  setnames (cu, "CUSTOMER_CODE", "CUSTCD")  
  
  # merge 
  cust_prod_cu <- merge(
    cust_prod,
    cu, all.x=TRUE
  )

  # total blank ecal code
  sum(is.na(cust_prod_cu$ACOLL_CUSTOMER_CODE))
  
  # split data based on ecal 
  cust_prod_cu_ecal <- cust_prod_cu[complete.cases(cust_prod_cu),]
  cust_prod_cu_no_ecal <- cust_prod_cu[!complete.cases(cust_prod_cu),]
  cust_prod_cu_no_ecal <- cust_prod_cu_no_ecal[, list(ACOLL_CUSTOMER_CODE, PRODUCT_CODE, CUSTCD)]

  cust_prod_cu_ecal_merge <- merge(
    cust_prod_cu_ecal[, list(PRODUCT_CODE,ACOLL_CUSTOMER_CODE)],
    cu[, list(CUSTCD, ACOLL_CUSTOMER_CODE)],
    by="ACOLL_CUSTOMER_CODE"
  )
  
  cust_prod_cu_ecal_merge_big <- rbind(cust_prod_cu_ecal_merge, cust_prod_cu_no_ecal)

  setkey(cust_prod_cu_ecal_merge_big, CUSTCD, PRODUCT_CODE)

  cad_codeFix_so_merge <- so_data[cust_prod_cu_ecal_merge_big, nomatch=0, allow.cartesian=T]

  
  # Assign total product sales to the result table
  result_table$total_product_sales <- sum(cad_codeFix_so_merge$Product_Total_Amount)
  result_table$total_sales_orders <- nrow(so_data)
  result_table$total_site_visits <- nrow(visit_logFromSept)

  analysis_file = paste0("P:/Data_Analysis/Analysis_Results/12_visits_partnum_cad_sales_total_products", "_", file.frm.date, "_", file.to.date, ".csv" )
  
  # Exporting results to csv
  # write.csv(analysis_6_results, file = "P:/Data_Analysis/Analysis_Results/keyword_analysis_6.csv", na = '', row.names = FALSE)
  write.csv(result_table, file = analysis_file, na = '', row.names = FALSE)
  

