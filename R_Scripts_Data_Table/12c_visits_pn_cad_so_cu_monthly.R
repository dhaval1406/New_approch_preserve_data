# This Data analysis is derived from 12b_visits_pn_cad_su_cu.R. 
# It calculates total number of visits, Part Number generated, CAD downloads and Sales Amount
# It uses below data from different servers
# Search_log, codeFix_log, caddownload_log -> web server logs
# SO data - from SQL server's SO table

### Changes compare to previous analysis
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

  # Connect to SQL servers ODBC connction
  misumi_sql <- odbcConnect("misumi_sql", uid="AccessDB", pwd="")
  
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
  
  cu <- sqlQuery(misumi_sql, "select CUSTOMER_CODE, ACOLL_CUSTOMER_CODE,  CUSTOMER_NAME from CU ")
  # cu.1 <- sqlQuery(misumi_sql, "select * from CU WHERE ACOLL_CUSTOMER_CODE='062843'")
  
  cu <- data.table(cu, key=c("CUSTOMER_CODE"))
  setnames (cu, "CUSTOMER_CODE", "CUSTCD")  
    
  #clean up data
  codeFix_log <- codeFix_log [, list (AccessDateTime, CUSTCD, PRODUCT_CODE, SERIES_CODE) ]
  cadFile_log <- cadFile_log [, list (AccessDateTime, CUSTCD, PRODUCT_CODE, SERIES_CODE) ]
  
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
  codeFix_log$SERIES_CODE = as.character(codeFix_log$SERIES_CODE)
  cadFile_log$SERIES_CODE = as.character(cadFile_log$SERIES_CODE)
  
  # Keep only unique records
  codeFix_log <- unique(codeFix_log)
  cadFile_log <- unique(cadFile_log)
  
  #
  # Figure out the date logic to split the result for each month
  #
  from_dates <- c('2013-09-01', '2013-10-01', '2013-11-01', '2013-12-01', '2014-01-01', '2014-02-01')
  to_dates <- c('2013-10-01', '2013-11-01', '2013-12-01', '2014-01-01', '2014-02-01', '2014-03-01')

  # Producing monthly results
  for(i in 1:length(from_dates)){
  
      codeFix_log_monthly <- codeFix_log[AccessDateTime >= from_dates[i] & AccessDateTime < to_dates[i]]
      cadFile_log_montly <- cadFile_log[AccessDateTime >= from_dates[i] & AccessDateTime < to_dates[i]]
      
      # Adding total column
      result_table <- data.table(total_codeFix = nrow(codeFix_log_monthly), 
                                 total_cadDownloads = nrow(cadFile_log_montly))
      
      # Merge CodeFix and CAD download data
      setkey(codeFix_log_monthly, CUSTCD, SERIES_CODE, PRODUCT_CODE)
      setkey(cadFile_log_montly, CUSTCD, SERIES_CODE, PRODUCT_CODE)
      
      cad_codeFix_merge <-codeFix_log_monthly[cadFile_log_montly, nomatch=0]
    
    #======================================= SO data - ODBC part =================================================
      
      # Format date
      from.date = as.Date(from_dates[i])
      to.date  = as.Date(to_dates[i]) + 30
    
      # Get data from SQL server that are greater or equal to from date
      # Also remove samples with Product_Total_Amount > 0.00
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
    # Creating analysis report using long format, including details from visit_log, cadFile_log, so_data, codeFix_log and CU data
    #           
    # 
      # merge  customer code, product code data with customer data from CU
      cust_prod_cu <- merge( cust_prod, cu, all.x=TRUE)
    
      # split data based on ecal code is blank or not
      cust_prod_cu_ecal <- cust_prod_cu[complete.cases(cust_prod_cu),]
      cust_prod_cu_no_ecal <- cust_prod_cu[!complete.cases(cust_prod_cu),]
      cust_prod_cu_no_ecal <- cust_prod_cu_no_ecal[, list(ACOLL_CUSTOMER_CODE, PRODUCT_CODE, CUSTCD)]
    
      # Merge to get all possible customer codes from the ecal code
      cust_prod_cu_ecal_merge <- merge(
        cust_prod_cu_ecal[, list(PRODUCT_CODE,ACOLL_CUSTOMER_CODE)],
        cu[, list(CUSTCD, ACOLL_CUSTOMER_CODE)],
        by="ACOLL_CUSTOMER_CODE"
      )
      
      # Combine both tables, with ecal and without ecal
      cust_prod_cu_ecal_merge_big <- rbind(cust_prod_cu_ecal_merge, cust_prod_cu_no_ecal)
    
      # Merge with SO data
      setkey(cust_prod_cu_ecal_merge_big, CUSTCD, PRODUCT_CODE)
      cad_codeFix_so_merge <- so_data[cust_prod_cu_ecal_merge_big, nomatch=0, allow.cartesian=T]
    
      # Assign total product sales to the result table
      result_table$total_product_sales <- sum(cad_codeFix_so_merge$Product_Total_Amount)
      result_table$total_sales_orders <- nrow(so_data)
      result_table$total_site_visits <- nrow(visit_logFromSept[AccessDateTime >= from_dates[i] & AccessDateTime < to_dates[i]])
    
      analysis_file = paste0("P:/Data_Analysis/Analysis_Results/12c_visits_partnum_cad_so_tot_prod", "_", month(from_dates[i], label=TRUE), ".csv" )
      
      # Exporting results to csv
      write.csv(result_table, file = analysis_file, na = '', row.names = FALSE)
  }

