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

#Files
 s.file = 'search_log_20131007_20131014.txt'
 c.file = 'codefix_log_20131007_20131014.txt'

#Extract a specific files from an archive with the target weblog directory
 system(paste0("7z x -y P:/Data_Analysis/Archive/archive_2013.7z ", s.file, " -o", getwd()))
 system(paste0("7z x -y P:/Data_Analysis/Archive/archive_2013.7z ", c.file, " -o", getwd()))

#Importing data
 search_log <- read.delim(s.file, header = TRUE, sep = "\t", quote = "", comment.char = "", na.strings=c("NA", ''))
 codeFix_log <- read.delim(c.file, header = TRUE, sep = "\t", quote = "", comment.char = "", na.strings=c("NA", ''))

#clean up data
 search_log <- search_log[c("AccessDateTime", "SessionId", "Q", "SEARCH_TYPE", "Status", "URL", "SERIES_CODE", "CUSTCD")]
 codeFix_log <- codeFix_log[c("SessionId","PRODUCT_CODE", "SERIES_CODE","Referer", "CUSTCD")]

# removing blank keywords and convert  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
# keeping results only with SEARCH_TYPE == 1
 search_log <- subset(search_log, subset = ( !is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1) ))
 search_log$Q <- tolower(search_log$Q)

 codeFix_log <- subset(codeFix_log, subset = (!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))))

# Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
 search_log$Q <- gsub("(.+[^s])s$", "\\1", search_log$Q)

### summarize total keyword searches - search_log[1:100,]
 total_keyword_search <- ddply(search_log, .(Q), summarise, 
                               sum_link = sum(Status=='Link'), 
                               sum_linkCtg = sum(Status=='LinkCtg'), 
                               total_search = sum (sum(Status=='NotFound'), (Status=='Hit'))
                               )

 total_codefix <- ddply(codeFix_log, .(SessionId, Referer), summarise, 
                        codefix_count = length(unique(Referer))
                       )

#Keep only records with Link and remove blank CUSTCD
 search_log <- subset (search_log, (!is.na(CUSTCD) & (Status=='Link')))
 codeFix_log <- subset(codeFix_log, !is.na(CUSTCD))

# Merge total_keyword_search with search 
 keyword_search_merge_anl <- merge(search_log, total_keyword_search, all.x=TRUE, by="Q", sort=FALSE)
 codefix_merge <- merge(codeFix_log, total_codefix, all.x=TRUE, by=c("SessionId", "Referer"), sort=FALSE)

# Get unique values before merge
 keyword_search_merge_anl <- unique(keyword_search_merge_anl[, c("Q", "SessionId", "URL", "SERIES_CODE", "CUSTCD", "sum_link", "sum_linkCtg", "total_search")])
 codefix_merge <- unique(codefix_merge[, c("SessionId", "Referer", "SERIES_CODE", "CUSTCD", "codefix_count", "PRODUCT_CODE")])


#merging keyword_search_merge and codefix_merge frames
 search_codefix_merge <- merge(
   keyword_search_merge_anl,
   codefix_merge,
   by.x = c("SessionId", "URL", "CUSTCD", "SERIES_CODE"), by.y = c("SessionId", "Referer", "CUSTCD", "SERIES_CODE"),
   sort = FALSE  
 )

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
search_codefix_so_merge <- merge(
  search_codefix_merge,
  so_data, 
  by.x = c("CUSTCD", "PRODUCT_CODE"), by.y = c("Customer_Code", "Product_Code"),
  sort = FALSE  
)

#match_custcd <- subset(so_data, Customer_Code %in%  unique(search_codefix_merge$CUSTCD))
analysis_6_results <- ddply(search_codefix_so_merge, .(Q, SERIES_CODE, Product_Name, BusinessUnit, Classify_Code,  
                                                       InnerCode, MediaCodeQT, total_search, sum_link), summarise, 
                           sum_product_total_amount = sum(Product_Total_Amount, na.rm = T), 
                           total_codefix = sum(codefix_count, na.rm = T)
                          )


# Sorting the data based on product total ammount(desc), total search (desc), keywords and number of link categories(desc)
analysis_6_results <- analysis_6_results[order( -analysis_6_results$sum_product_total_amount, 
                                                -analysis_6_results$total_search, 
                                                analysis_6_results$Q),  ]


analysis_file = paste0("6_SO_after_search", "_", file.frm.date, "_", file.to.date, ".csv" )

# Exporting results to csv
# write.csv(analysis_6_results, file = "P:/Data_Analysis/Analysis_Results/keyword_analysis_6.csv", na = '', row.names = FALSE)
# write.csv(analysis_6_results, file = analysis_file, na = '', row.names = FALSE)

unlink(c(s.file, c.file))

# save image of R data for further use
image_file = paste0("Processed_Data", "_", file.frm.date, "_", file.to.date, ".RData" )
# save.image(image_file)

runTime <- Sys.time()-begTime 
runTime

#================ TEST
# subset(search_log, Q=='msv' & SEARCH_TYPE ==1)
# subset(search_codefix_merge, Q=='msv')
# subset(so_data, Customer_Code == 'W70797')
# 
# head(so_data$Customer_Code)
# so_data$Customer_Code[grep('W707', so_data$Customer_Code, ignore.case=TRUE)]
