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

# ================================== Data cleansing and formatting  ======================================
        #Importing data
        search_log    <- fread(search_log_file,    header = TRUE,  sep = "\t", na.strings=c("NA", '')) 
        codeFix_log   <- fread(codefix_log_file,   header = TRUE,  sep = "\t", na.strings=c("NA", '')) 
        detailTab_log <- fread(detailtab_log_file, header = TRUE,  sep = "\t", na.strings=c("NA", '')) 
        category      <- fread('category.csv',     header = FALSE, sep = ",",  na.strings=c("NA", ''))
        
        #clean up data
        search_log [, c("USER_CODE",  "CookieId", "BRD_CODE", "IP", "UA", "LogFileName", "UpdateDate", "V17") := NULL]
        
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
        search_log    <- search_log   [!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) ) & (SEARCH_TYPE == 1)] [, Q := tolower(Q)]
        codeFix_log   <- codeFix_log  [ grep("WOSMUS", CUSTCD, invert=T) ] [, CUSTCD := NULL]
        detailTab_log <- detailTab_log[ grep("WOSMUS", CUSTCD, invert=T) ] [, CUSTCD := NULL]
                                      
        # Formatting Series_Code - fread assigns it to class 'integer64'
        search_log$SERIES_CODE    = as.character(search_log$SERIES_CODE)
        codeFix_log$SERIES_CODE   = as.character(codeFix_log$SERIES_CODE)
        detailTab_log$SERIES_CODE = as.character(detailTab_log$SERIES_CODE)
        
        # Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
        search_log$Q <- gsub("(.+[^s])s$", "\\1", search_log$Q)
        
        # summarize total keyword searches
        setkey(search_log, Q)
        search_log[, `:=` (sum_link = sum(Status=='Link'), 
                           sum_linkCtg = sum(Status=='LinkCtg'), 
                           total_search = sum (sum(Status=='NotFound'), (Status=='Hit'))),
                    by=Q ] 
        
        # To get correct unique records, setting key to NULL
        setkey(search_log, NULL)
        total_keyword_search <- unique(search_log[, c("Q", "sum_link", "sum_linkCtg", "total_search"), with=FALSE])
        
        # summarize total tab counts
        setkey(detailTab_log, SessionId, SERIES_CODE)
        detailTab_log[, tab_count := length(unique(Tab_Name)), by = list(SessionId, SERIES_CODE)] 
        
        # Kadach upar nu kadhi ne niche nu lakhavanu
        # total_detailtab <- detailTab_log[, list(tab_count = length(unique(Tab_Name))), by=list(SessionId, SERIES_CODE)] 
        
        # Function to format name of resultant CSV - generated during different analysis
        format_name <- function(x){
          time.stamp = format(Sys.time(), "%m%d%Y")
          file.name = paste(x, sprintf("%s.csv", time.stamp), sep="_")
          return(file.name)
        }

# ========================== Analysis1 - Keyword Transition Ratio =============================
        # Summarize CodeFix counts
        total_codefix <- codeFix_log
          
        # Set the key for faster operations
        setkey(total_codefix, SessionId, Referer)
          
        # Calculate codefix_count summerizing by SessionId and Referer URL
        total_codefix[, codefix_count := length(unique(Referer)), by=list(SessionId, Referer)] 
          
        # Keep unique records with the fields that we need
        setkey(total_codefix, NULL)
        total_codefix <- unique(total_codefix[, c("SessionId", "Referer", "codefix_count"), with=FALSE])
        
        # Setting key to Null, otherwise it will only show unique results for Q
        setkey(search_log, NULL)
        keyword_search_merge_anl <- unique(search_log[, c("Q", "SessionId","URL", "sum_link", "sum_linkCtg", "total_search"), with=FALSE])
        
        # Setting keys for merging data tables
        setkey(keyword_search_merge_anl, SessionId, URL)
        setkey(total_codefix, SessionId, Referer)
        
        #merging keyword_search_merge and codefix_merge frames
        # data.table doesn't support by.x by.y yet. So joining by using cbind
        # A good example is here - http://stackoverflow.com/questions/14126337/multiple-joins-merges-with-data-tables/14126721#14126721
        search_codefix_merge <- cbind( keyword_search_merge_anl, 
                                       total_codefix[J(keyword_search_merge_anl$SessionId, 
                                                       keyword_search_merge_anl$URL)
                                                     ][, list(codefix_count=codefix_count)])
        
        # Sorting based on highest 
        keyword_search_counts_anl <- search_codefix_merge [ , list(total_codefix = sum(codefix_count, na.rm = T)), 
                                                              by = list(Q, total_search, sum_linkCtg, sum_link)
                                                            ] [order(-total_search)] 
        # Format csv name
        analysis_file <- paste0( paste("1_keyword_transition_ratio", from.date, to.date, sep = "_" ), ".csv" )
        
        # Exporting results in csv
        write.csv(keyword_search_counts_anl, file = analysis_file, na = "0", row.names = FALSE)

# =========================== Analysis2 - Keyword Webid ===============================

        # Summarize CodeFix counts
        total_codefix <- codeFix_log
          
        # Set the key for faster operations
        setkey(total_codefix, SessionId, Referer, SERIES_CODE)
          
        # Calculate codefix_count summerizing by SessionId and Referer URL
        total_codefix[, codefix_count := length(unique(Referer)), by=list(SessionId, Referer, SERIES_CODE)] 
          
        # Keep unique records with the fields that we need
        setkey(total_codefix, NULL)
        total_codefix <- unique(total_codefix[, c("SessionId", "Referer","SERIES_CODE", "codefix_count"), with=FALSE])
        
        # Removing blank series codes
        keyword_search_merge <- search_log[!is.na(SERIES_CODE)]
        
        # Adding extra field to capture links by series code
        keyword_search_merge$link_series <- 1
        
        # Get unique values before merge
        setkey(detailTab_log, NULL)
        detailtab_merge <- unique(detailTab_log[, c("SessionId", "SERIES_CODE", "tab_count"), with=FALSE])
        
        # Setting keys for merging data tables
        keyword_search_merge_anl <- keyword_search_merge
        
        setkey(keyword_search_merge_anl, SessionId, URL, SERIES_CODE)
        setkey(total_codefix, SessionId, Referer, SERIES_CODE)
        
        #merging keyword_search_merge and codefix_merge frames
        # data.table doesn't support by.x by.y yet. So joining by using cbind
        # A good example is here - http://stackoverflow.com/questions/14126337/multiple-joins-merges-with-data-tables/14126721#14126721
        search_codefix_merge_series <- cbind( keyword_search_merge_anl, 
                                               total_codefix[J(keyword_search_merge_anl$SessionId, 
                                                               keyword_search_merge_anl$URL,
                                                               keyword_search_merge_anl$SERIES_CODE)
                                                             ])
        
        ### merging keyword_search_merge and codefix_merge frames
          setkey(search_codefix_merge_series, SessionId, SERIES_CODE)
          setkey(detailtab_merge, SessionId, SERIES_CODE)
          
          # merge by keys, remove non-matched lines (making it innter join)
          search_codefix_detailtab_merge<-detailtab_merge[search_codefix_merge_series, nomatch=0] 
        
        # Select unique records - replacing missing values as 0 
        # giving multiple results for multiple session ID. So replaced with ddply below
        
          keyword_search_counts <- search_codefix_detailtab_merge [ ,list( total_codefix = sum(codefix_count, na.rm = T),
                                                                           total_tabcount = sum(tab_count, na.rm = T),
                                                                           links_by_series = sum(link_series, na.rm = T) 
                                                                          ), 
                                                                     by = list(Q, SERIES_CODE, total_search, sum_link, sum_linkCtg)
                                                                  ]  [order(-total_search, Q, SERIES_CODE)] 
        
        # Format csv name
        analysis_file <- paste0( paste("2_keyword_webid", from.date, to.date, sep="_" ), ".csv" )
        
        # Exporting results in csv
        write.csv(keyword_search_counts, file = analysis_file, na = "0", row.names = FALSE)

# =========================== Analysis 3 - Keyword WebId PartNumber =============================

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
        
        #Keep only records with LinkCtg to save time on merging unnecessary records
        keyword_search_merge_anl <- search_log [Status=='LinkCtg']
        
        #
        # ----------------- Making category columns by splitting URL ------------------------     
        #        
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
        # -----------------------------------------------------------------------
        
        # Get unique values before merge
        keyword_search_merge_anl <- url_parts_search[, list(LinkCtg=.N), 
                                                        by=list(Q, SessionId, Link_URL, sum_link, 
                                                                sum_linkCtg, total_search, X6, X7, X8, X9, X10)]
        
        # merging keyword_search_merge and codefix_merge frames
        # coverting data.table to data.frame since merge sucks on data.table
        keyword_search_merge_anl <- as.data.frame(keyword_search_merge_anl)
        total_codefix <- as.data.frame(total_codefix)
          
        search_codefix_merge <- merge(
                                      keyword_search_merge_anl,
                                      total_codefix,all.x=TRUE,
                                      by.x = c("SessionId", "Link_URL"), by.y = c("SessionId", "Referer"),
                                      sort = FALSE) 
        
        search_codefix_merge <- as.data.table(search_codefix_merge)
        
        # Taking substring to match with each category ids, 3, 5, 7, 9, 11
        search_codefix_merge[, `:=`(cat_id_1 = substr(search_codefix_merge$X6, 1, 3),
                                    cat_id_2 = substr(search_codefix_merge$X7, 1, 5),
                                    cat_id_3 = substr(search_codefix_merge$X8, 1, 7),
                                    cat_id_4 = substr(search_codefix_merge$X9, 1, 9),
                                    cat_id_5 = substr(search_codefix_merge$X10, 1, 11))]
        
        # merge by keys, remove non-matched lines (making it innter join)
        # converting data.table to data.frame since NAs can't be used as key
          search_codefix_merge <- as.data.frame(search_codefix_merge)
          category <- as.data.frame(category)
          
          url_parts_merge <- merge(
            search_codefix_merge,
            category,
            by = c("cat_id_1", "cat_id_2", "cat_id_3", "cat_id_4", "cat_id_5"),
          )
          
          # Converting back to data.table for speed up
          url_parts_merge <- as.data.table(url_parts_merge)
        
        # Sorting the data based on total search (desc), keywords and number of link categories(desc)
        keyword_search_counts_anl <- url_parts_merge[, list(LinkCtg = sum(LinkCtg), 
                                                             total_codefix_linkCtg = sum(codefix_count, na.rm = T)),
                                                        by = list(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, SERIES_CODE, total_search, sum_link, sum_linkCtg)
                                                      ][order(-total_search,Q,-LinkCtg)]
        
        # Format csv name
        analysis_file <- paste0( paste("3_keyword_webid_partnumber", from.date, to.date, sep="_" ), ".csv" )
        
        # Exporting results in csv
        write.csv(keyword_search_counts_anl, file = analysis_file, na = '', row.names = FALSE)

# ================================== Analysis 4  ======================================

# ================================== Analysis 5  ======================================


# =============================== Processed Data Storage  ==================================

        ### Save image of R data for further use
        image_file_name <- paste0("P:/Data_Analysis/Processed_R_Datasets/", 
                                  paste("Data_Load_Prod", from.date, to.date, sep="_" ), ".RData")
        
        save.image(image_file_name)

runTime <- Sys.time()-begTime 
runTime