# List the files and removing everything
rm(list = ls())
gc() #it will free memory

# Trying to capture runtime
begTime <- Sys.time()

#Getting the required package
require(plyr)
require(data.table)
require(bit64)
#Set working directory
setwd("P:/Data_Analysis/Weblog_Data/")

#Importing data
 search_log <- fread('search_log_20130601_20130821.txt', header = TRUE, sep = "\t", na.strings=c("NA", '')) 
 codeFix_log <- fread('codefix_log_20130601_20130821.txt', header = TRUE, sep = "\t", na.strings=c("NA", '')) 

# search_log <- fread('search_log_20130712_20130719.txt', header = TRUE, sep = "\t", na.strings=c("NA", '')) 
# codeFix_log <- fread('codefix_log_20130712_20130719.txt', header = TRUE, sep = "\t", na.strings=c("NA", '')) 

#clean up data
search_log[, c("USER_CODE",  "CookieId", "BRD_CODE", "IP", "UA", "LogFileName", "UpdateDate", "V17") := NULL]

codeFix_log[, c("AccessDateTime",  "USER_CODE", "CookieId", "BRD_CODE", 
                "SHUNSAKU", "IP", "UA", "LogFileName", "UpdateDate", "V15") := NULL]

# removing blank series codes and conver  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
search_log <- search_log[!is.na(Q) & ( !(CUSTCD == "WOSMUS") | (is.na(CUSTCD)) )  & (SEARCH_TYPE == 1)] [,`:=` (CUSTCD= NULL, Q= tolower(Q))]
codeFix_log <- codeFix_log[!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))] [, CUSTCD := NULL]

search_log$SERIES_CODE = as.character(search_log$SERIES_CODE)
codeFix_log$SERIES_CODE = as.character(codeFix_log$SERIES_CODE)
  
# Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
search_log$Q <- gsub("(.+[^s])s$", "\\1", search_log$Q)

# Remove keywords with :
#       dot(.)
#       any numbers
#       any word with length 1 or 2
#       or contains u- but not followed by bolth(e.g u-bolt)
  search_log <- search_log[!grep("\\.+|\\d+|^\\w{1}$|^\\w{2}$|u-(?!bolt)", Q, perl=TRUE)]

# Remove length 3-6 searches that do not match with any category word of length 3-6
  # Without length 3-6 keywords
  search_log_no_cat3_6 <- search_log[!grep("^\\w{3,6}$", Q)]
  
  # With length 3-6 keywords
  search_log_cat3_6 <- search_log[grep("^\\w{3,6}$", Q)]

  # Keep only length 3-6 keywords that matches part of the category names
  # len3_6.names comes from bottom and yuki's category file
  search_log_cat3_6 <- search_log_cat3_6[Q %in% len3_6.names]

  # Combine both tables to get correct results
  search_log <- rbind(search_log_no_cat3_6, search_log_cat3_6)

  # Summarizing by keywords
  search_log[, `:=` (sum_linkCtg = sum(Status=='LinkCtg'), 
                     total_search = sum (sum(Status=='NotFound'), (Status=='Hit'))),
              by=Q] 

  # Keep records with status=Link and keep selected fields
  search_log <- search_log[Status=='Link', c("Q","SessionId","SERIES_CODE","Link_URL", "sum_linkCtg", "total_search"), with=FALSE]

  # Format URL to get rid of search piece
  search_log$Link_URL <- gsub("(^http.*/+).*", "\\1", search_log$Link_URL)

  # Summarize to get total_links by Series_Code
  total_keyword_search <- unique(search_log[, sum_link := .N, by=list(Q, SERIES_CODE)])

  
  # Set the key for faster operations
  #   setkey(codeFix_log, SessionId, Referer)
  
  # Calculate codefix_count summerizing by SessionId and Referer URL
  codeFix_log[, codefix_count := length(unique(Referer)), by=list(SessionId, Referer)] 
  
  # Keep unique records with the fields that we need
  total_codefix <- unique(codeFix_log[, c("SessionId", "SERIES_CODE", "codefix_count"), with=FALSE])

  # set key for faster operations
  setkey(total_keyword_search, SessionId, SERIES_CODE)
  setkey(total_codefix, SessionId, SERIES_CODE)
		
  search_codefix_merge <- merge(
    total_keyword_search,
    total_codefix,all.x=TRUE, allow.cartesian=TRUE#by keys)
  )
  
  # Summarize and sort the results
  keyword_search_counts_anl <- search_codefix_merge [,list(total_codefix = sum(codefix_count, na.rm = T)), 
                                                      by = list(Q, SERIES_CODE, Link_URL, total_search, sum_linkCtg, sum_link)
                                                     ] [order(-total_search, Q, -sum_link)] 

### Save image of R data for further use
# image_file_name <- paste0("P:/Data_Analysis/Processed_R_Datasets/Data_Load_Prod_", 
#                           format(Sys.time(), "%m%d%Y"), ".RData"
#                          )
# save.image(image_file_name)

write.csv(keyword_search_counts_anl, file = 'P:/Data_Analysis/Analysis_Results/10_analysis_search_links_12_weeks_3.csv', na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime

#=========================================================== Test the results =============================================
######### cleaning up more to remove part number type searches
  
  search_log_mini <- search_log[1:100]

  shaft <- unique(search_log[grep("-", Q)]$Q)

  shaft <- search_log[grep("\\w-(?=pin)", Q, perl=T)]
  search_log[grep("u-(?!bolt)", Q, perl=T)]$Q

  shaft <-(search_log[grep("^\\w{1}$", Q)]$Q)
  shaft <-(search_log[grep("^\\w{2}$", Q)]$Q)

  shaft <-unique(search_log[grep("^\\w{3}$", Q)])

  shaft.cat3 <- shaft[Q %in% tolower(len3.names)]
  
  unique(search_log[grep("^\\w{3}$", Q)]$Q)


############### Making a list of words between length 3-6
  # Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
#   all.cat.names <- gsub("(.+[^s])s$", "\\1", all.cat.names)

  # stemming plurals words anywhere in the sentense
  all.cat.names <- gsub("(.+[^s])s\\b", "\\1", all.cat.names)

  # Match any word with 3 characters - all.cat.names comes from summarize_categories_for_yuki prog
  len3.cat.names <- grep("\\b\\w{3}\\b", all.cat.names, value=T)
  len3.names <- tolower(unique(gsub(".*\\b(\\w{3})\\b.*|","\\1", len3.cat.names)))

  len4.cat.names <- grep("\\b\\w{4}\\b", all.cat.names, value=T)
  len4.names <- tolower(unique(gsub(".*\\b(\\w{4})\\b.*|","\\1", len4.cat.names)))
  
  len5.cat.names <- grep("\\b\\w{5}\\b", all.cat.names, value=T)
  len5.names <- tolower(unique(gsub(".*\\b(\\w{5})\\b.*|","\\1", len5.cat.names)))

  len6.cat.names <- grep("\\b\\w{6}\\b", all.cat.names, value=T)
  len6.names <- tolower(unique(gsub(".*\\b(\\w{6})\\b.*|","\\1", len6.cat.names)))

#   len3_6.names <- tolower(unique(gsub(".*(\\b\\w{3,6}\\b).*","\\1", len3_6.cat.names)))

  len3_6.names <- c(len3.names, 
                    len4.names, 
                    len5.names, 
                    len6.names
                   )


#### test for plural type stuff e.g pulleys

mini.mi <- search_log_cat3_6[1:10]
mini.mi$Q[1] = "shaftss"

  mini.mi[Q %like% len3_6.names]
