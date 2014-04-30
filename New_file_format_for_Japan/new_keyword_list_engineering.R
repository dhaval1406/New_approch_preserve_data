### Purpose : To populate new keyword list for engineering to work on 
### Data : Processed and saved data from Analysis9_api_prototype
###        from here -> P:/Data_Analysis/Processed_R_Datasets/2months_data.RData  
### 
### Description: This analysis is mix of Analysis8_prototype.R
###
###
###
###
###

# List the files and removing everything
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(data.table)
require(plyr)

#Set working directory
setwd("P:/Data_Analysis/Analysis_Results/")

# Load already processed data
load("P:/Data_Analysis/Processed_R_Datasets/2months_data.RData") 

### ================================================================
### Code copied from Analysis8_prototype.R and optimized for speed
### ================================================================

  # removing internal acccounts with CUSTCD = "WOSMUS"
  codeFix_log <- subset(codeFix_log, subset = ( !is.na(Referer) ))
  
  total_codefix <- ddply(codeFix_log, .(SessionId, Referer, SERIES_CODE), summarise, 
                         codefix_count = length(unique(PRODUCT_CODE))
  )
  
  #Keep only records with LinkCtg to save time on merging unnecessary records
  search_log <- subset (search_log, Status=='LinkCtg')
  
  # Merge total_keyword_search with search 
  keyword_search_merge_anl <- merge(search_log, total_keyword_search, all.x=TRUE, by="Q", sort=FALSE)
  codefix_merge <- merge(codeFix_log, total_codefix, all.x=TRUE, by=c("SessionId", "Referer", "SERIES_CODE"), sort=FALSE)
  
  #=================================== Partial Match =======================================================
  # Using data.table
  library(data.table)
  
  # Get unique values before merge
  # url_parts_search_dt = data.table(url_parts_search)
  keyword_search_merge_anl = data.table(keyword_search_merge_anl)
  
  codefix_merge_dt = data.table(codefix_merge, key = 'SessionId') [, PRODUCT_CODE:=NULL]
  
  keyword_search_merge_anl_dt = unique( keyword_search_merge_anl[, LinkCtg := .N, 
                                                      by=list(Q, SessionId, Link_URL, sum_link, sum_linkCtg, total_search)] [,
                                                                                                                                                  
                                                      list(Q, SessionId, Link_URL, sum_link, sum_linkCtg, total_search, LinkCtg)]
                                      )
  # Setkey for faster join
  setkey(keyword_search_merge_anl_dt, SessionId)
  # setkey(keyword_search_merge_anl_dt, SessionId)
  
  # Partial merge of `codefix_merge_dt` and `keyword_search_merge_anl_dt` - using Link_URL as pattern, and Referer as table
  # Also wrap grep result with `mapply` to apply grep function to each element of each argument
  # Also transforming/taking substring to match with each category ids, 3, 5, 7, 9, 11
  search_codefix_merge_dt = codefix_merge_dt [ keyword_search_merge_anl_dt, nomatch = 0, allow.cartesian=TRUE ]  [ 
                                               
                                               mapply(grep, Link_URL, Referer)==1 ]  
  
  # Reset the key to get the unique values correctly
  search_codefix_merge_dt = setkey(search_codefix_merge_dt, NULL)
  search_codefix_merge_dt = unique(search_codefix_merge_dt)
  
  # Converting data.frame back to data.table to harness the power of data.table
  # url_parts_merge_dt = data.table(url_parts_merge)
  
  # Compound query: Summarizing based on `by` variablek
  #                 Keeping only selected varialbes
  #                 Sorting based on total search (desc), keywords and number of link categories(desc)
  keyword_search_counts_anl_dt = search_codefix_merge_dt[, `:=` (LinkCtg = sum(LinkCtg), total_codefix_linkCtg = sum(codefix_count, na.rm = T)), 
                                                              by =list(Q, total_search, sum_link, sum_linkCtg)] [,
                                                     
                                                              list(Q, total_search, sum_link, sum_linkCtg, LinkCtg, total_codefix_linkCtg, Link_URL)] [
                                                              
                                                              order(-total_search, Q, -LinkCtg)  ]
                                  
  # Reset the key to get the correct value for unique records
  keyword_search_counts_anl_dt = unique(setkey(keyword_search_counts_anl_dt, NULL))
  # keyword_search_counts_anl_dt = unique(setkey(keyword_search_counts_anl_dt, NULL))
  
  # Keeping only columns that are used in the search prototype
  keyword_search_counts_anl_dt = keyword_search_counts_anl_dt[, list(Q, Link_URL)]
  # keyword_search_counts_anl_dt = keyword_search_counts_anl_dt[, list(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, Link_URL)]
  
  # Getting only category id, that is last part of the Link_URL. 
  # Similar regex function is used in Analysis 4 & 5.
  # Function to take last url part and selecting second column for just match value
  URL_parts <- function(x) {
    m <- regexec(".*/(\\w+)/$", x)
    do.call(rbind, lapply(regmatches(x, m), `[`, c(2L)))
  }
  
  category_ids <- URL_parts(as.character(keyword_search_counts_anl_dt$Link_URL))
  # category_ids <- URL_parts(as.character(keyword_search_counts_anl_dt$Link_URL))
  colnames(category_ids) <- c("Category_Id")
  
  # Append category ids back to original tabl
  keyword_search_counts_anl_dt <- data.table(keyword_search_counts_anl_dt, category_ids)
  # keyword_search_counts_anl_dt <- data.table(keyword_search_counts_anl_dt, category_ids)
  
  # Adding a sort_order field to preserve the order in the webpage query
  keyword_search_counts_anl_dt[, sort_order:=(1:nrow(keyword_search_counts_anl_dt))]

### ================================================================
### Remove previously optimized keywords to get new list
### ================================================================

# Read in previouly submitted files
  submitted_file_cat = "P:/Data_Analysis/Keyword_Analysis_Results_Gordana/Updated_after_phase1_Gordana/submitted_12162013/keyword_category_jp_format_12162013.txt"
  submitted_file_series = "P:/Data_Analysis/Keyword_Analysis_Results_Gordana/Updated_after_phase1_Gordana/submitted_12162013/keyword_series_jp_format.txt"

  keywords_cat  <- read.delim(submitted_file_cat, header = F, sep = "\t", quote = "", comment.char = "", na.strings=c("NA", ''))
  keywords_series  <- read.delim(submitted_file_series, header = F, sep = "\t", quote = "", comment.char = "", na.strings=c("NA", ''))

# Combine category and series data to get list of keywords
  submitted_keywords <- rbind(keywords_cat, keywords_series)

# New keywords that are not presnt in previouly submitted files
  new_keywords <- keyword_search_counts_anl_dt[!keyword_search_counts_anl_dt$Q %in% as.character(unique(submitted_keywords$V1)), ]

# Clean up keywords - removing control characters etc.
  new_keywords <- new_keywords[!is.na(new_keywords$Category_Id), ]
  new_keywords <- new_keywords[c(grep("[[:cntrl:]]", new_keywords$Q, ignore.case=T, invert=T)), ]

# # Create group number per unique keyword, to use in aggregate.data.frame's by - this will preserve the sort order
#   creat_no <- data.frame(Q = unique(new_keywords$Q), No.= 1:length(unique(new_keywords$Q)))
# 
# # Merge group sort order with original data 
#   new_keywords_merge <- merge(new_keywords, creat_no, by="Q")

# Used aggregate.data.frame to Combine multiple columns by a specific column value. 
# This will return a data frame with a list of Cat values. 
# Code copies from `file_format_for_JP_Final.R`
#   data.cat.merge <- aggregate.data.frame(new_keywords_merge$Category_Id, by=list(new_keywords_merge$Q, new_keywords_merge$No.), paste)
  data.cat.merge <- aggregate.data.frame(new_keywords_merge$Category_Id, by=list(new_keywords_merge$Q), paste)
  
# Used sapply with paste function to collapse by ,. This will make the comma seperated category ids in the same columns
  data.cat.merge$x <- sapply(data.cat.merge$x, paste, collapse = ",")

#   # Add number 2 as the last column - Japan wants it in this format    
#     data.cat.merge$jpCode <- 2
# 
# Keeping what we need 
#   converted.data <- data.cat.merge[, -2]
  
write.table(data.cat.merge, file="P:/Data_Analysis/Keyword_Analysis_Results_Gordana/phase2_new_keywords/new_keyword_data_12202013.txt", quote = F, sep="\t", col.names=F, row.names= F)

