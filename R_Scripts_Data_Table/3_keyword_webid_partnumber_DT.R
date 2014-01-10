# List the files and removing everything
ls ()
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(plyr)

#Set working directory
setwd("P:/Data_Analysis/Analysis_Results/")

# Load already processed data
image_file_name <- paste0("P:/Data_Analysis/Processed_R_Datasets/Data_Load_Prod_", 
                          format(Sys.time(), "%m%d%Y"), ".RData")
load(image_file_name) 

# Trying to capture runtime
begTime <- Sys.time()

#============================================== Main Algorithem =======================================

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

##### Nichena ni jarur nathi
# Merge total_keyword_search with search 
# keyword_search_merge_anl <- merge(search_log, total_keyword_search, all.x=TRUE, by="Q", sort=FALSE)
# codefix_merge <- merge(codeFix_log, total_codefix, all.x=TRUE, by=c("SessionId", "Referer", "SERIES_CODE"), sort=FALSE)

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
                                             by=list(Q, SessionId, Link_URL, sum_link, sum_linkCtg, total_search, X6, X7, X8, X9, X10)]

# merging keyword_search_merge and codefix_merge frames
# coverting data.table to data.frame since merge sucks on data.table
  keyword_search_merge_anl <- as.data.frame(keyword_search_merge_anl)
  total_codefix <- as.data.frame(total_codefix)
  
  search_codefix_merge <- merge(
                              keyword_search_merge_anl,
                              total_codefix,all.x=TRUE,
                              by.x = c("SessionId", "Link_URL"), by.y = c("SessionId", "Referer"),
                              sort = FALSE                            
                           ) 
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
analysis_file = format_name("3_keyword_webid_partnumber");

# Exporting results in csv
write.csv(keyword_search_counts_anl, file = analysis_file, na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime

# save image of R data for further use in Analysis #4 and #5
save.image("P:/Data_Analysis/Processed_R_Datasets/Analysis_3.RData")
# for future saving - less data
#save(search_log, url_parts_merge, file = "Analysis_3.RData")
#=========================================================== Test the results =============================================

