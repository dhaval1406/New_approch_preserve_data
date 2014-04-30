# This program uses the analysis results from Analysis_8_prototype.R
# Below are the brief steps of this program
#   - Gets the information using API, for each unique keywords
#   - Makes a big dataframe using API results
#   - Does some data format for API dataframe e.g. CategoryID, Link_URL, sort_order=9999
#   - Merge (right join) with Analysis results by preserving the sort order

library(RCurl) 
library(rjson) 
library(plyr)

setwd("P:/Data_Analysis/Processed_R_Datasets")

########## Function to convert the list to datafram for each keyword data through api
  # Input arguments are "category list" and "keyword"
  convert_df <- function(category_list, k){
      category_list <- lapply(category_list, function(x){ 
        
                            # Assign category as parent if there isn't any
                            if(length(x$parent_categories) == 0){
                              modifyList(x, list( parent_categories = x$categoryName ))
                            }
                            # Otherwise, append category at the end of parent categories
                            else{
                              modifyList(x, list( parent_categories = c(x$parent_categories, x$categoryName) ))  
                            }  
                        })
      # Split the parent category names and assign it to a column
      # Create data frame from list elements
      # t() is used to take transpose 
      # Ignores columns named `categoryName_tagged` and `categoryName`
      category_list <- lapply(lapply(category_list, unlist), 
                                function(x) {
                                  names(x)[names(x) == "parent_categories"] <- "parent_categories1"
                                  data.frame(Q=k, t(x[!names(x) %in% c("categoryName_tagged", "categoryName")]))
                                }
                              )
        
      frm <- as.data.frame(rbind.fill(category_list))
      
      # Adding a coulumn with blank values in case if any category from 2-5 are not present. 
      # Otherwise program would fail
      if (is.na(names(frm)[4])) frm$parent_categories2 = NA
      if (is.na(names(frm)[5])) frm$parent_categories3 = NA
      if (is.na(names(frm)[6])) frm$parent_categories4 = NA
      if (is.na(names(frm)[7])) frm$parent_categories5 = NA
      
      return(frm)
  }

#########################  Get API data #########################
# Creating a unique list of keyword to be used for api to get the results
list_keywords <- unique(keyword_search_counts_anl_dt$Q)

# Loop through each keyword and 
# Get api data and
# Covert it to dataframe and 
# Store in `keyword` 
begTime <- Sys.time()
keyword <- data.frame()  # Create an empty data frame to store results

for ( i in 1:length(list_keywords)) { 
  
  if (tolower(list_keywords[i]) %in% c("bumper", "bumpers", "silicon", "nitrile", "rubber bumper", "rubber bumpers",
                                       "rubber washer","silicon","rubber washers","urethane washers","rubber sheets",
                                       "nitrile","urethane washer"
                                       )
      ) {
            print(sprintf("skipping %s", list_keywords[i]))
            next
        }
  # Make correct URL to get data through api 
  req <- paste("http://us.misumi-ec.com/vona2/api/1.0/keyword?Keyword=", 
               gsub(" ", "+", list_keywords[i]), # Multiple words would be passed using `+` sign
               "&categoryItemPerPage=99", #Setting category items to max to get all category names
               sep=""
              ) 
  url <- getURL(req) 
  mj <- fromJSON(url, method = "C")  
  
  # Capturing the keywords those do not return any results
  if(length(mj$response$body$category$list) == 0){
    print(sprintf("Add to the skip list %s", list_keywords[i]))
      next
  }
  
  keyword <- rbind(keyword, convert_df(mj$response$body$category$list, list_keywords[i])) 
} 

runTime <- Sys.time()-begTime 
runTime

##############  Format Column Names and URL ##############

  # Change the column order and rename to match with analysis results
  keyword <- keyword[ c( "Q", paste("parent_categories", 1:5, sep=""), "url" ) ]
  colnames(keyword) <- c("Q", paste("cat_name_", 1:5, sep=""), "Link_URL" )
  
  # Making complete/absolute URL 
  keyword$Link_URL <- paste("http://us.misumi-ec.com", keyword$Link_URL, sep="")

  # Exporting api results in csv
  write.csv(keyword, file = 'keyword.csv', na = '', row.names = FALSE)

############## Get Category_ID ###########################
  # Getting only category id, that is last part of the Link_URL. 
  # Similar regex function is used in Analysis 4, 5 & 8.
  #Function to take last url part and selecting second column for just match value
  URL_parts <- function(x) {
    m <- regexec(".*/(\\w+)/$", x)
    do.call(rbind, lapply(regmatches(x, m), `[`, c(2L)))
  }
  
  Category_Id <- URL_parts(as.character(keyword$Link_URL))
  
  # Append category ids back to original tabl
  keyword_temp <- data.frame(keyword, Category_Id)

##########################################################

# Keyword subset
# keyword_analysis <- as.data.frame(subset(keyword_search_counts_anl_dt, Q %in% c(list_keywords[1:5])))
# keyword_subset <- subset(keyword, Q %in% c(list_keywords[1:5]))
# keyword_merge <- merge(keyword_analysis, keyword_subset, by= names(keyword_subset), all.y=TRUE, sort=FALSE)
# keyword_merge <- keyword_merge[order(keyword_merge$Q, keyword_merge$sort_order, na.last=TRUE), ]

# Convert data table to data frame, so merge would work nicely on right join
keyword_analysis <- as.data.frame(keyword_search_counts_anl_dt)

# Right join analysis data with api data
keyword_merge <- merge(keyword_analysis, keyword_temp, by=names(keyword_temp), all.y=TRUE, sort=FALSE)
keyword_merge <- keyword_merge[order( keyword_merge$Q, keyword_merge$sort_order, na.last=TRUE ), ]

# Assign fake larger sort order, so SQL query can use order by ascending
# And api results would remain towards last
keyword_merge$sort_order[is.na(keyword_merge$sort_order)] <- 9999

# Exporting results in csv
write.csv(keyword_merge, file = 'keyword_merge_prototype.csv', na = '', row.names = FALSE)

############################## Testing Area ###############################

shaft_temp <- subset(keyword_merge, Q=="shaft")

# Creating a unique list of keyword to be used for api to get the results
list_keywords <- unique(keyword_search_counts_anl_dt$Q)


### Temporary save 2 months worth of processed records
save(category, codeFix_log, codefix_merge, codefix_merge_dt, detailTab_log, keyword_analysis, keyword_search_counts_anl_dt, search_log, total_keyword_search,
     file = "2months_data.RData")

# Load already processed data
load("P:/Data_Analysis/Processed_R_Datasets/2months_data.RData") 

####### Creating stemmed keyword list for Johnny #####
total_keyword_search.orig <- total_keyword_search

total_keyword_search$Q <- gsub("(.+[^s])s$", "\\1", total_keyword_search$Q)

total_keyword_search <- ddply(total_keyword_search, .(Q), summarise, 
                              sum_link = sum(sum_link), 
                              sum_linkCtg = sum(sum_linkCtg), 
                              total_search = sum (total_search)
    					)

total_keyword_search <- order(total_keyword_search, -total_keyword_search$total_search)

total_keyword_search <- total_keyword_search[order(-total_keyword_search$total_search), ]

write.csv(total_keyword_search, file="2months_stemmed_keywords", row.names=FALSE, na="0")

###### Getting results with total_searches counts 

temp <- read.csv("keyword_merge_prototype.csv", header=T)
temp.stemmed <- temp
temp.stemmed$Q <- gsub("(.+[^s])s$", "\\1", temp.stemmed$Q)

temp.stemmed.merged <- merge(temp, temp.stemmed)
temp.stemmed.merged <- unique(temp.stemmed.merged)

# temp.stemmed.merged.sorted <- temp.stemmed.merged[order(temp.stemmed.merged$Q, temp.stemmed.merged$sort_order), ]

temp.stemmed.merged.sorted.total <- merge(temp.stemmed.merged, total_keyword_search[, c("Q", "total_search")], by= "Q", all.x=TRUE, sort=FALSE)
temp.stemmed.merged.sorted.total <- temp.stemmed.merged.sorted.total[order(-temp.stemmed.merged.sorted.total$total_search, 
                                                                           temp.stemmed.merged.sorted.total$sort_order), ]

# temp.stemmed.merged[temp.stemmed.merged$Q=="shaft", ]
# unique(temp.stemmed.merged[temp.stemmed.merged$Q=="shaft", ])

write.csv(temp.stemmed.merged.sorted.total, file = 'keyword_analysis_categories_stemmed.csv', na = '', row.names = FALSE)
