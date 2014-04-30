################################################
#  Convert Keyword analysis file to JP format  #
#  Format : File needs to be `tab delimited`   #
#          Multi-value fields (Series_Code)    #
#          are comma seperated                 #
################################################

# Setting up working directory
setwd("P:/Data_Analysis/Keyword_Analysis_Results_Gordana")

### Function to clean up data, including an option
### to preprocess Not-Found data
data_cleanup <- function(file = NULL, preprocess = "No"){

  # Check that "file" is non-NULL; else throw error
    if(is.null(file)){
      stop("NULL file name")    
    }
  # Check that "preprocess" is yes or no; else throw error
    if(!tolower(preprocess) %in% c("yes", "no")){
      stop("Value for preprocess is not yes or no")    
    }

  # Read the csv file from keyword analysis
    keyword.data <- read.csv(file, header=T, , quote = "\"", comment.char = "", na.strings=c("NA", ''), , stringsAsFactors = F)

    if(tolower(preprocess) == "yes"){
      
        # Add No. field to make it consistent with Gordana's file
        keyword.data$No. <- 1:nrow(keyword.data)
        
        # Split Category_Id column for new line character.
        my.list <- strsplit(keyword.data[, 2], "\n")
        
        # Splits Category Id into different rows
        rep_times <- vapply(my.list, FUN=length, FUN.VALUE=integer(1))
        
        # Data frame with correct repetition of keywords and order number
        # By splitting line seperated field into seperate rows
        keyword.data <- data.frame(Category_Id=unlist(my.list), Q=rep(keyword.data$Q, rep_times), No. = rep(keyword.data$No., rep_times ) )
        
    }    
    
  # Keeping unique required columns from the data
    keyword.data <- unique(keyword.data[, c("No.", "Q", "Category_Id")])
  
  # Clean up data - Remove blank Category_Ids
  #               - Selcet rows without unnecessary known words from Category_Id
  #               - Selcet rows without known special characters from keywords             
    keyword.data <- keyword.data[!is.na(keyword.data$Category_Id), ]
    keyword.data <- keyword.data[c(grep("http|not found|product match|customer|tel|inquire|new", keyword.data$Category_Id, ignore.case=T, invert=T)), ]
    keyword.data <- keyword.data[c(grep("\\?", keyword.data$Q, ignore.case=T, invert=T)), ]

   return(keyword.data)
}

### Function to convert cleaned data to JP format
convert_jp_format <- function(data){
  # Used aggregate.data.frame to Combine multiple columns by a specific column value. 
  # This will return a data frame with a list of Cat values. 
    data.cat.merge <- aggregate.data.frame(data$Category_Id, by=list(data$Q, data$No.), paste)
  
  # Used sapply with paste function to collapse by ,. This will make the comma seperated category ids in the same columns
    data.cat.merge$x <- sapply(data.cat.merge$x, paste, collapse = ",")

  # Add number 2 as the last column - Japan wants it in this format    
    data.cat.merge$jpCode <- 2

  # Keeping what we need 
    converted.data <- data.cat.merge[, -2]
  
  return(converted.data) 
}

############################## Calling Function ##################################

# Data cleanup
  cleaned.file.1 <- data_cleanup("Keywords_analysis_categories.csv", "No")
  cleaned.file.2 <- data_cleanup("Not_found_kwrds.csv", "Yes")

# Combined both converted files together
  cleaned.data <- rbind(cleaned.file.1, cleaned.file.2)

# Record indexes with web-ids
  web_ids <- grep("^[0-9]+$", cleaned.data$Category_Id, ignore.case=T)

# Splitting Category Ids and Web Ids  
  cleaned.data.web_ids <- cleaned.data[ web_ids, ]
  cleaned.data.cat_ids <- cleaned.data[-web_ids, ]

# Calling convert_jp_format function to convert the results
  converted.data.web_ids <- convert_jp_format(cleaned.data.web_ids)
  converted.data.cat_ids <- convert_jp_format(cleaned.data.cat_ids)

# Exporting the results as tab delimited file (with comma seperated categories fields)  
  write.table(converted.data.web_ids, file="keyword_cat_jp_format_web_ids.txt", quote = F, sep="\t", col.names=F, row.names= F)
  write.table(converted.data.cat_ids, file="keyword_cat_jp_format_cat_ids.txt", quote = F, sep="\t", col.names=F, row.names= F)
    
