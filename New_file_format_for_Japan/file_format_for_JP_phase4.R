# Loading required libraries
require(data.table)
require(plyr)
require(combinat)
require(xlsx)

# Setting up working directory
setwd("P:/Data_Analysis/Keyword_Analysis_Results_Gordana/phase4_03282014/Real_Work_merge_permutations_03282014/")

cat.id.orig <- fread("keyword_category_jp_format_02272014_final.txt", header = F, sep = "\t", na.strings=c("NA", '')) 
cat.id.not.found <- fread("keyword_search_notFound_catid.txt", header = T, sep = "\t", na.strings=c("NA", '')) 

web.id.orig <- fread("keyword_series_jp_format_02272014_final.txt", header = F, sep = "\t", na.strings=c("NA", '')) 
web.id.not.found <- fread("keyword_search_notFound_webid.txt", header = T, sep = "\t", na.strings=c("NA", '')) 

# Data clean up -----------------------------------------------------------

  # check if there are any matches between 2 files
  setnames(cat.id.orig, c("keyword", "category_ID", "display_pattern"))
  setnames(web.id.orig, c("keyword", "web_ID", "display_pattern"))
  
  # Duplicates
#   cat.dups <- intersect(cat.id.not.found$keyword, cat.id.orig$keyword)
#   web.id.dups <- intersect(web.id.not.found$keyword, web.id.orig$keyword)
  
  # New keywords in not found files
  cat.id.no.dups <- setdiff(cat.id.not.found$keyword, cat.id.orig$keyword)
  web.id.no.dups <- setdiff(web.id.not.found$keyword, web.id.orig$keyword)

  # Keep new keywords data in not found files
  cat.id.new <- cat.id.not.found[keyword %in% cat.id.no.dups]
  web.id.new <- web.id.not.found[keyword %in% web.id.no.dups]


# Creating permutations for new keywords only --------------------------------------

create_permutations <- function(input_ds){
    # Translate multiple white spaces to single
    keywords <- gsub(" +", " ", input_ds$keyword)

    # split words by space
    splits <- strsplit(keywords, " ")

    # Create empty data table to store permutations
    my.dt <- data.table()
    
    #
    # Going through each split and creating permutations    
    #
    for (i in 1:length(splits)){
        # Getting list of permutation words for each keyword
        perm <- permn(splits[[i]])
        
        # Merge words to make permute keywords
        merged.parts <- sapply(perm, function(x) paste(x, collapse=' '))
    
        # Make a data frame and add keyword column from orignal data
        merged.parts <- data.table("permut_keyword"= merged.parts)
        merged.parts$keyword <- input_ds$keyword[i]
        
        # Add to final data table
        my.dt <- rbind(my.dt, merged.parts)
    }
  
    # Merge permute keywords with orinal data to get other columns
    my.dt.merge <- merge(my.dt, input_ds, by="keyword", all.x=T)
    
    # Keep unique Permutations
    my.dt.merge <- unique(my.dt.merge, by="permut_keyword")
    
    # Removing orignal keyword column and ordering
    my.dt.merge <- my.dt.merge[, -1, with=F][order(permut_keyword)]
    
    # setting column names and adding extra column with value 2 
    # for consistency to rbind later 
    my.dt.merge <- setnames(my.dt.merge, "permut_keyword", "keyword")
    my.dt.merge$display_pattern <- 2
    
    return(my.dt.merge)
}

# Create permutation by calling function
cat.id.new.permut <- create_permutations(cat.id.new)
web.id.new.permut <- create_permutations(web.id.new)

# Merge new keywords with original one ----------------------------
    
    cat.id.final <- rbind(cat.id.orig, cat.id.new.permut)
    web.id.final <- rbind(web.id.orig, web.id.new.permut)
      
    # Keep the unique records - sanity check
    cat.id.final <- unique(cat.id.final)
    web.id.final <- unique(web.id.final)


# Export results in JP style format ---------------------------------------
    write.table(cat.id.final, file="keyword_category_jp_format_05092014_final.txt", quote = F, sep="\t", col.names=F, row.names= F)
    write.table(web.id.final, file="keyword_series_jp_format_05092014_final.txt", quote = F, sep="\t", col.names=F, row.names= F)



# Test zone - exporting for Gordana/Paul in XLS ---------------------------
  setnames(cat.id.final, "category_ID", "cat/web_ID") 
  setnames(web.id.final, "web_ID", "cat/web_ID") 
  
  write.xlsx( rbind(cat.id.final, web.id.final), 
              file = "optimized_keywords_05092014.xlsx", 
              row.names = FALSE )  
  
