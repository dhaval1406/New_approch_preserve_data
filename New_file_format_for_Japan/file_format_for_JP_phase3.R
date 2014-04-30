# Loading required libraries
require(data.table)
require(plyr)
require(combinat)

# Setting up working directory
setwd("P:/Data_Analysis/Keyword_Analysis_Results_Gordana/phase3_02182014/Real_Work_merge_permutations_02262014/")

cat.id.orig <- fread("updated_keywords_category_12162013_updated.txt", header = F, sep = "\t", na.strings=c("NA", '')) 
cat.id.not.found <- fread("keyword_search_notFound_catid.txt", header = T, sep = "\t", na.strings=c("NA", '')) 

web.id.orig <- fread("keyword_series_jp_format.txt", header = F, sep = "\t", na.strings=c("NA", '')) 
web.id.not.found <- fread("keyword_search_notFound_webid.txt", header = T, sep = "\t", na.strings=c("NA", '')) 

# Data clean up -----------------------------------------------------------
  # check if there are any matches between 2 files
  intersect(cat.id.orig$V1, cat.id.not.found$keyword)
  intersect(web.id.orig$V1, web.id.not.found$keyword)

# check if there are any matches between keywords and stemming
  dup.stemming <- intersect(cat.id.not.found$keyword, cat.id.not.found$stemming)
  intersect(web.id.not.found$keyword, web.id.not.found$stemming) # no match for web.ids
  
  # clean up stemming words that exist in keyword field
  cat.id.not.found[stemming %in% dup.stemming]$stemming <- ''

# Seperate stemmed words --------------------------------------------------
  stemming.parts <- lapply(cat.id.not.found$stemming, strsplit, ",", perl=TRUE)
  stemming.parts.web.id <- lapply(web.id.not.found$stemming, strsplit, ",", perl=TRUE)

# Generate new rows for synonyms ------------------------------------------
  generate_new_synonym_rows <- function(part_list, ds){
      # Make seperate columns for each value    
      columns <- as.data.frame(do.call (rbind.fill.matrix, 
                                        lapply(part_list, function(x) do.call(rbind, x))))
      # 
      make_synonym_rows_list <- function(x) {
          
          my.list <-  lapply(1:ncol(columns), 
                            function(y){
                                cbind(keyword = columns[x,y], ds[x, -1, with=F])
                            })
  
          return(rbindlist(my.list))  
      }    
      
      # Calling above function, to make synonym row list
      synonym.list <- lapply(1:nrow(ds), make_synonym_rows_list)
      
      # Combine all list objects to create a data.table from it
      synonym.list.table <- rbindlist(synonym.list)
      # Exclue any NAs from the result
      synonym.list.table <- synonym.list.table[complete.cases(synonym.list.table), ]
      
      # Strip leading and trailing spaces
      synonym.list.table$keyword <- gsub("^\\s|\\s$", "", synonym.list.table$keyword)
      
      return(synonym.list.table)
  }

# Merge synonm data with cat.id.not.found data ----------------------------
    df1 <- generate_new_synonym_rows(stemming.parts, cat.id.not.found)
    df2 <- generate_new_synonym_rows(stemming.parts.web.id, web.id.not.found)
    
    # Merge stemming keywords with originals
    df.final <- rbind(cat.id.not.found, df1)
    df.final.web <- rbind(web.id.not.found, df2)
    
    # Don't need column `stemming` anymore
    df.final$stemming <- NULL
    df.final.web$stemming <- NULL
    
    # Make same column names and add third column with value 2 (for displacement)
    setnames(df.final, c("V1", "V2"))
    df.final$V3 <- 2    

    setnames(df.final.web, c("V1", "V2"))
    df.final.web$V3 <- 2    

    # Check for the duplicates
    df.final[duplicated(df.final$V1)]
    df.final[V1 %in% df.final[duplicated(df.final$V1)]$V1][order(V1)]

    df.final.web[duplicated(df.final.web$V1)]
    df.final.web[V1 %in% df.final.web[duplicated(df.final.web$V1)]$V1][order(V1)]
    
    # Keep the unique records
    df.final <- unique(df.final)
    df.final.web <- unique(df.final.web)

# Check for duplicates with original data ---------------------------------
  # web id doesn't have any duplicates
  dups <- intersect(cat.id.orig$V1, df.final$V1)
  df.final[V1 %in% dups]
  cat.id.orig[V1 %in% dups]

  # Resolve conflicts - provide list to Gordana
  all.dups <- rbind(df.final[V1 %in% dups], cat.id.orig[V1 %in% dups])
  write.csv(all.dups, "all.dupes.csv")

  # 02/27/2014 - Removing dups form orig file. GD confirmed to keep from new file
  cat.id.orig.nodup <- cat.id.orig[!V1 %in% dups]

# Combine not found with original data ------------------------------------
  df.final.big <- rbind(cat.id.orig.nodup, df.final)
  df.final.webid.big <- rbind(web.id.orig, df.final.web)
     
# Creating permutations for keywords --------------------------------------
#   my.ds <- df.final.big
  my.ds <- df.final.webid.big

  # Translate multiple white spaces to single
  keywords <- gsub(" +", " ", my.ds$V1)

  # split words by space
  splits <- strsplit(keywords, " ")

  # Create empty data table to store permutations
  my.dt <- data.table()

  for (i in 1:length(splits)){
    # Getting list of permutation words for each keyword
    perm <- permn(splits[[i]])
    
    # Merge words to make permute keywords
    merged.parts <- sapply(perm, function(x) paste(x, collapse=' '))

    # Make a data frame and add keyword column from orignal data
    merged.parts <- data.table("permut_keywords"= merged.parts)
    merged.parts$V1 <- my.ds$V1[i]
    
    # Add to final data table
    my.dt <- rbind(my.dt, merged.parts)
  }

  # Merge permute keywords with orinal data to get other columns
  my.dt.merge <- merge(my.dt, my.ds, by="V1", all.x=T)
  
  # Keep unique Permutations
  my.dt.merge <- unique(my.dt.merge, by="permut_keywords")
  
  # Removing orignal keyword column and ordering
  my.dt.merge <- my.dt.merge[, -1, with=F][order(permut_keywords)]

  # Export results in JP style format
#   write.table(my.dt.merge, file="keyword_category_jp_format_02272014_final.txt", quote = F, sep="\t", col.names=F, row.names= F)
  write.table(my.dt.merge, file="keyword_series_jp_format_02272014_final.txt", quote = F, sep="\t", col.names=F, row.names= F)

