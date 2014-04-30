require(xlsx)

# Setting up working directory
setwd("P:/Data_Analysis/Keyword_Analysis_Results_Gordana/Updated_after_phase1_Gordana/")

keyword.data <- read.xlsx("keyword_category-12032013.xls", sheetIndex=1, stringsAsFactors= F, encoding='UTF-8')

# keyword.data <- keyword.data.orig[keyword.data.orig$keyword %in% c("pully", "shaft"), ]

    keyword.data <- keyword.data[!is.na(keyword.data$category_code), ]
    keyword.data <- keyword.data[c(grep("\\?", keyword.data$keyword, ignore.case=T, invert=T)), ]

    keyword.data.main <- keyword.data[, c(1:3)]

    stemming.parts <- lapply(keyword.data$Stemming, strsplit, ",", perl=TRUE)
    synonyms.parts <- lapply(keyword.data$Synonims, strsplit, ",", perl=TRUE)
    

    generate_new_synonym_rows <- function(part_list){
    
        columns <- as.data.frame(do.call (rbind.fill.matrix, 
                                          lapply(part_list, function(x) do.call(rbind, x))))
    
        make_synonym_rows_list <- function(x) {
            
            my.list <-  lapply(1:ncol(columns), 
                              function(y){
                                  cbind(keyword = columns[x,y], keyword.data.main[x, -1])
                              })
    
            return(rbindlist(my.list))  
        }    
    
        synonym.list <- lapply( 1:nrow(keyword.data.main), make_synonym_rows_list)
        
        synonym.list.table <- rbindlist(synonym.list)
        synonym.list.table <- synonym.list.table[complete.cases(synonym.list.table), ]
        
        # Strip leading and trailing spaces
        synonym.list.table$keyword <- gsub("^\\s|\\s$", "", synonym.list.table$keyword)
        
        return(synonym.list.table)
    }


df1 <- generate_new_synonym_rows(stemming.parts)
df2 <- generate_new_synonym_rows(synonyms.parts)


df.final <- rbind(keyword.data.main, df1, df2)

# Keep the unique records
df.final <- unique(df.final)

# Replace/remove row.names columns with real row numbers
row.names(df.final) <- NULL

write.table(df.final, file="keyword_category_jp_format.txt", quote = F, sep="\t", col.names=F, row.names= F)

#####
#####
#### Take 2 - to check if therea are any duplicated keywords and 
#### if there are, extract them from the original data and provide to Gordana
#####
#####

# Setting up working directory
setwd("P:/Data_Analysis/Keyword_Analysis_Results_Gordana/Updated_after_phase1_Gordana/take2_1212/")

my.cat.data <-  read.delim('keyword_category_jp_format.txt', header = F, sep = "\t", quote = "", comment.char = "", na.strings=c("NA", ''))

my.cat.data <- unique (my.cat.data[order(my.cat.data$V1), ])

all_dups <- my.cat.data[my.cat.data$V1 %in% as.character(my.cat.data$V1[duplicated(my.cat.data$V1)]) , ]

all_no_dups <- my.cat.data[!my.cat.data$V1 %in% as.character(my.cat.data$V1[duplicated(my.cat.data$V1)]) , ]

write.csv(all_dups, "duplicate_keywords_category.csv", na = '', row.names = FALSE)


###
###  Take 3 - Gordana cleaned and provided dups file
###           So merging it with all_no_dups to provide unique list to Johnny
my.cat.data.fixed <-  read.csv('duplicate_keywords_category_fixed.csv', header=F, , quote = "\"", comment.char = "", na.strings=c("NA", ''), , stringsAsFactors = F)

# Combining non duplicate records with Gordana's unique records
my.new.cat.data <- rbind(all_no_dups, my.cat.data.fixed)


write.csv(my.new.cat.data, "updated_keywords_category_12162013.csv", na = '', row.names = FALSE)

write.table(my.new.cat.data, file="keyword_category_jp_format_12162013.txt", quote = F, sep="\t", col.names=F, row.names= F)