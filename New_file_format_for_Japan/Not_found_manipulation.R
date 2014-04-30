
setwd("P:/Data_Analysis/Keyword_Analysis_Results_Gordana")

rm(list=ls())
gc()

not.found.data <- read.csv("Not_found_kwrds.csv", header=T, , quote = "\"", comment.char = "", na.strings=c("NA", ''), stringsAsFactors = F)
# not.found.data <- not.found.data[1:15, ]

# Add No. field to make it consistent with Gordana's file
not.found.data$No. <- 1:nrow(not.found.data)

# Split Category_Id column for new line character.
my.list <- strsplit(not.found.data[, 2], "\n")

# Splits Category Id into different rows
rep_times <- vapply(my.list, FUN=length, FUN.VALUE=integer(1))

# Data frame with correct repetition of keywords and order number
# By splitting line seperated field into seperate rows
new.df <- data.frame(Category_Id=unlist(my.list), Q=rep(not.found.data$Q, rep_times), No. = rep(not.found.data$No., rep_times ) )

# Remove the rows with has http in them
keyword.data <- unique (new.df[-c(grep("http|not found|product match|customer|tel|inquire", new.df$Category_Id, ignore.case=T)), ])

keyword.cat.merge <- aggregate.data.frame(keyword.data$Category_Id, by=list(keyword.data$Q, keyword.data$No.), paste)

keyword.cat.merge$x <- sapply(keyword.cat.merge$x, paste, collapse=",")

keyword.cat.merge$jpCode <- 2

final.data <- keyword.cat.merge[, -2]

# write.table(final.data, file="keyword_cat_jp_format.txt", quote = F, sep="\t", col.names=F, row.names= F)


