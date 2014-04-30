
setwd("P:/Data_Analysis/Keyword_Analysis_Results_Gordana")

keyword.data <- read.csv("Keywords_analysis_categories.csv", header=T, , quote = "\"", comment.char = "", na.strings=c("NA", ''), stringsAsFactors = F)

keyword.data <- unique(keyword.data[, c("No.", "Q", "Category_Id")])

keyword.cat.merge <- aggregate.data.frame(keyword.data$Category_Id, by=list(keyword.data$Q, keyword.data$No.), paste)

keyword.cat.merge$x <- sapply(keyword.cat.merge$x, paste, collapse=",")

keyword.cat.merge$jpCode <- 2

final.data <- keyword.cat.merge[, -2]

write.table(final.data, file="keyword_cat_jp_format.txt", quote = F, sep="\t", col.names=F, row.names= F)


###### working piece to combine category ids for each unique keyword
new.dat <- aggregate.data.frame(dat$Cat, by=list(dat$Q), paste)

new.dat$x <- sapply(new.dat$x, paste, collapse=",")

write.table(new.dat, file="test_tab_comma.txt", sep="\t", col.names=F, row.names= F, quote = F)

str(keyword.data)
xx <- keyword.data[keyword.data$Q == "flat head", ]