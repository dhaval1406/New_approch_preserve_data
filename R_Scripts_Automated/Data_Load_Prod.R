# List the files and removing everything
rm(list = ls())
gc() #it will free memory

# Trying to capture runtime
begTime <- Sys.time()

# Getting file names from command line arguments
args <- commandArgs(trailingOnly = TRUE)

search_log_file <- args[1]
detailtab_log_file <- args[2]
codefix_log_file <- args[3]

rm(args)

#Getting the required package
require(plyr)

#Set working directory
setwd("P:/Data_Analysis/Weblog_Data/")

#Importing data
search_log    <- read.delim(search_log_file, header = TRUE, sep = "\t", quote = "", comment.char = "", na.strings=c("NA", ''))
detailTab_log <- read.delim(detailtab_log_file, header = TRUE, sep = "\t", quote = "", comment.char = "", na.strings=c("NA", ''))
codeFix_log   <- read.delim(codefix_log_file, header = TRUE, sep = "\t", quote = "", comment.char = "", na.strings=c("NA", ''))
category      <- read.csv('category.csv', header = FALSE, sep = ",", quote = "\"", comment.char = "", na.strings=c("NA", ''))

#clean up data
search_log <- search_log[c("AccessDateTime", "SessionId", "Q", "SEARCH_TYPE", "Status", "URL", "Link_URL", "SERIES_CODE", "CUSTCD")]
	# Link_URL is used in Analysis # 3
	#search_log <- search_log[c("AccessDateTime", "SessionId", "Q", "SEARCH_TYPE", "Status", "Link_URL", "SERIES_CODE", "CUSTCD")]
codeFix_log   <- codeFix_log[c("SessionId","PRODUCT_CODE", "SERIES_CODE","Referer", "CUSTCD")]
detailTab_log <- detailTab_log[c("SessionId","Tab_Name", "SERIES_CODE", "CUSTCD")]
# first two columns are not used
category <- category[, - c(1:2, 13)]
# Assigning column names
colnames(category) <- c("cat_id_1", "cat_name_1","cat_id_2", "cat_name_2","cat_id_3", "cat_name_3","cat_id_4", "cat_name_4","cat_id_5", "cat_name_5")

# removing blank series codes and conver  keywords to lower case and 
# removing internal acccounts with CUSTCD = "WOSMUS"
search_log <- subset(search_log, subset = ( !is.na(Q) & (!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))) ), select = -CUSTCD )
search_log$Q <- tolower(search_log$Q)

codeFix_log <- subset(codeFix_log, subset = (!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))), select = -CUSTCD)
	# For analysis # 3, non blank Referer is used
	#codeFix_log <- subset(codeFix_log, subset = ( !is.na(Referer) & (!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))) ), select = -CUSTCD)
detailTab_log <- subset(detailTab_log, subset = (!(CUSTCD == "WOSMUS") | (is.na(CUSTCD))), select = -CUSTCD)

# Normalizing keywords by stemming, currently only stemming plurals e.g. words that end with `s`
search_log$Q <- gsub("(.+[^s])s$", "\\1", search_log$Q)

### summarize total keyword searches - search_log[1:100,]
total_keyword_search <- ddply(search_log, .(Q), summarise, 
                              sum_link = sum(Status=='Link'), 
                              sum_linkCtg = sum(Status=='LinkCtg'), 
                              total_search = sum (sum(Status=='NotFound'), (Status=='Hit'))
							)

# total_codefix <- ddply(codeFix_log, .(SessionId, Referer), summarise, 
#                        #codefix_count = sum(Referer != ''), 
#                        #codefix_count = length(unique(c(SessionId)))
#                        codefix_count = length(unique(Referer))
# 					            )
# 
# # for Analysis # 3	
# total_codefix_anl3 <- ddply(codeFix_log, .(SessionId, Referer, SERIES_CODE), summarise, 
#             					 codefix_count = length(unique(PRODUCT_CODE))
#                       )
# 		

total_detailtab<- ddply(detailTab_log, .(SessionId, SERIES_CODE), summarise, 
                       #codefix_count = length(unique(c(SessionId)))x
                       tab_count = length(unique(Tab_Name))
)
		
# Merge total_keyword_search with search 
# keyword_search_merge <- merge(search_log, total_keyword_search, all.x=TRUE, by="Q", sort=FALSE)
# codefix_merge <- merge(codeFix_log, total_codefix, all.x=TRUE, by=c("SessionId", "Referer"), sort=FALSE)

# Function to format resultant CSV - generated during different analysis
format_name <- function(x){
  time.stamp = format(Sys.time(), "%m%d%Y")
  file.name = paste(x, sprintf("%s.csv", time.stamp), sep="_")
  return(file.name)
}

# save image of R data for further use
save.image("P:/Data_Analysis/Processed_R_Datasets/Data_Load_Prod.RData")

runTime <- Sys.time()-begTime 
runTime

# ========================================== Test Area =================================================

