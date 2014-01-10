# List the files and removing everything
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(data.table)

#Set working directory
setwd("P:/Data_Analysis/Analysis_Results/")

# Load already processed data
load("P:/Data_Analysis/Processed_R_Datasets/Analysis_3.RData")

# Trying to capture runtime
begTime <- Sys.time()

# Pre req : Using R data from Analysis 3. Analysis 3 code must be run uptil here.
#============================================== Main Algorithem =======================================

# Summarize based on Link Categories
# Finding coversion rate, totalLinkCtg / total_search
# Sorting the data based on total search (desc), keywords and number of link categories(desc)

keyword_search_counts_anl_4 <- url_parts_merge[, list(LinkCtg=sum(LinkCtg), 
                                                      total_codefix_linkCtg = sum(codefix_count, na.rm = T)),
                                                 by=list(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, total_search, sum_link, sum_linkCtg)
                                               
                                               ] [, ConvRate := LinkCtg/total_search ] [ order(-total_search, Q, -LinkCtg) ];

# Format csv name
analysis_file = format_name("4_keyword_category");

# Exporting results in csv
write.csv(keyword_search_counts_anl_4, file = analysis_file, na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 
runTime
#=========================================================== Test the results =============================================
