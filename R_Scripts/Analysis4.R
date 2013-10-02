# List the files and removing everything
rm(list = ls())
gc() #it will free memory

#Getting the required package
require(plyr)

#Set working directory
#  setwd("P:/TempData_06042013/")
setwd("P:/R/New_approch_preserve_data/")
load("Analysis_3.RData")

# Trying to capture runtime
begTime <- Sys.time()

# Pre req : Using R data from Analysis 3. Analysis 3 code must be run uptil here.
keyword_search_counts_anl_4 <- ddply(url_parts_merge, .(Q, cat_name_1, cat_name_2, cat_name_3, cat_name_4, cat_name_5, total_search, sum_link, sum_linkCtg), summarise, 
                                   LinkCtg = sum(LinkCtg), 
                                   total_codefix_linkCtg = sum(codefix_count, na.rm = T)
                                  )

# Finding coversion rate, totalLinkCtg / total_search
keyword_search_counts_anl_4$ConvRate <-  keyword_search_counts_anl_4$LinkCtg / keyword_search_counts_anl_4$total_search

# Sorting the data based on total search (desc), keywords and number of link categories(desc)
keyword_search_counts_anl_4 <- keyword_search_counts_anl_4[order(-keyword_search_counts_anl_4$total_search, 
                                                                 keyword_search_counts_anl_4$Q, 
                                                                 -keyword_search_counts_anl_4$LinkCtg),  ]

write.csv(keyword_search_counts_anl_4, file = "4_keyword_category.csv", na = '', row.names = FALSE)

runTime <- Sys.time()-begTime 

runTime

# save image of R data for further use
#save.image("Analysis_3_take3.RData")

#=========================================================== Test the results =============================================

# subset(keyword_search_counts_anl1, Q %in% c('fep', 'lead screw', ''))
# 
# blank1 <- subset(search_log, Q %in% c(''))
# 
# blank2 <- search_log[!complete.cases(search_log$Q),]
# 
# #merge(x, df1, all.x=TRUE, by.x="Site.1", by.y="Sites", sort=FALSE)
# 
# ### creating a function in ddply - may be used later on to create a function
# #ddply(sub_search, .(Q), function(x) {
# #  x$new.var <- ifelse(x$Status == 'Link', 'My status is link', 'My status is not link')
# #  return(x)
# #})
# 
