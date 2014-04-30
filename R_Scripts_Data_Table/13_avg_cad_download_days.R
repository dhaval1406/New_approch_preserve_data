#
# This analysis is to calculate average CAD download days per month
# Data source is MS SQL Server and table is `us_dtcadaccesslog_str`
# The goal is to create similar structs as Google Analytics's `Frequency & Recency`


# Trying to capture runtime
begTime <- Sys.time()

#Getting the required package
require(data.table)
require(RODBC)
require(xlsx)
# Connect to SQL servers ODBC connction
misumi_sql <- odbcConnect("misumi_sql", uid="AccessDB", pwd="")

cad_access <- sqlQuery(misumi_sql, paste("select ACCESS_TIME, CUSTOMER_CODE from us_dtcadaccesslog_str"))
cad_access <- data.table(cad_access, key="CUSTOMER_CODE")

# removing internal accounts
cad_access <- cad_access [!CUSTOMER_CODE == "WOSMUS"]

# sort by customer code, date
cad_access <- cad_access[order(CUSTOMER_CODE, ACCESS_TIME)]

# convert customer code to character class
cad_access[,`:=` ( CUSTOMER_CODE=as.character(CUSTOMER_CODE),
                   ACCESS_TIME=as.Date(ACCESS_TIME) )]

#
# From 04/01/2012 - using all data and much better way
#
cad_access_2012 <- cad_access[ACCESS_TIME >= "2012-04-01" & ACCESS_TIME < "2014-04-01"]

# Use below if you don't want to count same day visits
# cad_access_2012 <- unique(cad_access_2012, by=names(cad_access_2012))

cad_access_2012$dayssincelast<-unlist(by(cad_access_2012$ACCESS_TIME,cad_access_2012$CUSTOMER_CODE,function(x) c(NA,diff(x))))

# Assign 500 days to NA values, assuming that they haven't downloaded CAD since that many days
cad_access_2012$dayssincelast[which(is.na(cad_access_2012$dayssincelast)=="TRUE")] <- 500

# Format date to month year format, so we can combine results by month
cad_access_2012$ACCESS_TIME <- format(cad_access_2012$ACCESS_TIME, "%Y-%m")

# Sorting by date
cad_access_2012 <- cad_access_2012[order(ACCESS_TIME)]

# Making ranges similar to Google Analystics
cad_access_2012$dayssincelast_cat <- as.character(cad_access_2012$dayssincelast)
cad_access_2012$sort_order <- cad_access_2012$dayssincelast

cad_access_2012[dayssincelast>=8  & dayssincelast<=14,  `:=`(dayssincelast_cat = '8-14',    sort_order = 8)]
cad_access_2012[dayssincelast>=15  & dayssincelast<=30,  `:=`(dayssincelast_cat = '15-30',   sort_order = 9)]
cad_access_2012[dayssincelast>=31  & dayssincelast<=60,  `:=`(dayssincelast_cat = '31-60',   sort_order = 10)]
cad_access_2012[dayssincelast>=61  & dayssincelast<=120, `:=`(dayssincelast_cat = '61-120',  sort_order = 11)]
cad_access_2012[dayssincelast>=121 & dayssincelast<=364, `:=`(dayssincelast_cat = '121-364', sort_order = 12)]
cad_access_2012[dayssincelast>=365 ,                     `:=`(dayssincelast_cat = '365+',    sort_order = 13)]

# Summarizing total visits by days since last visits  
cad_access_2012_sum <- cad_access_2012[, list(total_visits = .N), 
                                       by=list(ACCESS_TIME, dayssincelast_cat, sort_order)] [order(ACCESS_TIME, sort_order)] 

cad_access_2012_sum$sort_order <- NULL

analysis_file = paste0("P:/Data_Analysis/Analysis_Results/13_avg_cad_download_days.xlsx" )
      
write.xlsx(cad_access_2012_sum, file = analysis_file, row.names = FALSE)



#
# Plotting adventure
#

require(ggplot2)
require(grid)

pdf(file='P:/Data_Analysis/Analysis_Results/13_avg_cad_download_days.pdf', paper="USr", width=11, height=8)

cad_access_2012_sum$dayssincelast_cat <- factor(cad_access_2012_sum$dayssincelast_cat, levels=unique(cad_access_2012_sum$dayssincelast_cat), ordered=TRUE)

g <- ggplot(cad_access_2012_sum, 
            aes(x=dayssincelast_cat, y=total_visits, fill=ACCESS_TIME)) +
     geom_bar(color="black", stat="identity") +
     facet_wrap(~ ACCESS_TIME)+
     xlab("Days Since Last Download") +
     ylab("Total CAD Downloads") +
     # no legend required
     guides(fill=FALSE)+
     
    theme(panel.margin = unit(1, "lines"),
            # Vertical x-axis
            axis.text.x = element_text(angle = 90, hjust = 0.5),
            # Main plot heading
            plot.title = element_text(size = rel(1.4), vjust=1, face="bold"),
            # Facet elements titles
            strip.text.x = element_text(size = 11, hjust = 0.5, vjust = 0.5, face = 'bold'))
  
print(g)

dev.off()



