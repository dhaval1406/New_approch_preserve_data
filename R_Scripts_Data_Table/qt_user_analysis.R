#  ------------------------------------------------------------------------
# This code is a fine piece of engineering
# It uses unsupervised learning results from hierarchical clustering here:
# P:/R/hierarchical_clustering_trends_data.R
# 
# The clustering algorithm clusters simlar customer together in 5 clusters. 
# Johnny asked to drill down cluster 4 further to get more deep restuls
# 
# Major challenging work is to plot multiple y-axis and different plots 
# in same panel, e.g. line chart and bar chart together

library(RODBC)
library(data.table)
library(ggplot2)
library(lubridate)

misumi_sql <- odbcConnect("misumi_sql", uid="AccessDB", pwd="")


# Get all tables under misumi_sql
xx <- sqlTables(misumi_sql)
xx[grep('qt', xx$TABLE_NAME, ignore.case=TRUE), ]

# Getting data from QT table
qt_columns <- sqlColumns(misumi_sql, "QT")
sqlQuery(misumi_sql, paste("select count(*) from QT"))

# Getting data from us_dtuser table
sqlColumns(misumi_sql, "us_dtuser")$COLUMN_NAME
sqlQuery(misumi_sql, paste("select count(*) from us_dtuser"))
grep("customer", sqlColumns(misumi_sql, "us_dtuser")$COLUMN_NAME, ignore.case=T, value=T)

# Reading data with required columns only
dt_user_clean <- as.data.table(sqlQuery(misumi_sql, paste("select CUSTOMER_CODE, CUSTOMER_NAME from us_dtuser")))

# sample_qt <- as.data.table(sqlQuery(misumi_sql, paste("select QTDate, CUSTOMER_CODE, MEDIA_CODE from QT where QTDate > '2013-12-01'")))
sample_qt <- as.data.table(sqlQuery(misumi_sql, paste("select QTDate, QUOTATION_NUMBER, CUSTOMER_CODE, MEDIA_CODE from QT ")))

# Quote numbers to exclude
qt_exclude <- as.data.table(sqlQuery(misumi_sql, paste("select * from QT_EXCLUDE")))

# Comparing POSIXct dates on dat.table
# sample_qt[QTDate < as.POSIXct("2014-02-05")]

#### Clean up data
# Exclude misumi related accounts
sample_qt <- sample_qt[!CUSTOMER_CODE %in% c('WOSMUS', '777777', '945330', 'w33036')]
sample_qt <- sample_qt[!QUOTATION_NUMBER %in% qt_exclude$QT_NUMBER]
# sample_qt <- qt_user_merge[!QTDate >= as.POSIXct("2014-03-01")]

# Unique records
dt_user_clean <- unique(dt_user_clean)

# Strip white spaces
sample_qt$MEDIA_CODE <- gsub("\\s", "", sample_qt$MEDIA_CODE)

setkey(sample_qt, CUSTOMER_CODE)
setkey(dt_user_clean, CUSTOMER_CODE)

# Merge Quote and User information together
qt_user_merge <- sample_qt[dt_user_clean, nomatch=0]

# Extract Month and Year from the date
# qt_user_merge[,  `:=` ( MONTH = month(QTDate), YEAR = year(QTDate))]

### Taking a subset for testing purpose
# qt_user_merge_orig <- qt_user_merge
# qt_user_merge <- qt_user_merge_orig[1:1000]

# Summerize
# qt_user_merge <- qt_user_merge[, list(med_count=.N), by=list(CUSTOMER_CODE, CUSTOMER_NAME, MEDIA_CODE, MONTH, YEAR)]

# Count Quotes by each media code per month
qt_user_merge <- qt_user_merge[, list(med_count=.N), by=list(CUSTOMER_CODE, CUSTOMER_NAME, 
                                                         MEDIA_CODE, MONTH = floor_date(QTDate, "month"))] 
# Calculate total Quotes by month
qt_user_merge[, total_qt:=sum(med_count, na.rm=T), by=list(CUSTOMER_CODE, CUSTOMER_NAME, MONTH)]
# Calculate ratio and round it  
qt_user_merge[, ratio:= round( (med_count/total_qt),3 ), by=list(CUSTOMER_CODE, CUSTOMER_NAME, MONTH)]

# Conver data format
qt_user_merge$CUSTOMER_CODE <- as.character(qt_user_merge$CUSTOMER_CODE)
qt_user_merge$CUSTOMER_NAME <- as.character(qt_user_merge$CUSTOMER_NAME)
qt_user_merge$MEDIA_CODE <- as.character(qt_user_merge$MEDIA_CODE)

# Update media code meanings
qt_user_merge$Media_Desc <- ""

qt_user_merge[MEDIA_CODE == "G1"]$Media_Desc <- "WOS"
qt_user_merge[MEDIA_CODE == "M1"]$Media_Desc <- "EMail"
qt_user_merge[MEDIA_CODE == "F1"]$Media_Desc <- "Fax"
qt_user_merge[MEDIA_CODE == "W1"]$Media_Desc <- "Web Portal"
qt_user_merge[MEDIA_CODE == "T1"]$Media_Desc <- "Phone"
qt_user_merge[MEDIA_CODE == "P1"]$Media_Desc <- "Mail"
qt_user_merge[MEDIA_CODE == "LP"]$Media_Desc <- "Local Purchase"
qt_user_merge[MEDIA_CODE == "V1"]$Media_Desc <- "Visit"

# Get list of customer code and names, to use in the for loop
cust_names_all <- unique(qt_user_merge[, 1:2, with=F], by=c("CUSTOMER_CODE", "CUSTOMER_NAME"))

### making a small pice to teste the graph
# cust_names_orig <- cust_names
# cust_names <- unique(qt_user_merge[, 1:2, with=F], by=c("CUSTOMER_CODE", "CUSTOMER_NAME"))
# 
# qt_user_merge_orig <- qt_user_merge
# qt_user_merge <- qt_user_merge[CUSTOMER_CODE %in% names(table(qt_user_merge[1:100]$CUSTOMER_CODE) >3) ]


# Excluding March'14 data as per Johnny
qt_user_merge <- qt_user_merge[!MONTH >= as.POSIXct("2014-03-01")]


# Function to pad missing month data  -------------------------------------

    pad_missing_months <- function(mydata){
        big.frame <- data.table()
        
        for(med_code in unique(mydata$MEDIA_CODE)){
              mydata.temp <- mydata[MEDIA_CODE==med_code][order(MONTH)]
              
              mydata.merge <- merge(mydata.temp, all.months, by="MONTH", all=T)
              mydata.merge$ratio[is.na(mydata.merge$ratio)] <- 0
              mydata.merge$CUSTOMER_CODE <- unique(mydata.temp$CUSTOMER_CODE)
              mydata.merge$Media_Desc <- unique(mydata.temp$Media_Desc)
              big.frame <- rbind(big.frame, mydata.merge)  
        }
        
        return(big.frame)
    }

# Read Customer Code Group from files -------------------------------------
library(gridExtra)
library(scales)
library(gtable)
library(RODBC)
library(data.table)
library(ggplot2)
library(reshape2)


# Original customer code files
# setwd("P:/Data_Analysis/QT_Analysis/CustomerCode/")

# Clustered customer code - After clusting algorithm applied
setwd("P:/Data_Analysis/QT_Analysis/Clustered_CustCode/")

fileNames <- list.files(path = getwd(), pattern = "*.txt")

lapply(fileNames, function(x){

        f.name = gsub(".txt", "", x)
        
        file1 <- fread(paste0(f.name,".txt"), header = TRUE, na.strings=c("NA", '')) 
        
        setnames(file1 ,"Customer_Code")
        
        cust_names <- cust_names_all[CUSTOMER_CODE %in% file1$Customer_Code]
        
        # 2 y axis - without ggplot ---------------------------------------------
        pdf(file= paste0("../plot_", f.name, ".pdf"), paper="letter", width=8, height=7)
        
        for(i in seq(nrow(cust_names))){
          mydata = qt_user_merge[CUSTOMER_CODE == cust_names[i, CUSTOMER_CODE]]
          
          if(length(unique(mydata$MONTH)) <= 1) next # Skip if there are 1 or less rows
        
        #     New Page 
              grid.newpage()
              
              # pad data for missing months
              mydata = pad_missing_months(mydata)

              #### Line chart with points
              p1 <- ggplot(data=mydata, aes(x=MONTH, y=ratio)) + 
                          geom_line(aes(color=Media_Desc), size=1.9) + 
                          geom_point(aes(color=Media_Desc), size=5) + 
                          # Making plot title with company code
                          ggtitle( paste0( cust_names[i, CUSTOMER_NAME], "(", cust_names[i, CUSTOMER_CODE], ")" ) ) +
                          ylab('Ratio') + ylim(c(0,1)) +
                          # Formatting date on x-axis for Month'Year format  
                          scale_x_datetime(labels = date_format("%b%y"), breaks = date_breaks("1 month")) +
                          theme(legend.position = 'bottom', 
                                panel.background = element_rect(fill = NA), 
                                plot.margin = unit(c(1, 2, 0.5, 0.5), 'cm'),
                                # Main plot heading 
                                plot.title = element_text(size = rel(1.4), vjust=2.5, face="bold"))
            
            #   Melting data to get  WOS/ALL QT
#               mydata1 <- mydata[Media_Desc == "WOS", -c(2,3, 7, 8), with = F]  
              mydata1 <- mydata[Media_Desc == "WOS", c("MONTH", "CUSTOMER_CODE", "med_count", "total_qt"), with = F]  
              mydata1 <- melt(mydata1, id=c("CUSTOMER_CODE", "MONTH"))      
              mydata1$value[is.na(mydata1$value)] <- 0
        
              #### Bar plot for WOS QT and All QT
              p2 <- ggplot(data=mydata1, aes(x=as.character(MONTH), y=value, fill=variable)) + 
                          # Bar with width 0.5, "bin" to have averaged number, wheat color and transparant  
                          geom_bar(width=0.4, stat="bin", alpha=0.5, position="dodge") + 
                          ylab('Total Quotes') +
                          scale_fill_hue(c=400, h=c(260,0),name="Quote Methods", labels=c("WOS QT", "All QT"))+
                          theme_bw() %+replace% 
                          theme(legend.position = 'bottom', 
                                panel.background = element_rect(fill = NA), 
                                plot.margin = unit(c(1, 2, 0.5, 0.5), 'cm'),
                                panel.grid.major = element_blank(),
                                panel.grid.minor = element_blank())
                                
            # extract gtable
            g1 <- ggplot_gtable(ggplot_build(p1))
            g2 <- ggplot_gtable(ggplot_build(p2))

            # overlap the panel of 2nd plot on that of 1st plot
            pp <- c(subset(g1$layout, name == "panel", se = t:r))
            g <- gtable_add_grob(g1, g2$grobs[[which(g2$layout$name == "panel")]], pp$t, 
                pp$l, pp$b, pp$l)

            ## adding guide of plot 2
            ia <- which(g2$layout$name == "guide-box")
            pos <- ia
            g$grobs[[pos]] <- cbind(g1$grobs[[pos]], g2$grobs[[pos]], size = 'first')
          
            # axis tweaks
            ia <- which(g2$layout$name == "axis-l")
            ga <- g2$grobs[[ia]]
            ax <- ga$children[[2]]
            ax$widths <- rev(ax$widths)
            ax$grobs <- rev(ax$grobs)
            ax$grobs[[1]]$x <- ax$grobs[[1]]$x - unit(1, "npc") + unit(0.15, "cm")
            g <- gtable_add_cols(g, g2$widths[g2$layout[ia, ]$l], length(g$widths) - 1)
            g <- gtable_add_grob(g, ax, pp$t, length(g$widths) - 1, pp$b)
            
            
            ### Adding lable on y-axis for the other side
            pp <- c(subset(g2$layout, name == 'ylab', se = t:r))
            
            ia <- which(g2$layout$name == "ylab")
            ga <- g2$grobs[[ia]]
            ga$rot <- 270
            ga$x <- ga$x - unit(1, "npc") + unit(1.0, "cm")
            
            g <- gtable_add_cols(g, g1$widths[g1$layout[ia,]$l], length(g$widths) - 1)
            g <- gtable_add_grob(g, ga, pp$t, length(g$widths) - 1, pp$b)
            g$layout$clip <- "off"
            
            # draw it
            grid.draw(g)
        }
        
        dev.off()
})
#  ------------------------------------------------------------------------
####### ONLY 1 Y-AXIS

# pdf(file='./myplot.pdf', paper="letter", width=8, height=7)

# # Plotting graphs for each Customer Name
# for(i in seq(nrow(cust_names))){
#   mydata = qt_user_merge[CUSTOMER_CODE == cust_names[i, CUSTOMER_CODE]]
#   
#   if(length(unique(mydata$MONTH)) <= 1) next # Skip if there are 1 or less rows
#   
#   my_graph <- ggplot(data=mydata, aes(x=MONTH, y=med_count)) + 
#               geom_line(aes(color=Media_Desc)) + 
#               geom_point(aes(color=Media_Desc)) + 
#               # Making plot title with company code
#               ggtitle( paste0( cust_names[i, CUSTOMER_NAME], "(", cust_names[i, CUSTOMER_CODE], ")" ) ) +
#               # Formatting date on x-axis for Month'Year format  
#               scale_x_datetime(labels = date_format("%b%y")) +
# #               guides(colour = guide_legend("title"), size = guide_legend("title"),shape = guide_legend("title")) +
#     
#               theme(panel.margin = unit(1, "lines"), 
#                     # Vertical x-axis
# #                     axis.text.x = element_text(hjust = 0.5),
#                     # Main plot heading 
#                     plot.title = element_text(size = rel(1.4), vjust=3.1, face="bold"))
# 
#   print(my_graph)
# 
# }
# 
# dev.off()

