##  This analysis was part of the MOSS outage where customers were required to be contacted after one full day of outage
##  Johnny provided 2 list to dedupe and get unique customer details, so no customer would be contacted more than once
#   Also provided end results to Jacob Oji in Marking

setwd("P:/Data_Analysis/Weblog_Data/")

require(xlsx)
require(data.table)

usa_logins <- as.data.table( read.xlsx("USA_LOGIN.XLSX", sheetIndex=1, encoding="UTF-8") )
usa_quotes <- as.data.table( read.xlsx("USA_QT.XLSX", sheetIndex=1, encoding="UTF-8") )


setkey(usa_logins, CUSTOMER_CODE, USER_CODE)
setkey(usa_quotes, CUSTOMER_CODE, USER_CODE)

usa_logins[duplicated(usa_logins)]
usa_quotes[duplicated(usa_quotes),]

usa_quotes <- unique(usa_quotes)
usa_logins <- unique(usa_logins)


# setkey(usa_logins, CUSTOMER_CODE)
# setkey(usa_quotes, CUSTOMER_CODE)

usa_logins[usa_quotes, nomatch=0]

write.xlsx(usa_logins, file = "USA_LOGINS_CLEAN.xlsx", row.names = FALSE, showNA=F)

usa_logins[TEL=="NULL" & EMAIL == "NULL"]
usa_quotes[TEL=="NULL" & EMAIL == "NULL"]



usa_logins[EMAIL == "NULL"]

usa_logins[duplicated(usa_logins$EMAIL)]
usa_quotes[duplicated(usa_quotes$EMAIL)]

usa_logins[EMAIL == "inquire@misumiusa.com"]

usa_logins[EMAIL == "masashi.2trj.takigawa@misumi.co.jp"]


usa_quotes[EMAIL == "inquire@misumiusa.com"]
