## When running conversion ratios, requisted by Kashi as part of KPI, 
## I found mismatch in Visit Logs and CodeFix logs after Feb'14
## So drilling deeper and try to present case to Japan on mismatching
## for further investigation.

require(data.table)
require(bit64)

setwd("P:/Data_Analysis/Weblog_Data/")


v1 <- fread("visit_log_20140331_20140406.txt", header = TRUE, sep = "\t", na.strings=c("NA", '')) 
c1 <- fread("codefix_log_20140331_20140406.txt", header = TRUE, sep = "\t", na.strings=c("NA", '')) 


v1 <- v1[!is.na(SessionId)]
c1 <- c1[!is.na(SessionId)]

v1[, AccessDateTime:=as.POSIXct(AccessDateTime)]  
c1[, AccessDateTime:=as.POSIXct(AccessDateTime)]  
  
# setkey(v1, AccessDateTime, SessionId)
# setkey(c1, AccessDateTime, SessionId)

setkey(v1, SessionId, CookieId)
setkey(c1, SessionId, CookieId)

v1[c1, nomatch=0, allow.cartesian=T]

v1.c1.merge <- v1[c1, nomatch=0, allow.cartesian=T]


v1[CookieId=="84abc43f-e4a6-45b9-9ad2-783a83659af4"]
c1[CookieId=="84abc43f-e4a6-45b9-9ad2-783a83659af4"]

c1[CUSTCD=="W93082"]
v1[CUSTCD=="W93082"]

c1[CookieId=="6cc69682-0f50-4a2d-bc27-ef731ae590e8"]
v1[CookieId=="6cc69682-0f50-4a2d-bc27-ef731ae590e8"]

nrow(v1)
nrow(c1)

c1.1 <- c1[20000:20050][, 1:5, with=F]

### found 1 match 
v1[USER_CODE %in% na.omit(c1.1$USER_CODE)]
c1[USER_CODE == 155084]
v1[USER_CODE == 155084]

# No match
unique(na.omit(c1.1$CUSTCD))
v1[CUSTCD %in% na.omit(c1.1$CUSTCD)]

# No  match
unique(c1.1$CookieId)
v1[CookieId %in% c1.1$CookieId]

# No match
unique(c1.1$SessionId)
v1[SessionId %in% c1.1$SessionId]

