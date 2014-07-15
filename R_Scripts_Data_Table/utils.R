# ------------------------------------------------------------------------
# This script is collection of useful/handy functions from my earlier 
# development.
#
#  ------------------------------------------------------------------------


#
# the opposite of the %in% operator; aka not in.  this implementation was 
# shamelessly stolen from the help("%in%") documentation.
#
#    (1:10) %nin% c(3,7,12)
#
"%nin%" <- function(x, y) !x %in% y

#
# creates a valid csv that can be submitted to kaggle
#
create.submission <- function (data, file = "../red-swingline-predictions.csv") {
  
  # extract only the customer and predicted plan
  add.plan.hat (data)
  submission <- data [, list (customer_ID = customer.id, plan = plan.hat) ]
  
  # write to a csv file
  write.csv (submission, file, row.names = FALSE, quote = FALSE)
  
  return (normalizePath (file))
}

#
# read and combine the list of tab delimited files 
#
combined.tab.files <- rbindlist( lapply( fileNames, function(x) {
  xx <- fread(x, header = TRUE, sep = "\t", na.strings=c("NA", ''))
}))

#
# read and combine the list of comma delimited files 
#
combined.comma.files <- rbindlist( lapply( fileNames, function(x) {
  xx <- fread(x, header = TRUE, sep = ",", na.strings=c("NA", ''))
}))

#
# function to take last url part and selecting second column for just match value
#
extract.cat.id <- function(x) {
  m <- regexec(".*/(\\w+)/$", x)
  do.call(rbind, lapply(regmatches(x, m), `[`, c(2L)))
}


