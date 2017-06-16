
library("RSQLite")
library("lubridate")
library("tidyr")
library("stringr")


# Database file paths
police <- "O:/AllUsers/CovStat/Data Portal/repository/Data/Database Files/Police.db"
development <- "O:/AllUsers/CovStat/Data Portal/repository/Data/Database Files/Development.db"
fire <- "O:/AllUsers/CovStat/Data Portal/repository/Data/Database Files/Fire.db"
waste <- "O:/AllUsers/CovStat/Data Portal/repository/Data/Database Files/SolideWaste.db"
# Export Locations


# DB pull to place in separate file for OpenData portal

######################### Public Safety ###################################### 
# Police Runs -----------------------------------------------------------------------------------------------
cons.police <- dbConnect(drv=RSQLite::SQLite(), dbname = police)
pruns <- dbGetQuery(cons.police, 'select * from PoliceRuns')
dbDisconnect(cons.police)

pruns$Address <- trimws(x = pruns$Address)
pruns$Address <- gsub(pattern = "^\\d{1,5}\\s|Apt.\\s\\d*|Apt|APT|\\-|[[:lower:]]", replacement = "", x = pruns$Address)

names(pruns)[1] <- "Incident#"
names(pruns)[2] <- "Date_Time"
names(pruns)[4] <- "Incident_Type"
names(pruns)[10] <- "Gun_Reported"
names(pruns)[11] <- "Neighborhood"
#names(pruns)[14] <- "Longitude"
#names(pruns)[15] <- "Latitude"

pruns$Neighborhood[pruns$Neighborhood == "CENTRAL BUSINESS DISTRICT"] <- "Central Business District"

#Correcting for 2013 codings
suspicious <- c("Suspicious Person", "Suspicious Activity")
susp_match <- pruns$Incident_Type %in% suspicious
pruns$Incident_Type[susp_match] <- "Suspicious Person/Vehicle"

pruns <- pruns[, c(1, 2, 5, 4, 6, 7, 9, 10, 11, 13)]

#Offset latitdue and longitude
#offset_crds <- function(df, lat, lon, scientific = FALSE){
  #Position, decimal degrees

  #Earth's radius, sphere
  #R <- 6378137

  #offsets in meters
  #dn <- 20
  #de <- 20

  #Coordinate offsets in radians
  #dLat <- dn/R
  #dLon <- de/(R*cos(pi*lat/180))

  #OffsetPosition, decimal degrees
  #df$lat <- as.numeric(lat + dLat * 180/pi)
  #df$lon <- as.numeric(lon + dLon * 180/pi)
  
  #if(!scientific) {
    #df$lat <- format(df$lat, scientific=FALSE)
   # df$lon <- format(df$lon, scientific=FALSE)
 #}
  
  #df$Latitude <- df$lat
  #df$Longitude <- df$lon
  
  #df$lat <- NULL
  #df$lon <- NULL
  #return(df)
#}

#pruns <- offset_crds(pruns, pruns$Latitude, pruns$Longitude)


# Write file for dashboard 
write_multiple <- function(x, file){
  write.csv(x, file, row.names = FALSE)
}
write_multiple(pruns, "U:/CityWide Performance/CovStat/CovStat Projects/Police/Tableau Files/OpenData_PoliceRuns.csv")

# Create directory for exporting files and set as working directory
policeDir <- "C:/Users/tsink/Google Drive/OpenData Files/Police/Runs"

if(!dir.exists(policeDir)){
  dir.create(policeDir, showWarnings = TRUE)
  setwd(policeDir)
} else {
  setwd(policeDir)
}

pruns2017 <- pruns[pruns$Date >= as.Date("2017-01-01"),]
#pruns2016 <- pruns[pruns$Date >= as.Date("2016-01-01") & pruns$Date < as.Date("2017-01-01"),]
#pruns2015 <- pruns[pruns$Date >= as.Date("2015-01-01") & pruns$Date < as.Date("2016-01-01"),]
#pruns2014 <- pruns[pruns$Date >= as.Date("2014-01-01") & pruns$Date < as.Date("2015-01-01"),]
#pruns2013 <- pruns[pruns$Date >= as.Date("2013-01-01") & pruns$Date < as.Date("2014-01-01"),]

write_multiple(pruns2017, file="pruns2017.csv")
#write_multiple(pruns2016, file="pruns2016.csv")
#write_multiple(pruns2015, file="pruns2015.csv")
#write_multiple(pruns2014, file="pruns2014.csv")
#write_multiple(pruns2013, file="pruns2013.csv")

#write.csv(pruns, file="C:/Users/tsink/Google Drive/OpenData Files/Police/PoliceRuns.csv", row.names = FALSE)

# Arrests -------------------------------------------------------------------------------------------------------
cons.police <- dbConnect(drv=RSQLite::SQLite(), dbname=police)

parrest <- dbGetQuery(cons.police, 'select * from Arrests')
dbDisconnect(cons.police)

parrest$Location <- trimws(x = parrest$Location)
parrest$Location <- gsub(pattern = "^\\d{1,5}\\s|Apt.\\s\\d*|Apt|APT|\\-|[[:lower:]]|Intersection|\\:|[0-9]*$|\\#|[0-9][[:upper:]]+$", 
                         replacement = "", 
                         x = parrest$Location)
parrest$Location <- trimws(x = parrest$Location)
parrest <- within(parrest, {
  Date <- strptime(Date, format = "%m/%d/%Y %H:%M")
})
parrest$Date <- as.character(parrest$Date)
parrest <- na.omit(parrest)

#parrest <- parrest[order(parrest$Name, parrest$Date),]
#parrest <- transform(parrest, id=match(Name, unique(parrest$Name)))

# Assign 'id' to represent number of arrests
parrestAgg <- aggregate(Count~Date+Name, parrest, sum)
parrestAgg$id <- 1:nrow(parrestAgg)
parrestAgg$Count <- NULL
parrest <- left_join(parrest, parrestAgg, by = c("Date" = "Date", "Name" = "Name"))


parrest <- parrest[, c(14, 2, 4, 5, 6, 7, 9, 11)] 
names(parrest)[1] <- "ID"
names(parrest)[2] <- "Date_Time"
names(parrest)[4:7] <- c("Charge", "DUI_Charge", "Gun_Charge", "Neighborhood")


parrest$Neighborhood[parrest$Neighborhood == "CENTRAL BUSINESS DISTRICT"] <- "Central Business District"


# Write file for dashboard 
write_multiple <- function(x, file){
  write.csv(x, file, row.names = FALSE)
}
write_multiple(parrest, "U:/CityWide Performance/CovStat/CovStat Projects/Police/Tableau Files/OpenData_Arrests.csv")

# Create directory for exporting files and set as working directory
arrestDir <- "C:/Users/tsink/Google Drive/OpenData Files/Police/Arrests"


if(!dir.exists(arrestDir)){
  dir.create(arrestDir, showWarnings = TRUE)
  setwd(arrestDir)
} else {
  setwd(arrestDir)
}

parrest2017 <- parrest[parrest$Date_Time >= as.Date("2017-01-01"),]
parrest2017 <- na.omit(parrest2017)

#parrest2016 <- parrest[parrest$Date_Time >= as.Date("2016-01-01") & parrest$Date_Time < as.Date("2017-01-01"),]
#parrest2016 <- na.omit(parrest2016)
#parrest2015 <- parrest[parrest$Date_Time >= as.Date("2015-01-01") & parrest$Date_Time < as.Date("2016-01-01"),]
#parrest2015 <- na.omit(parrest2015)
#parrest2014 <- parrest[parrest$Date_Time >= as.Date("2014-01-01") & parrest$Date_Time < as.Date("2015-01-01"),]
#parrest2014 <- na.omit(parrest2014)
#parrest2013 <- parrest[parrest$Date_Time >= as.Date("2013-01-01") & parrest$Date_Time < as.Date("2014-01-01"),]
#parrest2013 <- na.omit(parrest2013)

write_multiple <- function(x, file){
  write.csv(x, file, row.names = FALSE)
}
write_multiple(parrest2017, "Arrests2017.csv")
#write_multiple(parrest2016, "Arrests2016.csv")
#write_multiple(parrest2015, "Arrests2015.csv")
#write_multiple(parrest2014, "Arrests2014.csv")
#write_multiple(parrest2013, "Arrests2013.csv")



# Park Safety ---------------------------------------------------------------------------------------
cons.development <- dbConnect(drv=RSQLite::SQLite(), dbname = development)
safety <- dbGetQuery(cons.development, 'select * from ParkSafety')
dbDisconnect(cons.development)

safety$Address <- trimws(x = safety$Address)
safety$Address <- gsub(pattern = "^\\d{1,5}\\s|Apt.\\s\\d*|Apt|APT|\\-|[[:lower:]]", replacement = "", x = safety$Address)

safety <- safety[, c(1, 2, 5, 4, 6, 7, 9, 10, 11, 13, 14)]
names(safety)[1]  <- "Incident#"
names(safety)[2]  <- "Date_Time"
names(safety)[4] <- "Incident Type"
names(safety)[8:9] <- c("Gun_Reported", "Neighborhood")

write_multiple <- function(x, file){
  write.csv(x, file, row.names = FALSE)
}
write_multiple(safety, "U:/CityWide Performance/CovStat/CovStat Projects/Police/Tableau Files/OpenData_ParkSafety.csv")

# Create directory for exporting files and set as working directory
parkSafetyDir <- "C:/Users/tsink/Google Drive/OpenData Files/Police/ParkSafety"

if(!dir.exists(parkSafetyDir)){
  dir.create(parkSafetyDir, showWarnings = TRUE)
  setwd(parkSafetyDir)
} else {
  setwd(parkSafetyDir)
}

safety2017 <- safety[safety$Date >= as.Date("2017-01-01"),]
#safety2016 <- safety[safety$Date >= as.Date("2016-01-01") & safety$Date < as.Date("2017-01-01"),]
#safety2015 <- safety[safety$Date >= as.Date("2015-01-01") & safety$Date < as.Date("2016-01-01"),]
#safety2014 <- safety[safety$Date >= as.Date("2014-01-01") & safety$Date < as.Date("2015-01-01"),]
#safety2013 <- safety[safety$Date >= as.Date("2013-01-01") & safety$Date < as.Date("2014-01-01"),]


write_multiple(safety2017, "park_safety2017.csv")
#write_multiple(safety2016, "park_safety2016.csv")
#write_multiple(safety2015, "park_safety2015.csv")
#write_multiple(safety2014, "park_safety2014.csv")
#write_multiple(safety2013, "park_safety2013.csv")


# Part I/II Crimes ----------------------------------------------------------------------------------
cons.police <- dbConnect(drv=RSQLite::SQLite(), dbname = police)
clearance <- dbGetQuery(cons.police, 'select * from ClearanceRates')
dbDisconnect(cons.police)

types <- toupper(c("aggravated assault", "arson", "auto theft", "breaking and entering", "forcible rape", 
                 "larceny-theft (except auto theft)", "murder and non-negligent manslaughter", "robbery"))

types_match <- clearance$Charge %in% types
clearance$Type[types_match] <- "Part I"
clearance$Type[!types_match] <- "Part II"

clearance <- clearance[, c(7, 5, 6, 10, 3, 4, 2, 1, 9)]
names(clearance)[6:9] <- c("Charge_Code", "Crimes", "Clearance_Rate", "Crimes_Cleared")
clearance$Charge <- str_trim(clearance$Charge, side = c("both"))


write.csv(clearance, "U:/CityWide Performance/CovStat/CovStat Projects/Police/Tableau Files/OpenData_Crimes.csv", row.names = FALSE)
write.csv(clearance, "C:/Users/tsink/Google Drive/OpenData Files/Police/Crimes/PartI&IICrimes.csv", row.names = FALSE)

# Fire Department ------------------------------------------------------------------------------------------------------
# Fire Runs ------------------------------
cons.fire <- dbConnect(drv=RSQLite::SQLite(), dbname = fire)
fire_runs <- dbGetQuery(cons.fire, 'select * from FireRuns')
dbDisconnect(cons.fire)

f_columns <- c(2, 6, 8, 9, 10, 49, 50, 5, 51, 53, 37, 18)
fire_runs <- fire_runs[f_columns]
fire_runs <- unite(fire_runs, Address, st_prefix, street, st_type)
#fire_runs$alm_date <- dmy(fire_runs$alm_date)

replace_string <- function(df){
    df <- str_replace(df, "^\\_", "")
    df <- str_replace(df, "[[:punct:]]", " ")
    df <- str_replace(df, "\\_", " ")
}
fire_runs$Address <- replace_string(fire_runs$Address)
fire_runs$Address <- str_trim(fire_runs$Address, side = c("both"))
#Keep only Fire & Rescue realted calls.  Drop EMS and medical related.
drop <- grepl(pattern = "Ambulance|EMS incident|Medical assist|EMS Call|(EMS)", x = fire_runs$S_code)
#drop_match <- fire_runs$S_code %in% drop
fire_runs <- fire_runs[!drop,]

names(fire_runs) <- c("Incident#", "Date", "Address","Incident_Type", "Incident_Description", "Run_Description",
                       "Neighborhood", "Sector", "Notification_Time", "Arrival_Time")

#To get response times here
fire_runs$Response_Time_Mins <- as.numeric(hms(fire_runs$Arrival_Time) - hms(fire_runs$Notification_Time))/60
fire_runs$Response_Time_Mins <- round(fire_runs$Response_Time_Mins, 2)

fire_runs$Neighborhood[fire_runs$Neighborhood == "WALLACE WOODS"] <- "Wallace Woods"
fire_runs$Neighborhood[fire_runs$Neighborhood == "CENTRAL BUSINESS DISTRICT"] <- "Central Business District"
fire_runs$Neighborhood[fire_runs$Neighborhood == "PEASELBURG"] <- "Peaselburg"
fire_runs$Neighborhood[fire_runs$Neighborhood == "KUHRS LANE"] <- "Kuhrs Lane"

write_multiple <- function(x, file){
  write.csv(x, file, row.names = FALSE)
}
write_multiple(fire_runs, "U:/CityWide Performance/CovStat/CovStat Projects/Fire/TableauFiles/OpenData_Fire_Rescue.csv")

# Create directory for exporting files and set as working directory
fireRunsDir <- "C:/Users/tsink/Google Drive/OpenData Files/Fire/FireRuns"

if(!dir.exists(fireRunsDir)){
  dir.create(fireRunsDir, showWarnings = TRUE)
  setwd(fireRunsDir)
} else {
  setwd(fireRunsDir)
}

fire_runs2017 <- fire_runs[fire_runs$Date >= as.Date("2017-01-01"),]
#fire_runs2016 <- fire_runs[fire_runs$Date >= as.Date("2016-01-01") & fire_runs$Date < as.Date("2017-01-01"),]
#fire_runs2015 <- fire_runs[fire_runs$Date >= as.Date("2015-01-01") & fire_runs$Date < as.Date("2016-01-01"),]
#fire_runs2014 <- fire_runs[fire_runs$Date >= as.Date("2014-01-01") & fire_runs$Date < as.Date("2015-01-01"),]

write_multiple(fire_runs2017, "fire_rescue2017.csv")
#write_multiple(fire_runs2016, "fire_rescue2016.csv")
#write_multiple(fire_runs2015, "fire_rescue2015.csv")
#write_multiple(fire_runs2014, "fire_rescue2014.csv")

# Ambulance Runs --------------------------------------------------------------------------------------------
cons.fire <- dbConnect(drv = RSQLite::SQLite(), dbname = fire)
amb_runs <- dbGetQuery(cons.fire, 'select * from AmbulanceRuns')
dbDisconnect(cons.fire)

amb_columns <- c(5, 1, 16, 14, 19, 21, 6, 8)
amb_runs <- amb_runs[amb_columns]

names(amb_runs) <- c("Date", "Disposition", "Call_Type", "Found_To_Be",
                     "Neighborhood", "Sector", "Dispatched_Time", "Arrival_Time")

#To get response times here
amb_runs$Response_Time_Mins <- as.numeric(hms(amb_runs$Arrival_Time) - hms(amb_runs$Dispatched_Time))/60
amb_runs$Response_Time_Mins <- round(amb_runs$Response_Time_Mins, 2)

#replace_string <- function(df){
  #df <- str_replace(df, "^\\d{1,5}", "")
  #df <- str_replace(df, "COVINGTON.*", "")
  #df <- str_replace(df, "^[[:lower:]]{1,2}", "")
#}
#amb_runs$Address <- replace_string(amb_runs$Address)


amb_runs$Neighborhood[amb_runs$Neighborhood == "WALLACE WOODS"] <- "Wallace Woods"
amb_runs$Neighborhood[amb_runs$Neighborhood == "CENTRAL BUSINESS DISTRICT"] <- "Central Business District"
amb_runs$Neighborhood[amb_runs$Neighborhood == "PEASELBURG"] <- "Peaselburg"
amb_runs$Neighborhood[amb_runs$Neighborhood == "KUHRS LANE"] <- "Kuhrs Lane"
amb_runs$Neighborhood[amb_runs$Neighborhood == "AUSTINBURG"] <- "Austinburg"

write_multiple <- function(x, file){
  write.csv(x, file, row.names = FALSE)
}
write_multiple(amb_runs, "U:/CityWide Performance/CovStat/CovStat Projects/Fire/TableauFiles/OpenData_EMS_Ambulance.csv")

# Create directory for exporting files and set as working directory
ambRunsDir <- "C:/Users/tsink/Google Drive/OpenData Files/Fire/AmbulanceRuns"


if(!dir.exists(ambRunsDir)){
  dir.create(ambRunsDir, showWarnings = TRUE)
  setwd(ambRunsDir)
} else {
  setwd(ambRunsDir)
}

amb_runs$Date <- ymd(amb_runs$Date)
amb_runs2017 <- amb_runs[amb_runs$Date >= as.Date("2017-01-01"),]
#amb_runs2016 <- amb_runs[amb_runs$Date >= as.Date("2016-01-01") & amb_runs$Date < as.Date("2017-01-01"),]
#amb_runs2015 <- amb_runs[amb_runs$Date >= as.Date("2015-01-01") & amb_runs$Date < as.Date("2016-01-01"),]
#amb_runs2014 <- amb_runs[amb_runs$Date >= as.Date("2014-01-01") & amb_runs$Date < as.Date("2015-01-01"),]

write_multiple(amb_runs2017, "amb_runs2017.csv")
#write_multiple(amb_runs2016, "amb_runs2016.csv")
#write_multiple(amb_runs2015, "amb_runs2015.csv")
#write_multiple(amb_runs2014, "amb_runs2014.csv")

# Solid Waste Misses -------------------------------------------------------------------------------
cons.waste <- dbConnect(drv=RSQLite::SQLite(), dbname = waste)
missed <- dbGetQuery(cons.waste, 'select * from WO_Misses')
dbDisconnect(cons.waste)


missed_columns <- c(1, 7, 8, 14)
missed <- missed[missed_columns]

missed$Month <- month(missed$WO_Date)
missed$Weekday <- weekdays(as.Date(missed$WO_Date))
missed$WeekOfYear <- week(missed$WO_Date)

missed$Address <- gsub("^\\d{1,5}\\s", "", missed$Address)

names(missed)[2] <- "Residential"
names(missed)[4] <- "Neighborhood"

missed <- missed[, c(1, 5, 7, 6, 2, 3, 4)]

missed$WO_Date <- ymd(missed$WO_Date)

write_multiple <- function(x, file){
  write.csv(x, file, row.names = FALSE)
}
write_multiple(missed, "U:/CityWide Performance/CovStat/CovStat Projects/Operations/Rumpke/Tableau Files/OpenData_WasteMisses.csv")

# Create directory for exporting files and set as working directory
opDir <- "C:/Users/tsink/Google Drive/OpenData Files/City Manager/Operations"

if(!dir.exists(opDir)){
  dir.create(opDir, showWarnings = TRUE)
  setwd(opDir)
} else {
  setwd(opDir)
}

missed_FY2017 <- missed[missed$WO_Date >= as.Date("2016-07-01"),]
missed_FY2016 <- missed[missed$WO_Date >= as.Date("2015-01-01") & missed$WO_Date < as.Date("2016-07-01"),]

write_multiple(missed_FY2017, "WasteMissesFY2017.csv")
write_multiple(missed_FY2016, "WasteMissesFY2016.csv")

# Waste & Recycling Tonnage -------------------------------------------------------------------
cons.waste <- dbConnect(drv=RSQLite::SQLite(), dbname = waste)
tonnage <- dbGetQuery(cons.waste, 'select * from WasteTonnage')
dbDisconnect(cons.waste)

tonnage <- tonnage[-2]
tonnage$ActTons <- round(tonnage$ActTons, 2)
tonnage$Date <- mdy(tonnage$Date)

# Add additional Date fields
tonnage$Month <- month(tonnage$Date)
tonnage$Weekday <- weekdays(as.Date(tonnage$Date))
tonnage$WeekOfYear <- week(tonnage$Date)

tonnage <- tonnage[, c(4, 6, 8, 7, 5, 1, 2, 3)]
#trim white space at both ends and change values for 'Blank Value' and landfill diversions
tonnage$LandfillDescription <- str_trim(tonnage$LandfillDescription, side = "both")
tonnage$LandfillDescription[tonnage$LandfillDescription == "Blank Value"] <- "RUMPKE BUTLER LANDFILL"
tonnage$LandfillDescription[tonnage$LandfillDescription == "RECYCLE EXPRESS"] <- "Landfill Diversion"
tonnage$LandfillDescription[tonnage$LandfillDescription == "WALTON RECYCLING"] <- "Landfill Diversion"
tonnage$LandfillDescription[tonnage$LandfillDescription == "CENTER CITY RECYCLING"] <- "Landfill Diversion"

write_multiple <- function(x, file){
  write.csv(x, file, row.names = FALSE)
}
write_multiple(tonnage, "U:/CityWide Performance/CovStat/CovStat Projects/Operations/Rumpke/Tableau Files/OpenData_WasteTonnage.csv")

# Create directory for exporting files and set as working directory
opDir <- "C:/Users/tsink/Google Drive/OpenData Files/City Manager/Operations"

if(!dir.exists(opDir)){
  dir.create(opDir, showWarnings = TRUE)
  setwd(opDir)
} else {
  setwd(opDir)
}

tonnage_FY2017 <- tonnage[tonnage$Date >= as.Date("2016-07-01"),]
#tonnage_FY2016 <- tonnage[tonnage$Date >= as.Date("2015-01-01") & tonnage$Date < as.Date("2016-07-01"),]

write_multiple(tonnage_FY2017, "WasteTonnageFY2017.csv")
#write_multiple(tonnage_FY2016, "WasteTonnageFY2016.csv")

# Red Tag Set Outs -------------------------------------------------------------------
cons.waste <- dbConnect(drv=RSQLite::SQLite(), dbname = waste)
rtags <- dbGetQuery(cons.waste, 'select * from RedTags')
dbDisconnect(cons.waste)

keep <- c(2, 4, 9, 10, 18)
rtags <- rtags[keep]
names(rtags)[1] <- "Date"

# Add additional Date fields
rtags$Month <- month(rtags$Date)
rtags$Weekday <- weekdays(as.Date(rtags$Date))
rtags$WeekOfYear <- week(rtags$Date)

rtags <- unite(rtags, Street, c(St_Name, St_Suffix), sep = " ")

#Lookup table to change code with description
codes <- c("CVGAD" = "Alternative Disposal", "CVGR" = "Billed to City",
           "CVGRP" = "Violation Paid by Customer")
rtags$Tag_Code <- codes[rtags$Tag_Code]

rtags$Date <- ymd(rtags$Date)

rtags <- rtags[,c(1, 5, 7, 6, 2:4)]

write_multiple <- function(x, file){
  write.csv(x, file, row.names = FALSE)
}
write_multiple(rtags, "U:/CityWide Performance/CovStat/CovStat Projects/Operations/Rumpke/Tableau Files/OpenData_RedTags.csv")

# Create directory for exporting files and set as working directory
opDir <- "C:/Users/tsink/Google Drive/OpenData Files/City Manager/Operations"

if(!dir.exists(opDir)){
  dir.create(opDir, showWarnings = TRUE)
  setwd(opDir)
} else {
  setwd(opDir)
}

rtags_FY2017 <- rtags[rtags$Date >= as.Date("2016-07-01"),]
rtags_FY2016 <- rtags[rtags$Date >= as.Date("2015-01-01") & rtags$Date < as.Date("2016-07-01"),]

write_multiple(rtags_FY2017, "RedTagsFY2017.csv")
write_multiple(rtags_FY2016, "RedTagsFY2016.csv")

# Recycling Accounts -------------------------------------------------------------------
cons.waste <- dbConnect(drv=RSQLite::SQLite(), dbname = waste)
rec_accounts <- dbGetQuery(cons.waste, 'select * from Recycling_Monthly')
dbDisconnect(cons.waste)

rec_accounts$Month <- month(rec_accounts$Date)

rec_accounts <- rec_accounts[, c(1:2, 9, 3:8)]

write_multiple <- function(x, file){
  write.csv(x, file, row.names = FALSE)
}
write_multiple(rec_accounts, "U:/CityWide Performance/CovStat/CovStat Projects/Operations/Rumpke/Tableau Files/OpenData_RecyclingAccounts.csv")

# Create directory for exporting files and set as working directory
opDir <- "C:/Users/tsink/Google Drive/OpenData Files/City Manager/Operations"

if(!dir.exists(opDir)){
  dir.create(opDir, showWarnings = TRUE)
  setwd(opDir)
} else {
  setwd(opDir)
}

rec_accounts2017 <- rec_accounts[rec_accounts$Date >= as.Date("2017-01-01"),]
rec_accounts2016 <- rec_accounts[rec_accounts$Date >= as.Date("2016-01-01") & rec_accounts$Date < as.Date("2016-12-31"),]

write_multiple(rec_accounts2017, "RecycleAccounts2017.csv")
write_multiple(rec_accounts2016, "RecycleAccounts2016.csv")

