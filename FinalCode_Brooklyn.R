
#CLEAN DATA
brooklyn_cleaned <- read.csv("brooklyn_cleaned.csv", stringsAsFactors = F)
brooklyn_cleaned <- brooklyn_cleaned[complete.cases(brooklyn_cleaned[,c('SchoolDist', 'FireComp', 'PolicePrct', 'HealthCent', 'building_class', 
                                                                        'building_class_at_sale', 'BldgFront', 'BldgDepth',
                                                                        'LotArea', 'NumBldgs', 'NumFloors', 'AssessLand', 'AssessTot')]),]

names(brooklyn_cleaned) <- tolower(names(brooklyn_cleaned)) 
brooklyn_cleaned$unit_id <- seq(1:nrow(brooklyn_cleaned))

#GET VARIABLES THAT WE ARE GOING TO USE IN
library(dplyr)
brooklyn_cleaned<- select(brooklyn_cleaned, 'address', 'zip_code', 'sale_price', 'sale_date', 'ownername', 'ownertype', 'schooldist', 'firecomp', 'policeprct', 'healthcent', 'zonedist1', 'zonedist2', 'zonedist3', 'yearbuilt', 'yearalter1', 'yearalter2', 'borough', 'block', 'lot','building_class', 'building_class_at_sale', 'bldgfront', 'bldgdepth', 'neighborhood','residential_units', 'commercial_units', 'bldgarea', 'comarea', 'resarea', 'officearea', 'retailarea', 'garagearea', 'strgearea', 'factryarea', 'otherarea', 'lotarea', 'numbldgs', 'numfloors', 'assessland', 'assesstot', 'land_sqft', 'gross_sqft')
brooklyn_cleaned <- brooklyn_cleaned[1:10000,] #get first 10000 data
brooklyn_cleaned$unit_id <- 1:nrow(brooklyn_cleaned) #assign ids
# CHANGE VARIABLE NAME TO MATCH WITH DATABASE SCHEMA
colnames(brooklyn_cleaned)[colnames(brooklyn_cleaned)=="apartment_number"] <- "apt_num"
colnames(brooklyn_cleaned)[colnames(brooklyn_cleaned)=="sale_price"] <- "price"
colnames(brooklyn_cleaned)[colnames(brooklyn_cleaned) == "ownername"] <- "owner_name"
colnames(brooklyn_cleaned)[colnames(brooklyn_cleaned) == "ownertype"] <- "owner_type"

#LOAD TABLES INTO DATABASE
library(RPostgreSQL)
library(ggplot2)

#LOAD DRIVER
drv <- dbDriver('PostgreSQL')

#CREATE CONNECTION
con <- dbConnect(drv, dbname = 'Final Project (Finalized)',host = 's19db.apan5310.com', port = 50101, user = 'postgres', password = 'gb455o0x')

stmt <- 
  "
  CREATE TABLE broker (
    broker_id			  int,
    broker_name		  varchar(100) NOT NULL,
    broker_address	varchar(150),
    broker_state		varchar(2),
    broker_zip	   numeric(5,0),
    PRIMARY KEY (broker_id)
  );

CREATE TABLE buyer (
  buyer_id			  int,
  buyer_name		  varchar(100) NOT NULL,
  buyer_address		varchar(150),
  buyer_state		  varchar(2),
  buyer_zip			  numeric(5,0),
  broker_id		    int,
  PRIMARY KEY (buyer_id),
  FOREIGN KEY (broker_id	) REFERENCES broker (broker_id)
);

CREATE TABLE owner (
  owner_id		   int,
  owner_name	   varchar(100) NOT NULL,
  owner_type	   varchar(10),
  broker_id		   int,
  PRIMARY KEY (owner_id),
  FOREIGN KEY (broker_id	) REFERENCES broker (broker_id)
);

CREATE TABLE property(
  unit_id		     int,
  address		     varchar(100) NOT NULL,
  zip_code		   numeric(5,0),
  price		       numeric,
  owner_id		   int,
  district_id		 int,
  PRIMARY KEY (unit_id),
  FOREIGN KEY (owner_id) REFERENCES owner (owner_id)
);

CREATE TABLE listing(
  listing_id		int,	
  unit_id		    int NOT NULL,
  broker_id		  int NOT NULL,
  status		    varchar(10) NOT NULL,
  date_create	  date NOT NULL,
  date_modify	  date,
  PRIMARY KEY (listing_id),
  FOREIGN KEY (unit_id) REFERENCES property (unit_id),
  FOREIGN KEY (broker_id	) REFERENCES broker (broker_id)
);

CREATE TABLE transaction (
  owner_id		  int,
  buyer_id		  int,	
  broker_id		  int,
  listing_id		int,
  sale_date		  date,
  PRIMARY KEY (owner_id, buyer_id, broker_id, listing_id),
  FOREIGN KEY (owner_id) REFERENCES owner (owner_id),
  FOREIGN KEY (buyer_id) REFERENCES buyer (buyer_id),
  FOREIGN KEY (broker_id) REFERENCES broker (broker_id),
  FOREIGN KEY (listing_id) REFERENCES listing (listing_id)
);

CREATE TABLE activity_code (
  activity_id		int,
  activity			varchar(50),	
  description		text,
  PRIMARY KEY (activity_id)
);

CREATE TABLE buyer_activity (
  activity_id		int,
  buyer_id			int,
  listing_id		int,	
  date				  date,
  PRIMARY KEY (activity_id, buyer_id, listing_id)
);

CREATE TABLE owner_activity (
  activity_id		int,
  owner_id			int,
  listing_id		int,
  date				  date,
  PRIMARY KEY (activity_id, owner_id, listing_id)
);

CREATE TABLE district (
  district_id		int,
  schooldist	  int, 
  firecomp		  varchar(10),
  policeprct	  int,
  healthcent	  int,
  PRIMARY KEY (district_id)
);

ALTER table property
ADD FOREIGN KEY (district_id) REFERENCES district (district_id);

CREATE TABLE zone (
	zone_id		    int,
	zonedist		  varchar(10),
	zonedist_num	int,
	district_id		int,
	PRIMARY KEY (zone_id),
	FOREIGN KEY (district_id) REFERENCES district (district_id)
);

CREATE TABLE borough (
  borough_id		int,
  borough		    int NOT NULL,
  block			    int NOT NULL,
  district_id		int,
  PRIMARY KEY (borough_id),
  FOREIGN KEY (district_id) REFERENCES district (district_id)
);

CREATE TABLE building (
	building_id		            int,
	building_class	          char(2) NOT NULL,
	building_class_at_sale		char(2),
	bldgfront				          numeric
	bldgdepth				          numeric,
	PRIMARY KEY(building_id)
);

CREATE TABLE unit_building (
  unit_id			             int,
  building_id		           int,
  PRIMARY KEY (unit_id, building_id),
  FOREIGN KEY (unit_id) REFERENCES property (unit_id),
  FOREIGN KEY (building_id) REFERENCES building (building_id)
);

CREATE TABLE building_feature (
  building_id		           int,
  neighborhood		         varchar(50) NOT NULL,
  numfloors		             numeric(5,2),
  residential_units	       int,
  commercial_units	       int,
  PRIMARY KEY (building_id),
  FOREIGN KEY (building_id) REFERENCES building (building_id)
);

CREATE TABLE building_layout (
  building_id	  int,
  bldgarea		  int,
  comarea 		  int,
  resarea		    int,
  officearea	  int,
  retailarea		int,
  garagearea	  int,
  strgearea		  int,
  factryarea	  int,
  otherarea		  int,
  PRIMARY KEY (building_id),
  FOREIGN KEY (building_id) REFERENCES building (building_id)
);

CREATE TABLE lot (
  lot_id      int,
  lotarea			int,
  numbldgs		int,
  assessland	int,
  assesstot		int,
  PRIMARY KEY (lot_id )
);

CREATE TABLE unit_lot (
  unit_id			int,
  lot_id      int,
  PRIMARY KEY (unit_id, lot_id ),
  FOREIGN KEY (unit_id) REFERENCES property (unit_id),
  FOREIGN KEY (lot_id ) REFERENCES lot (lot_id )
);

CREATE TABLE  unit_detail (
  unit_id			int,
  land_sqft		int,
  gross_sqft	int,
  PRIMARY KEY (unit_id),
  FOREIGN KEY (unit_id) REFERENCES property (unit_id)
);

CREATE TABLE activity (
  action_id		int,
  year			  int,
  action		  varchar(20),
  PRIMARY KEY (action_id)
);

CREATE TABLE unit_activity (
  unit_id		  int,
  action_id		int,
  PRIMARY KEY (unit_id, action_id),
  FOREIGN KEY (unit_id) REFERENCES property (unit_id),
  FOREIGN KEY (action_id) REFERENCES activity (action_id)
); "

dbGetQuery(con, stmt)

###ETL
#Since each row contains information that is unique to a listing ID, listing_id is set firstly
listing_id <- seq(1:nrow(brooklyn_cleaned))
brooklyn_cleaned$listing_id <- listing_id


#TABLE broker
#Brokers' information are added to the data drame to boost CRM functionality of the database. 
#Since these variables are made up, input values are sourced from another dataset and combined to data frame
#Load outsourced dataset and choose first 50,000 data points
buyerbroker <- read.csv("buyer-broker.csv", stringsAsFactors = F)
buyerbroker1 <- buyerbroker[1:10000,]
#Examine dataset
summary(buyerbroker1)
brooklyn_cleaned$sale_date <- as.Date(brooklyn_cleaned$sale_date)
summary(brooklyn_cleaned$sale_date)
#Set zip_code to stanndard 5 numbers
buyerbroker1$Business.Zip <- strtrim(buyerbroker1$Business.Zip, 5)
buyerbroker1$Business.Zip <- as.numeric(buyerbroker1$Business.Zip)
#Set id for brokers 
broker_id <- seq(1:nrow(buyerbroker1))
buyerbroker1$broker_id <- broker_id
#Load brokers's information into brooklyn_cleaned matching broker_id with listing_id
brooklyn_cleaned$broker_name <- buyerbroker1$Business.Name[match(brooklyn_cleaned$listing_id,buyerbroker1$broker_id)]
brooklyn_cleaned$broker_address <- buyerbroker1$Business.Address.1[match(brooklyn_cleaned$listing_id,buyerbroker1$broker_id)]
brooklyn_cleaned$broker_city <- buyerbroker1$Business.City[match(brooklyn_cleaned$listing_id,buyerbroker1$broker_id)]
brooklyn_cleaned$broker_state <- buyerbroker1$Business.State[match(brooklyn_cleaned$listing_id,buyerbroker1$broker_id)]
brooklyn_cleaned$broker_zip <- buyerbroker1$Business.Zip[match(brooklyn_cleaned$listing_id,buyerbroker1$broker_id)]
#Examine brokers' information in updated brooklyn_cleaned for repeating values
summary(brooklyn_cleaned$broker_name)
#Since there are repeated value in broker_name, brooklyn_cleaned with unique values created and add broker_id
broker_brooklyn_cleaned <- unique(brooklyn_cleaned[c('broker_name', 'broker_address', 'broker_city', 'broker_state', 'broker_zip')])
broker_brooklyn_cleaned$broker_id <- 1:nrow(broker_brooklyn_cleaned)
#load into the database
dbWriteTable(con, name="broker", value=broker_brooklyn_cleaned, row.names=FALSE, append=TRUE)
#load broker_id into brooklyn_cleaned
brooklyn_cleaned$broker_id <- broker_brooklyn_cleaned$broker_id[match(brooklyn_cleaned$broker_name,broker_brooklyn_cleaned$broker_name)]
#check few repeating broker_name to ensure that brooklyn_cleaned is loaded correctly:
brooklyn_cleaned[c('broker_id', 'broker_name')][brooklyn_cleaned$broker_name %in% c('ABRAHAM ANTHONY M','BEN BAY REALTY CO'),]


#TABLE buyer
#Similar to broker, buyers' informatiom are made up for CRM purpose
#Apply the same procedures as table broker, variables of buyer table are loaded based on outsourced dataset
buyerbroker2 <- buyerbroker[20000:30000,] 
buyerbroker2$Business.Zip <- strtrim(buyerbroker2$Business.Zip, 5)
buyerbroker2$Business.Zip <- as.numeric(buyerbroker2$Business.Zip)
#Set id for buyers
buyer_id <- seq(1:nrow(buyerbroker2))
buyerbroker2$buyer_id <- buyer_id
#Load buyer's information into brooklyn_cleaned matching buyer_id with listing_id
brooklyn_cleaned$buyer_name <- buyerbroker2$Business.Name[match(brooklyn_cleaned$listing_id,buyerbroker2$buyer_id)]
brooklyn_cleaned$buyer_address <- buyerbroker2$Business.Address.1[match(brooklyn_cleaned$listing_id,buyerbroker2$buyer_id)]
brooklyn_cleaned$buyer_city <- buyerbroker2$Business.City[match(brooklyn_cleaned$listing_id,buyerbroker2$buyer_id)]
brooklyn_cleaned$buyer_state <- buyerbroker2$Business.State[match(brooklyn_cleaned$listing_id,buyerbroker2$buyer_id)]
brooklyn_cleaned$buyer_zip <- buyerbroker2$Business.Zip[match(brooklyn_cleaned$listing_id,buyerbroker2$buyer_id)]
#Examine buyers' information in updated brooklyn_cleaned for repeating values
summary(brooklyn_cleaned$buyer_name)
#Since there are repeated value in buyer_name, brooklyn_cleaned with unique values created and add buyer_id
buyer_brooklyn_cleaned <- unique(brooklyn_cleaned[c('buyer_name', 'buyer_address', 'buyer_city', 'buyer_state', 'buyer_zip', 'broker_id')])
buyer_brooklyn_cleaned$buyer_id <- 1:nrow(buyer_brooklyn_cleaned)
#load into the database
dbWriteTable(con, name="buyer", value=buyer_brooklyn_cleaned, row.names=FALSE, append=TRUE)
#load buyer_id into brooklyn_cleaned
brooklyn_cleaned$buyer_id <- buyer_brooklyn_cleaned$buyer_id[match(brooklyn_cleaned$buyer_name,buyer_brooklyn_cleaned$buyer_name)]
#check few repeating buyer_name to ensure that brooklyn_cleaned is loaded correctly:
brooklyn_cleaned[c('buyer_id', 'buyer_name')][brooklyn_cleaned$buyer_name %in% c('CARR INC','CARRIS S JOSEPH'),]


##TABLE owner
#variable OwnerName from the brooklyn_cleaned has missing values, however based on the schema of the real estate database the company should always know the owner name. With this in mind, all NA will be filled with "Brooklyn Homes" (as our clients own it) to fit with the constraints. 
brooklyn_cleaned$owner_name <- as.character(brooklyn_cleaned$owner_name)
brooklyn_cleaned$owner_name[is.na(brooklyn_cleaned$owner_name)] <- "BROOKLYN HOMES"
brooklyn_cleaned$owner_name <- as.factor(brooklyn_cleaned$owner_name)
#Since there are repeating values within owner_name so we cannot simply add a column with incrementing integer numbers for the primary key of owner_id as this would lead to owner_id with multiple primary keys. 
#Create temporary dataframe with unique owner names
owner_brooklyn_cleaned <- unique(brooklyn_cleaned[c('owner_name', 'owner_type', 'broker_id')])
# Add incrementing integers
owner_brooklyn_cleaned$owner_id <- 1:nrow(owner_brooklyn_cleaned)
#Before adding the owner_id primary key to the main dataframe, brooklyn_cleaned, we can push the owner data to the database:
dbWriteTable(con, name="owner", value=owner_brooklyn_cleaned, row.names=FALSE, append=TRUE)
#Add owner_id to brooklyn_cleaned
# Map owner_id
owner_id_list <- owner_brooklyn_cleaned[c('owner_id', 'owner_name')]
# Add owner_id to the main dataframe
brooklyn_cleaned$owner_id <- owner_id_list$owner_id[match(brooklyn_cleaned$owner_name,owner_id_list$owner_name)]
brooklyn_cleaned$owner_id <- as.factor(brooklyn_cleaned$owner_id)
summary(brooklyn_cleaned$owner_name)
#check few repeating owner_name to ensure that brooklyn_cleaned is loaded correctly:
brooklyn_cleaned[c('owner_id', 'owner_name')][brooklyn_cleaned$owner_name %in% c('SILVERSHORE PROPERTIE','PENRITH URF LLC'),]


#TABLE listing
#Few variables are made up and added to fulfill the logical of the schema by randomly assign values
#variable: status - the status of the listing
set.seed(1234)
brooklyn_cleaned$status <- sample(c("Active","Closed","Under Contract","Contigent","Deal Pending","Expired","Withdraw"),10000,replace=TRUE)
brooklyn_cleaned$status <- as.factor(brooklyn_cleaned$status)
#variable: date_create - initinal date of listing being created
brooklyn_cleaned$date_create <- sample(seq(as.Date('2000/01/01'), as.Date('2016/01/01'), by="day"), 10000,replace=TRUE)
#variable: date_modify - initinal date of listing being modified
brooklyn_cleaned$date_modify <- sample(seq(as.Date('2000/01/15'), as.Date('2017/01/01'), by="day"), 10000,replace=TRUE)
#temporary brooklyn_cleaned of listing is created 
listing_brooklyn_cleaned <- brooklyn_cleaned[c('listing_id', 'unit_id', 'broker_id', 'status', 'date_create', 'date_modify')]
#load into the database
dbWriteTable(con, name="listing", value=listing_brooklyn_cleaned, row.names=FALSE, append=TRUE)


#TABLE transaction
#temporary brooklyn_cleaned of transaction is created 
transaction_brooklyn_cleaned <- brooklyn_cleaned[c('owner_id', 'buyer_id', 'broker_id', 'listing_id', 'sale_date')]
#load into the database
dbWriteTable(con, name="transaction", value=transaction_brooklyn_cleaned, row.names=FALSE, append=TRUE)


#TABLE activity_code
#Few variables are made up and added to fulfill the logical of the schema by randomly assign values
#variable: activity_id 
set.seed(50)
brooklyn_cleaned$activity <- sample(c("Saved","Favorited","Apointment Requested","Disliked","Info Requested"),10000,replace=TRUE)
brooklyn_cleaned$activity <- as.factor(brooklyn_cleaned$activity)
#variable: description
set.seed(50)
brooklyn_cleaned$description <- sample(c("Lising is saved to account","Listing is favorited","Appointment is requested to see listing in person","Listing doesn't satisfy customer's needs","More info is being requested"),10000,replace=TRUE)
brooklyn_cleaned$description <- as.factor(brooklyn_cleaned$description)
#Create temporary dataframe for activity_code
activity_brooklyn_cleaned <- unique(brooklyn_cleaned[c('activity', 'description')])
# Add incrementing integers
activity_brooklyn_cleaned$activity_id <- 1:nrow(activity_brooklyn_cleaned)
#load into the database
dbWriteTable(con, name="activity_code", value=activity_brooklyn_cleaned, row.names=FALSE, append=TRUE)
#Map activity_id back to main brooklyn_cleaned
brooklyn_cleaned$activity_id <- activity_brooklyn_cleaned$activity_id[match(brooklyn_cleaned$activity,activity_brooklyn_cleaned$activity)]


#TABLE buyer_activity
#Few variables are made up and added to fulfill the logical of the schema by randomly assign values
#variable: b_date
set.seed(300)
brooklyn_cleaned$b_date <- sample(seq(as.Date('2017/02/13'), as.Date('2018/07/01'), by="day"), 10000,replace=TRUE)
#temporary brooklyn_cleaned of transaction is created 
buyeract_brooklyn_cleaned <- brooklyn_cleaned[c('activity_id', 'buyer_id', 'listing_id', 'b_date')]
#load into the database
dbWriteTable(con, name="buyer_activity", value=buyeract_brooklyn_cleaned, row.names=FALSE, append=TRUE)


#TABLE owner_activity
#Few variables are made up and added to fulfill the logical of the schema by randomly assign values
#variable: o_date
set.seed(300)
brooklyn_cleaned$o_date <- sample(seq(as.Date('2017/04/27'), as.Date('2018/06/01'), by="day"),10000,replace=TRUE)
#temporary brooklyn_cleaned of transaction is created 
owneract_brooklyn_cleaned <- brooklyn_cleaned[c('activity_id', 'owner_id', 'listing_id', 'o_date')]
#load into the database
dbWriteTable(con, name="owner_activity", value=owneract_brooklyn_cleaned, row.names=FALSE, append=TRUE)


#TABLE district
#get district table
temp_district <- unique(brooklyn_cleaned[c('schooldist', 'firecomp', 'policeprct', 'healthcent')])
temp_district$district_id <- 1:nrow(temp_district)  
dbWriteTable(con, name = "district", value = district, row.names = FALSE, append = TRUE)
#put id in main dataframe
brooklyn_cleaned$district_id <- temp_district$district_id[match(interaction(brooklyn_cleaned$schooldist, brooklyn_cleaned$firecomp, brooklyn_cleaned$policeprct, brooklyn_cleaned$healthcent), interaction(temp_district$schooldist, temp_district$firecomp,  temp_district$policeprct, temp_district$healthcent))]
district <- temp_district 


#TABLE building
temp_building <- unique(brooklyn_cleaned[c('building_class', 'building_class_at_sale', 'bldgfront', 'bldgdepth')])
temp_building$building_id <- 1:nrow(temp_building) #assign ids
dbWriteTable(con, name = "building", value = building, row.names = FALSE, append = TRUE)
#building id for main table, we use loop here to compare the matching values
brooklyn_cleaned$building_id <- 0
startTime <- proc.time() #time the process
for (i in 1:nrow(brooklyn_cleaned)) {
  for (j in 1:nrow(temp_building)) {
    if (brooklyn_cleaned[i, 'building_class'] == temp_building[j,'building_class'] && 
        brooklyn_cleaned[i, 'building_class_at_sale'] == temp_building[j,'building_class_at_sale'] &&
        brooklyn_cleaned[i, 'bldgfront'] == temp_building[j,'bldgfront'] &&
        brooklyn_cleaned[i, 'bldgdepth'] == temp_building[j,'bldgdepth']) {
      brooklyn_cleaned$building_id[i] <- temp_building$building_id[j]
    }
  }
}
proc.time() - startTime
building <- temp_building



#TABLE building_feature
building_feature <- brooklyn_cleaned[,c('building_id', 'neighborhood', 'numfloors', 'residential_units','commercial_units')]
dbWriteTable(con, name = "building_feature", value = building_feature, row.names = FALSE, append = TRUE)


#TABLE building_layout
building_layout <- brooklyn_cleaned[,c('building_id', 'bldgarea', 'comarea', 'resarea',  'officearea', 'retailarea', 'garagearea', 'strgearea',
                                       'factryarea', 'otherarea')]
building_layout <- building_layout[!duplicated(building_layout[,'building_id']),]
dbWriteTable(con, name = "building_layout", value = building_layout, row.names = FALSE, append = TRUE)


#TABLE lot
lot <- brooklyn_cleaned[,c("lotarea", "numbldgs", "assessland","assesstot")]
lot <- unique(lot)
lot$lot_id <- seq(1, nrow(lot)) 
dbWriteTable(con, name = "lot", value = lot, row.names = FALSE, append = TRUE)
#lot id for main table, same idea: loop
brooklyn_cleaned$lot_id <- 0
startTime <- proc.time()
for (i in 1:nrow(brooklyn_cleaned)) {
  for (j in 1:nrow(lot)) {
    if (brooklyn_cleaned[i, 'lotarea'] == lot[j,'lotarea'] && 
        brooklyn_cleaned[i, 'numbldgs'] == lot[j,'numbldgs'] &&
        brooklyn_cleaned[i, 'assessland'] == lot[j,'assessland'] &&
        brooklyn_cleaned[i, 'assesstot'] == lot[j,'assesstot']) {
      brooklyn_cleaned$lot_id[i] <- lot$lot_id[j]
    }
  }
}
proc.time() - startTime


#TABLE unit_lot
unit_lot <- brooklyn_cleaned[,c('unit_id', 'lot_id')]
dbWriteTable(con, name = "unit_lot", value = unit_lot, row.names = FALSE, append = TRUE)

#TABLE zone
#this is the table we get from 1NF. To get zone table, we first combine all zone1, zone2 and zone 3 values then put them under one variable named “zoneDist”. 
zoneDist <- c(brooklyn_cleaned$zonedist1, brooklyn_cleaned$zonedist2, brooklyn_cleaned$zonedist3) #discard zone dist 4 beacuse of 100%NA
zoneDist_num <- seq(length(zoneDist)) 
#expand zones, label each zone zone_number. Since units repeat twice, simply assign first ⅓ zone1, next ⅓ zone2 and final ⅓ zone3
zoneDist_num[1:10000] <- 1
zoneDist_num[10001:20000] <- 2
zoneDist_num[20001:30000] <- 3
zone <- data.frame(zoneDist, zoneDist_num)
zone$zone_id <- seq(1:nrow(zone)) #assign ids
dbWriteTable(con, name = "zone", value = zone, row.names = FALSE, append = TRUE)
zone$district_id <- brooklyn_cleaned$district_id
names(zone) <- c('zonedist', 'zonedist_num', 'zone_id', 'district_id')


#TABLE activity
#same idea as zone, put year_built, year altered 1 and altered 2 under one variable
year <- c(brooklyn_cleaned$yearbuilt, brooklyn_cleaned$yearalter1, brooklyn_cleaned$yearalter2)
action <- seq(length(year))
#expand actions, assign the actions. The first ⅓ are years built, next ⅓ are year altered 1 and rest ⅓ are year altered 2
action[1:10000] <- "Build"
action[10001:20000] <- "First Alter"
action[20001:30000] <- "Second Alter"
activity <- data.frame(year, action)
activity$action_id <- seq(1:nrow(activity))
dbWriteTable(con, name = "activity", value = activity, row.names = FALSE, append = TRUE)
#insert unit id to match each row, will be dropped later
activity$unit_id <- 0
activity$unit_id[1:10000] <- brooklyn_cleaned$unit_id
activity$unit_id[10001:20000] <- brooklyn_cleaned$unit_id
activity$unit_id[20001:30000] <- brooklyn_cleaned$unit_id
#there are year of 0, change them to na
activity[activity == 0] <- NA
activity <- activity[complete.cases(activity[activity$year, ]),] #drop actions that do not exist


#TABLE unit_activity
unit_activity <- data.frame(activity$action_id, activity$unit_id)
names(unit_activity) <- c("action_id", "unit_id")
activity <- activity[,-4] #drop unit id from zone to keep it 3nf
dbWriteTable(con, name = "unit_activity", value = unit_activity, row.names = FALSE, append = TRUE)


#TABLE property
property <- brooklyn_cleaned[c('unit_id', 'address', 'zip_code', 'owner_id', 'price', 'district_id')]
dbWriteTable(con, name="property", value=property, row.names=FALSE, append=TRUE)


#TABLE borough
temp_borough_df <- unique(brooklyn_cleaned[c('borough', 'block','district_id')])
temp_borough_df$borough_id <- 1:nrow(temp_borough_df)
dbWriteTable(con, name="borough", value=temp_borough_df, row.names=FALSE, append=TRUE)
#Method of match(interaction)
brooklyn_cleaned$borough_id <- temp_borough_df$borough_id[match(interaction(brooklyn_cleaned$borough, brooklyn_cleaned$block, brooklyn_cleaned$district_id), 
                                                                interaction(temp_borough_df$borough, temp_borough_df$block, temp_borough_df$district_id))]


#TABLE unit_building
unit_building <- brooklyn_cleaned[c('unit_id', 'building_id')]
dbWriteTable(con, name="unit_building", value=unit_building, row.names=FALSE, append=TRUE)

##unit_detail
unit_detail <- brooklyn_cleaned[c('unit_id', 'land_sqft', 'gross_sqft')]
dbWriteTable(con, name="unit_detail", value=unit_detail, row.names=FALSE, append=TRUE)











