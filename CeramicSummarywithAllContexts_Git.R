#R code for analysis of ceramics assemblages to compare with lithic assemblages from the Hermitage
#Created by: EAB 11/22/2017
#Last Update: EAB 11/22/2017
#load the library
library(DBI)
require(RPostgreSQL)
library(RODBC)

# tell DBI which driver to use
pgSQL <- dbDriver("PostgreSQL")
# establish the connection
DRCcon<-dbConnect(pgSQL, host=connection$host, port=connection$port,
                  dbname=connection$dbname,
                  user=connection$user, password=connection$password)


CeramicData<-dbGetQuery(DRCcon,'
                         SELECT
                        "public"."tblContext"."ContextID",
                        "public"."tblProjectName"."ProjectName",
                        "public"."tblCeramicMaterial"."CeramicMaterial",
                        SUM("public"."tblCeramic"."Quantity") as "REW_Count"
                        FROM
                        "public"."tblProject"
                        LEFT JOIN "public"."tblProjectName" ON "public"."tblProject"."ProjectNameID" = "public"."tblProjectName"."ProjectNameID"
                        INNER JOIN "public"."tblContext" ON "public"."tblContext"."ProjectID" = "public"."tblProject"."ProjectID"
                        INNER JOIN "public"."tblContextSample" ON "public"."tblContextSample"."ContextAutoID" = "public"."tblContext"."ContextAutoID"
                        INNER JOIN "public"."tblGenerateContextArtifactID" ON "public"."tblContextSample"."ContextSampleID" = "public"."tblGenerateContextArtifactID"."ContextSampleID"
                        INNER JOIN "public"."tblCeramic" ON "public"."tblCeramic"."GenerateContextArtifactID" = "public"."tblGenerateContextArtifactID"."GenerateContextArtifactID"
                        INNER JOIN "public"."tblCeramicWare" ON "public"."tblCeramic"."WareID" = "public"."tblCeramicWare"."WareID"
                        INNER JOIN "public"."tblCeramicMaterial" ON "public"."tblCeramic"."CeramicMaterialID" = "public"."tblCeramicMaterial"."CeramicMaterialID"
                        LEFT JOIN "public"."tblContextFeatureType" ON "public"."tblContext"."FeatureTypeID" = "public"."tblContextFeatureType"."FeatureTypeID"
                        
                        WHERE
                        (("public"."tblCeramicMaterial"."CeramicMaterial" = \'Refined EW\') and
                        ("public"."tblProject"."ProjectID" in (\'1400\',\'1401\',\'1402\',\'1403\',\'1404\',\'1405\',\'1406\',\'1407\',\'1410\',\'1412\',\'1413\',\'1414\')))
                        Group by "public"."tblContext"."ContextID",
                        "public"."tblProjectName"."ProjectName",
                        "public"."tblCeramicMaterial"."CeramicMaterial"                        
                        Order by "public"."tblContext"."ContextID"
                        ')

# what's in the dataframe?
str(CeramicData)
require(dplyr)
require(plyr)
require(tidyr)

#CeramicData2 <- ddply(CeramicData, .(ContextID, CeramicMaterial), summarise, Count=sum(Count))
#reshape data
#CeramicData3<-CeramicData2 %>%
#  spread(Form, Count)


SpatialData<-dbGetQuery(DRCcon,'
                        SELECT
                        "public"."tblProject"."ProjectID",
                        "public"."tblProjectName"."ProjectName",
                        "public"."tblContext"."ContextID",
                        "public"."tblContext"."QuadratID",
                        "public"."tblContextLevelType"."LevelType",
                        "public"."tblContextUnitType"."UnitType",
                        string_agg (distinct COALESCE ("public"."tblContextQuadratBoundary"."QuadratNorthing") || \'; \' || COALESCE ("public"."tblContextQuadratBoundary"."QuadratEasting"), \'; \') AS "QuadBoundaries",
                        avg("public"."tblContextQuadratBoundary"."QuadratNorthing") as MeanNorthing,
                        avg("public"."tblContextQuadratBoundary"."QuadratEasting") as MeanEasting,
                        max("public"."tblContextQuadratBoundary"."QuadratNorthing")-min("public"."tblContextQuadratBoundary"."QuadratNorthing") as NorthingDim,
                        max("public"."tblContextQuadratBoundary"."QuadratEasting")-min("public"."tblContextQuadratBoundary"."QuadratEasting") as EastingDim,
                        (max("public"."tblContextQuadratBoundary"."QuadratNorthing")-min("public"."tblContextQuadratBoundary"."QuadratNorthing"))*(max("public"."tblContextQuadratBoundary"."QuadratEasting")-min("public"."tblContextQuadratBoundary"."QuadratEasting")) 
                        as area,
                        "public"."tblContextDepositType"."DepositType",
                        "public"."tblContextFeatureType"."FeatureType" AS "FeatureType",
                        "public"."tblContext"."FeatureNumber" AS "FeatureNumber",
                        "public"."tblContext"."LevelDesignation",
                        "public"."tblContext"."DAACSStratigraphicGroup",
                        "public"."tblContext"."DAACSStratigraphicGroupInterpretation",
                        "public"."tblContext"."DAACSPhase"
                        FROM
                        "public"."tblProject"
                        LEFT JOIN "public"."tblProjectName" ON "public"."tblProject"."ProjectNameID" = "public"."tblProjectName"."ProjectNameID"
                        INNER JOIN "public"."tblContext" ON "public"."tblContext"."ProjectID" = "public"."tblProject"."ProjectID"
                        LEFT JOIN "public"."tblContextUnitType" ON "public"."tblContext"."UnitTypeID" = "public"."tblContextUnitType"."UnitTypeID"
                        LEFT JOIN "public"."tblContextLevelType" ON "public"."tblContext"."LevelTypeID" = "public"."tblContextLevelType"."LevelTypeID"
                        LEFT JOIN "public"."tblContextQuadratBoundary" ON "public"."tblContext"."ProjectID" = "public"."tblContextQuadratBoundary"."ProjectID" AND "public"."tblContext"."QuadratID" = "public"."tblContextQuadratBoundary"."QuadratID"
                        LEFT JOIN "public"."tblContextDepositType" ON "public"."tblContext"."DepositTypeID" = "public"."tblContextDepositType"."DepositTypeID"
                        LEFT JOIN "public"."tblContextFeatureType" ON "public"."tblContext"."FeatureTypeID" = "public"."tblContextFeatureType"."FeatureTypeID"
                        WHERE
                        "public"."tblProject"."ProjectID" in (\'1400\',\'1401\',\'1402\',\'1403\',\'1404\',\'1405\',\'1406\',\'1407\',\'1410\',\'1412\',\'1413\',\'1414\')
                        GROUP BY
                        "public"."tblProject"."ProjectID",
                        "public"."tblProjectName"."ProjectName",
                        "public"."tblContext"."ContextID",
                        "public"."tblContext"."QuadratID",
                        "public"."tblContextUnitType"."UnitType",
                        "public"."tblContextLevelType"."LevelType",
                        "public"."tblContextDepositType"."DepositType",
                        "public"."tblContextFeatureType"."FeatureType",
                        "public"."tblContext"."FeatureNumber",
                        "public"."tblContext"."ExcavatorSedimentDescription",
                        "public"."tblContext"."ExcavatorInterpretation",
                        "public"."tblContext"."LevelDesignation",
                        "public"."tblContext"."DAACSStratigraphicGroup",
                        "public"."tblContext"."DAACSStratigraphicGroupInterpretation",
                        "public"."tblContext"."DAACSPhase"
                        ORDER BY
                        "public"."tblContext"."ContextID" ASC
                        ')
#write.csv(SpatialData, 'CeramicSpatialData.csv')

#need to rename Quadrat "ECN01"
SpatialData$QuadratID[SpatialData$QuadratID == "'ECN01 '"] <- "ECN01"

#Remove clean up/out of strat context deposits
SpatialData <- filter(SpatialData, DepositType != 'Clean-Up/Out-of-Stratigraphic Context'& DepositType != 'Surface Collection')

#DataAllSites<-full_join(LithicData3, SpatialData, by="ContextID")
#I think this is what I actually want
CeramicDataAllSites<-left_join(SpatialData, CeramicData, by='ContextID')

#changing QuadIDs to match those for Triplex
CeramicDataAllSites$QuadratID2<-ifelse(grepl("TXE5A", CeramicDataAllSites$ContextID),
                                paste('TXE5A'),
                                (ifelse(grepl("TXE5B", CeramicDataAllSites$ContextID),
                                        paste('TXE5B'),
                                        ifelse(grepl("TXE6B", CeramicDataAllSites$ContextID),
                                               paste('TXE6B'),
                                               ifelse(grepl("TXE7C", CeramicDataAllSites$ContextID),
                                                      paste('TXE7C'),
                                                      ifelse(grepl("TXE7D", CeramicDataAllSites$ContextID),
                                                             paste('TXE7D'),
                                                             ifelse(grepl("TXM1", CeramicDataAllSites$ContextID),
                                                                    paste('TXM1'),
                                                                    ifelse(grepl("TXM2", CeramicDataAllSites$ContextID),
                                                                           paste('TXM2'),
                                                                           ifelse(grepl("TXM3", CeramicDataAllSites$ContextID),
                                                                                  paste('TXM3'),
                                                                                  ifelse(grepl("TXM4", CeramicDataAllSites$ContextID),
                                                                                         paste('TXM4'),
                                                                                         ifelse(grepl("TXN1", CeramicDataAllSites$ContextID),
                                                                                                paste('TXN1'),
                                                                                                ifelse(grepl("TXN2", CeramicDataAllSites$ContextID),
                                                                                                       paste('TXN2'),
                                                                                                       ifelse(grepl("TXN3", CeramicDataAllSites$ContextID),
                                                                                                              paste('TXN3'),
                                                                                                              ifelse(grepl("TXN4", CeramicDataAllSites$ContextID),
                                                                                                                     paste('TXN4'),
                                                                                                                     ifelse(grepl("TXS4", CeramicDataAllSites$ContextID),
                                                                                                                            paste('TXS4'),
                                                                                                                            ifelse(grepl("TXW3", CeramicDataAllSites$ContextID),
                                                                                                                                   paste('TXW3'),
                                                                                                                                   ifelse(grepl("TXW6A", CeramicDataAllSites$ContextID),
                                                                                                                                          paste('TXW6A'),
                                                                                                                                          paste(CeramicDataAllSites$QuadratID)
                                                                                                                                   )))))))))))))))))


#changing spatial coordinates for Triplex and Yard Cabin because they are entered in DAACS using the local MBY datum/
#grid
#read in Coordinate data transformed to Plantation Grid
YC_Tri_Coord<-read.csv(file="YC_TriplexSpatialCoordinatesReassign.csv", header=TRUE, sep=",")
#Calculate means of Northing and Easting Averages like in the query

YC_Tri_Coord<-mutate(YC_Tri_Coord, meannorthing=rowMeans(select(YC_Tri_Coord, contains("Y")))) %>%
  mutate(YC_Tri_Coord, meaneasting=rowMeans(select(YC_Tri_Coord, contains("X")))) %>%
  #Calculate northing/easting dimensions and area like in query
  mutate(YC_Tri_Coord, northingdim=(pmax(NW.Y, NE.Y, SW.Y, SE.Y)-pmin(NW.Y, NE.Y, SW.Y, SE.Y))) %>%
  mutate(YC_Tri_Coord, eastingdim=(pmax(NW.X, NE.X, SW.X, SE.X)-pmin(NW.X, NE.X, SW.X, SE.X))) %>%
  mutate(YC_Tri_Coord, area=northingdim*eastingdim)

require(tibble)
#merge new Northings and Eastings with original dataset
#if unit is one from triplex or yard cabin put meannorthing, easting from spreadsheet, otherwise put in northing easting from
#original dataset
#have to specify which package to use with rename because plyr and dplyr don't work well together
YC_Tri_Coord<-dplyr::rename(YC_Tri_Coord, QuadratID2 = QuadID)
CeramicDataAllSites2<-filter(CeramicDataAllSites, UnitType == 'Quadrat/Unit')
CeramicDataAllSites3<-left_join(CeramicDataAllSites2, YC_Tri_Coord, by="QuadratID2") %>%
  mutate(meannorthing=ifelse((ProjectID == '1400' | ProjectID == '1404'),
                             meannorthing.y,
                             meannorthing.x)) %>%
  mutate(meaneasting=ifelse((ProjectID == '1400' | ProjectID == '1404'),
                            meaneasting.y,
                            meaneasting.x)) %>%
  mutate(area_update=ifelse((ProjectID == '1400' | ProjectID == '1404'),
                            area.y,
                            area.x))

#Select columns that are useful for GIS analysis
CeramicDataAllSites4 <-
  select(CeramicDataAllSites3, 1:2, 22:23, 37:39) %>%
  replace_na(list(REW_Count=0)) %>%
  mutate(Area=ifelse((ProjectID == '1400' | ProjectID == '1404'),
                     paste('MBY'),
                     ifelse((ProjectID == '1410' | ProjectID == '1412' | ProjectID == '1402'),
                            paste('FH'),
                            paste('FQ')
                     )))

#have to detach plyr package because otherwise it prevents the group_by/summarise functions from working
detach(package:plyr)
#create object for GIS kriging, start by selecting only First Hermitage sites and get rid of a bunch of extraneous columns
FirstHermCeramicsGIS<-filter(CeramicDataAllSites4, Area == 'FH') %>%
  group_by(ProjectID, ProjectName.x, QuadratID2, meannorthing, meaneasting, area_update) %>%
  summarise(REW_Count2 = sum(REW_Count)) %>%
  mutate(REW_Count2 = REW_Count2 + .5) %>%
  mutate(REWDensity = REW_Count2/area_update) %>%
  na.omit

#checking for duplicate quad ids
duplicated(FirstHermCeramicsGIS$QuadratID2)

#checking for duplicate quad ids
write.csv(FirstHermCeramicsGIS, 'FirstHermCeramics.csv')

#same analysis for Field Quarter
FieldQuarterCeramicsGIS<-filter(CeramicDataAllSites4, Area == 'FQ') %>%
  group_by(ProjectID, ProjectName.x, QuadratID2, meannorthing, meaneasting, area_update) %>%
  summarise(REW_Count2 = sum(REW_Count)) %>%
  mutate(REW_Count2 = REW_Count2 + .5) %>%
  mutate(REWDensity = REW_Count2/area_update) %>%
  na.omit

#checking for duplicate quad ids
duplicated(FieldQuarterCeramicsGIS$QuadratID2)

#checking for duplicate quad ids
write.csv(FieldQuarterCeramicsGIS, 'FieldQuarterCeramics.csv')

#same analysis for MBY
MBYCeramicsGIS<-filter(CeramicDataAllSites4, Area == 'FQ') %>%
  group_by(ProjectID, ProjectName.x, QuadratID2, meannorthing, meaneasting, area_update) %>%
  summarise(REW_Count2 = sum(REW_Count)) %>%
  mutate(REW_Count2 = REW_Count2 + .5) %>%
  mutate(REWDensity = REW_Count2/area_update) %>%
  na.omit

#checking for duplicate quad ids
duplicated(MBYCeramicsGIS$QuadratID2)

#checking for duplicate quad ids
write.csv(MBYCeramicsGIS, 'MBYCeramics.csv')
