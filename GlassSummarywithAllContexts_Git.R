#R code for analysis of glass assemblages to compare with lithic assemblages from the Hermitage
#Created by: EAB 11/27/2017
#Last Update: EAB 11/27/2017
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


GlassData<-dbGetQuery(DRCcon,'
                        SELECT
                        "public"."tblContext"."ContextID",
                        "public"."tblProjectName"."ProjectName",
                        "public"."tblGlassMaterial"."GlassMaterial",
                        SUM("public"."tblGlass"."Quantity") as "Glass_Count"
                        FROM
                        "public"."tblProject"
                        LEFT JOIN "public"."tblProjectName" ON "public"."tblProject"."ProjectNameID" = "public"."tblProjectName"."ProjectNameID"
                        INNER JOIN "public"."tblContext" ON "public"."tblContext"."ProjectID" = "public"."tblProject"."ProjectID"
                        INNER JOIN "public"."tblContextSample" ON "public"."tblContextSample"."ContextAutoID" = "public"."tblContext"."ContextAutoID"
                        INNER JOIN "public"."tblGenerateContextArtifactID" ON "public"."tblContextSample"."ContextSampleID" = "public"."tblGenerateContextArtifactID"."ContextSampleID"
                        INNER JOIN "public"."tblGlass" ON "public"."tblGlass"."GenerateContextArtifactID" = "public"."tblGenerateContextArtifactID"."GenerateContextArtifactID"
                        INNER JOIN "public"."tblGlassMaterial" ON "public"."tblGlass"."GlassMaterialID" = "public"."tblGlassMaterial"."GlassMaterialID"
                        LEFT JOIN "public"."tblContextFeatureType" ON "public"."tblContext"."FeatureTypeID" = "public"."tblContextFeatureType"."FeatureTypeID"
                        
                        WHERE
                        (("public"."tblGlassMaterial"."GlassMaterial" = \'Non-Lead\') and
                        ("public"."tblProject"."ProjectID" in (\'1400\',\'1401\',\'1402\',\'1403\',\'1404\',\'1405\',\'1406\',\'1407\',\'1410\',\'1412\',\'1413\',\'1414\')))
                        Group by "public"."tblContext"."ContextID",
                        "public"."tblProjectName"."ProjectName",
                        "public"."tblGlassMaterial"."GlassMaterial"                        
                        Order by "public"."tblContext"."ContextID"
                        ')

require(dplyr)
require(plyr)
require(tidyr)

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

#need to rename Quadrat "ECN01"
SpatialData$QuadratID[SpatialData$QuadratID == "'ECN01 '"] <- "ECN01"

#Remove clean up/out of strat context deposits
SpatialData <- filter(SpatialData, DepositType != 'Clean-Up/Out-of-Stratigraphic Context'& DepositType != 'Surface Collection')

#DataAllSites<-full_join(LithicData3, SpatialData, by="ContextID")
#I think this is what I actually want
DataAllSites<-left_join(SpatialData, GlassData, by='ContextID')

#changing QuadIDs to match those for Triplex
DataAllSites$QuadratID2<-ifelse(grepl("TXE5A", DataAllSites$ContextID),
                                paste('TXE5A'),
                                (ifelse(grepl("TXE5B", DataAllSites$ContextID),
                                        paste('TXE5B'),
                                        ifelse(grepl("TXE6B", DataAllSites$ContextID),
                                               paste('TXE6B'),
                                               ifelse(grepl("TXE7C", DataAllSites$ContextID),
                                                      paste('TXE7C'),
                                                      ifelse(grepl("TXE7D", DataAllSites$ContextID),
                                                             paste('TXE7D'),
                                                             ifelse(grepl("TXM1", DataAllSites$ContextID),
                                                                    paste('TXM1'),
                                                                    ifelse(grepl("TXM2", DataAllSites$ContextID),
                                                                           paste('TXM2'),
                                                                           ifelse(grepl("TXM3", DataAllSites$ContextID),
                                                                                  paste('TXM3'),
                                                                                  ifelse(grepl("TXM4", DataAllSites$ContextID),
                                                                                         paste('TXM4'),
                                                                                         ifelse(grepl("TXN1", DataAllSites$ContextID),
                                                                                                paste('TXN1'),
                                                                                                ifelse(grepl("TXN2", DataAllSites$ContextID),
                                                                                                       paste('TXN2'),
                                                                                                       ifelse(grepl("TXN3", DataAllSites$ContextID),
                                                                                                              paste('TXN3'),
                                                                                                              ifelse(grepl("TXN4", DataAllSites$ContextID),
                                                                                                                     paste('TXN4'),
                                                                                                                     ifelse(grepl("TXS4", DataAllSites$ContextID),
                                                                                                                            paste('TXS4'),
                                                                                                                            ifelse(grepl("TXW3", DataAllSites$ContextID),
                                                                                                                                   paste('TXW3'),
                                                                                                                                   ifelse(grepl("TXW6A", DataAllSites$ContextID),
                                                                                                                                          paste('TXW6A'),
                                                                                                                                          paste(DataAllSites$QuadratID)
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
DataAllSites2<-filter(DataAllSites, UnitType == 'Quadrat/Unit')
DataAllSites3<-left_join(DataAllSites2, YC_Tri_Coord, by="QuadratID2") %>%
  mutate(meannorthing=ifelse((ProjectID == '1400' | ProjectID == '1404'),
                             meannorthing.y,
                             meannorthing.x)) %>%
  mutate(meaneasting=ifelse((ProjectID == '1400' | ProjectID == '1404'),
                            meaneasting.y,
                            meaneasting.x)) %>%
  mutate(area_update=ifelse((ProjectID == '1400' | ProjectID == '1404'),
                            area.y,
                            area.x))

GlassDataAllSites4 <-
  select(DataAllSites3, 1:3, 3:7, 22:23, 37:39) %>%
  replace_na(list(Glass_Count=0)) %>%
  mutate(Area=ifelse((ProjectID == '1400' | ProjectID == '1404'),
                     paste('MBY'),
                     ifelse((ProjectID == '1410' | ProjectID == '1412' | ProjectID == '1402'),
                            paste('FH'),
                            paste('FQ')
                     )))

#have to detach plyr package because otherwise it prevents the group_by/summarise functions from working
detach(package:plyr)
#create object for GIS kriging, start by selecting only First Hermitage sites and get rid of a bunch of extraneous columns
FirstHermGlassGIS<-filter(GlassDataAllSites4, Area == 'FH') %>%
  group_by(ProjectID, ProjectName.x, QuadratID2, meannorthing, meaneasting, area_update) %>%
  summarise(Glass_Count2 = sum(Glass_Count)) %>%
  mutate(Glass_Count2 = Glass_Count2 + .5) %>%
  mutate(GlassDensity = Glass_Count2/area_update) %>%
  na.omit

#checking for duplicate quad ids
duplicated(FirstHermGlassGIS$QuadratID2)

#checking for duplicate quad ids
write.csv(FirstHermCeramicsGIS, 'FirstHermGlass.csv')

#same analysis for Field Quarter
FieldQuarterGlassGIS<-filter(GlassDataAllSites4, Area == 'FQ') %>%
  group_by(ProjectID, ProjectName.x, QuadratID2, meannorthing, meaneasting, area_update) %>%
  summarise(Glass_Count2 = sum(Glass_Count)) %>%
  mutate(Glass_Count2 = Glass_Count2 + .5) %>%
  mutate(GlassDensity = Glass_Count2/area_update) %>%
  na.omit

#checking for duplicate quad ids
duplicated(FieldQuarterGlassGIS$QuadratID2)

#checking for duplicate quad ids
write.csv(FieldQuarterGlassGIS, 'FieldQuarterGlass.csv')

#Same analysis for MBY
MBYGlassGIS<-filter(GlassDataAllSites4, Area == 'MBY') %>%
  group_by(ProjectID, ProjectName.x, QuadratID2, meannorthing, meaneasting, area_update) %>%
  summarise(Glass_Count2 = sum(Glass_Count)) %>%
  mutate(Glass_Count2 = Glass_Count2 + .5) %>%
  mutate(GlassDensity = Glass_Count2/area_update) %>%
  na.omit

#checking for duplicate quad ids
duplicated(MBYGlassGIS$QuadratID2)

#checking for duplicate quad ids
write.csv(MBYGlassGIS, 'MBYGlass.csv')

