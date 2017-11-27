#R code for analysis of lithic assemblages from the Hermitage
#Created by: EAB 11/18/2017
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


LithicData<-dbGetQuery(DRCcon,'
SELECT
"public"."tblContext"."ContextID",
"i"."Quantity" as "Count",
"k"."GenArtifactForm" as "Form",
string_agg(distinct COALESCE("ma"."GenArtifactMaterialType")||\', \'||COALESCE("n"."GenArtifactManuTech"), \'; \') as "MaterialandManufacturingTechnique",
"i"."Weight",
"i"."Notes"
                       
FROM
"public"."tblProject"
LEFT JOIN "public"."tblProjectName" ON "public"."tblProject"."ProjectNameID" = "public"."tblProjectName"."ProjectNameID"
INNER JOIN "public"."tblContext" ON "public"."tblContext"."ProjectID" = "public"."tblProject"."ProjectID"
INNER JOIN "public"."tblContextSample" ON "public"."tblContextSample"."ContextAutoID" = "public"."tblContext"."ContextAutoID"
INNER JOIN "public"."tblGenerateContextArtifactID" ON "public"."tblContextSample"."ContextSampleID" = "public"."tblGenerateContextArtifactID"."ContextSampleID"
INNER JOIN "public"."tblGenArtifact" as "i" ON "public"."tblGenerateContextArtifactID"."ArtifactID" = "i"."ArtifactID"
INNER JOIN "public"."tblGenArtifactForm" as "k" ON "i"."GenArtifactFormID" = "k"."GenArtifactFormID"
INNER JOIN "public"."tblGenArtifactMaterial" as "m" ON "m"."GenerateContextArtifactID" = "i"."GenerateContextArtifactID"
left join "public"."tblGenArtifactMaterialType" as "ma" on "m"."GenArtifactMaterialTypeID" = "ma"."GenArtifactMaterialTypeID"
INNER JOIN "public"."tblGenArtifactManuTech" as "n" ON "m"."GenArtifactManuTechID" = "n"."GenArtifactManuTechID"
WHERE
"public"."tblProject"."ProjectID" in (\'1400\',\'1401\',\'1402\',\'1403\',\'1404\',\'1405\',\'1406\',\'1407\',\'1410\',\'1412\',\'1413\',\'1414\') AND ("k"."GenArtifactForm" LIKE (\'Flake%\') OR
 "k"."GenArtifactForm" LIKE (\'Point%\') OR "k"."GenArtifactForm" = (\'Shatter\') OR "k"."GenArtifactForm" IN (\'Abrader\', \'Adze\', \'Biface\', \'Blank, stone tool\', \'Gorget\',\'Uniface\') OR ("k"."GenArtifactForm" = \'Tool, unidentified\' AND "ma"."GenArtifactMaterialType" IN (\'Chert/Flint, grey/black\', \'Chert/Flint, other\', \'Stone, unidentified\', \'Stone, unid sedimentary\', \'Stone, unid metamorphic\')))
GROUP BY
"public"."tblContext"."ContextID",
"k"."GenArtifactForm",
"i"."Quantity",
"i"."Weight",
"i"."Notes"

ORDER BY
"public"."tblContext"."ContextID" ASC
')

require(dplyr)
require(plyr)
require(tidyr)

#Have to get rid of forms with commas because they will become column names later in code
LithicData$Form[LithicData$Form == "Flake, retouched"] <- "Flake_retouched"
LithicData$Form[LithicData$Form == "Flake, cortical"] <- "Flake_cortical"
LithicData$Form[LithicData$Form == "Point, side notched"] <- "Point_SideNotched"
LithicData$Form[LithicData$Form == "Point, corner notched"] <- "Point_CornerNotched"
LithicData$Form[LithicData$Form == "Point, base notched"] <- "Point_BaseNotched"
LithicData$Form[LithicData$Form == "Point, stemmed"] <- "Point_Stemmed"
LithicData$Form[LithicData$Form == "Point, triangular"] <- "Point_Triangular"
LithicData$Form[LithicData$Form == "Point, lanceolate"] <- "Point_Lanceolate"
LithicData$Form[LithicData$Form == "Point, unidentified"] <- "Point_Unid"
LithicData$Form[LithicData$Form == "Tool, unidentified"] <- "Tool_Unid"


LithicData2 <- ddply(LithicData, .(ContextID, Form), summarise, Count=sum(Count))
#reshape data
LithicData3<-LithicData2 %>%
  spread(Form, Count)


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
write.csv(SpatialData, 'LithicsSpatialData.csv')

#need to rename Quadrat "ECN01"
SpatialData$QuadratID[SpatialData$QuadratID == "'ECN01 '"] <- "ECN01"

#Remove clean up/out of strat context deposits
SpatialData <- filter(SpatialData, DepositType != 'Clean-Up/Out-of-Stratigraphic Context'& DepositType != 'Surface Collection')

#DataAllSites<-full_join(LithicData3, SpatialData, by="ContextID")
#I think this is what I actually want
DataAllSites<-left_join(SpatialData, LithicData3, by='ContextID')

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
#Select columns that are useful for GIS analysis
DataAllSites4 <-
  select(DataAllSites3, 1:7, 20:33, 47:49) %>%
  replace_na(list(Biface = 0, Flake = 0, Flake_cortical = 0, Flake_retouched = 0, 
                  Point_BaseNotched = 0, Point_CornerNotched = 0, Point_Lanceolate = 0, Point_SideNotched = 0, Point_Stemmed =0,
                  Point_Triangular = 0, Point_Unid = 0, Shatter = 0, Tool_Unid = 0)) %>%
  mutate(Area=ifelse((ProjectID == '1400' | ProjectID == '1404'),
                         paste('MBY'),
                         ifelse((ProjectID == '1410' | ProjectID == '1412' | ProjectID == '1402'),
                                paste('FH'),
                                paste('FQ')
                         )))

#---------------------------Creating Objects for Kriging
#create object for GIS kriging, start by selecting only First Hermitage sites and get rid of a bunch of extraneous columns
FirstHermGIS<-filter(DataAllSites4, Area == 'FH') %>%
  select(1:3,8:25) 

#pairing it down to only a few columns to do data check on count
#FirstHermGIS2<-filter(DataAllSites4, Area == 'FH') %>%
#  na.omit() %>%
#  select(1:2, 17, 29)

#have to detach plyr package because otherwise it prevents the group_by/summarise functions from working
detach(package:plyr)
#FirstHermGIS3<- FirstHermGIS2 %>%
#  group_by(QuadratID2)%>%
#  summarise(count=sum(Flake))

#another way of checking for issues with northing and easting coords
#QuadAvg<-FirstHermGIS %>%
#  group_by(ProjectID, QuadratID2, meannorthing, meaneasting, area) %>%
#  summarise(count=sum(Flake))

#create object for gis analysis, group by columns of interest,
#summarise count by quadratid, add .5 to counts to get rid of zeros (ArcGIS kriging can't handle zeros)
#then calculate densities and omit NAs
FirstHermGIS4<- FirstHermGIS %>%
  group_by(ProjectID, ProjectName, QuadratID2, meannorthing, meaneasting, area_update)%>%
  summarise(count=sum(Flake)) %>%
  mutate(count = count + .5) %>%
  mutate(FlakeDensity = count/area_update) %>%
  na.omit

#checking for duplicate quad ids
duplicated(FirstHermGIS4$QuadratID2)
  
write.csv(FirstHermGIS4, 'FirstHermFlakeDensity2.csv')

#now doing the same analysis for the field quarter, have put this all in a single
#pipe

#create object for gis analysis, group by columns of interest,
#summarise count by quadratid, add .5 to counts to get rid of zeros (ArcGIS kriging can't handle zeros)
#then calculate densities and omit NAs
FieldQuarterGIS<-filter(DataAllSites4, Area == 'FQ') %>%
  select(1:3,8:25) %>%
  group_by(ProjectID, ProjectName, QuadratID2, meannorthing, meaneasting, area_update) %>%
  summarise(count=sum(Flake)) %>%
  mutate(count = count + .5) %>%
  mutate(FlakeDensity = count/area_update)  %>%
  na.omit()

#checking for duplicate quad ids
duplicated(FieldQuarterGIS$QuadratID2)

write.csv(FieldQuarterGIS, 'FieldQuarterFlakeDensity.csv')


#now doing the same analysis for the MBY

#create object for gis analysis, group by columns of interest,
#summarise count by quadratid, add .5 to counts to get rid of zeros (ArcGIS can't handle zeros)
#then calculate densities
MBYGIS<-filter(DataAllSites4, Area == 'MBY') %>%
  select(1:3,8:25) %>%
  group_by(ProjectID, ProjectName, QuadratID2, meannorthing, meaneasting, area_update) %>%
  summarise(count=sum(Flake)) %>%
  mutate(count = count + .5) %>%
  mutate(FlakeDensity = count/area_update) %>%
  na.omit()

#checking for duplicate quad ids
duplicated(MBYGIS$QuadratID2)

write.csv(MBYGIS, 'MBYFlakeDensity.csv')

#--------------------------Point Analysis Data------------------------
#Calculating Projectile Point densities for First Hermitage, same process as with flakes but with points
FirstHermPoints<-filter(DataAllSites4, Area == 'FH') %>%
  select(1:3,12:18,21:25) %>%
  group_by(ProjectID, ProjectName, QuadratID2, meannorthing, meaneasting, area_update) %>%
  #summarise statement sums all point counts
  summarise(PointCount=sum(Point_BaseNotched, Point_SideNotched, Point_CornerNotched, Point_Lanceolate, Point_Stemmed, Point_Triangular, Point_Unid)) %>%
  mutate(PointCount = PointCount + .5) %>%
  mutate(PointDensity = PointCount/area_update) %>%
  na.omit()

#checking for duplicate quad ids
duplicated(FirstHermPoints$QuadratID2)

write.csv(FirstHermPoints, 'FirstHermPoints.csv')


#Calculating Projectile Point densities for Field Quarter, same process as with flakes but with points
FieldQuarterPoints<-filter(DataAllSites4, Area == 'FQ') %>%
  select(1:3,12:18,21:25) %>%
  group_by(ProjectID, ProjectName, QuadratID2, meannorthing, meaneasting, area_update) %>%
  #summarise statement sums all point counts
  summarise(PointCount=sum(Point_BaseNotched, Point_SideNotched, Point_CornerNotched, Point_Lanceolate, Point_Stemmed, Point_Triangular, Point_Unid)) %>%
  mutate(PointCount = PointCount + .5) %>%
  mutate(PointDensity = PointCount/area_update) %>%
  na.omit()

#checking for duplicate quad ids
duplicated(FieldQuarterPoints$QuadratID2)

write.csv(FieldQuarterPoints, 'FieldQuarterPoints.csv')


#Calculating Projectile Point densities for MBY, same process as with flakes but with points
MBYPoints<-filter(DataAllSites4, Area == 'MBY') %>%
  select(1:3,12:18,21:25) %>%
  group_by(ProjectID, ProjectName, QuadratID2, meannorthing, meaneasting, area_update) %>%
  #summarise statement sums all point counts
  summarise(PointCount=sum(Point_BaseNotched, Point_SideNotched, Point_CornerNotched, Point_Lanceolate, Point_Stemmed, Point_Triangular, Point_Unid)) %>%
  mutate(PointCount = PointCount + .5) %>%
  mutate(PointDensity = PointCount/area_update) %>%
  na.omit()

#checking for duplicate quad ids
duplicated(MBYPoints$QuadratID2)

write.csv(MBYPoints, 'MBYPoints.csv')
