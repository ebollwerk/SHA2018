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

require (dplyr)
require (tidyr)

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

require (dplyr)
require (tidyr)

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
#Do Northing and Easting Averages like in the query
YC_Tri_Coord<-mutate(YC_Tri_Coord, meannorthing=rowMeans(select(YC_Tri_Coord, contains("Y"))))
YC_Tri_Coord<-mutate(YC_Tri_Coord, meaneasting=rowMeans(select(YC_Tri_Coord, contains("X"))))

require(tibble)
#merge new Northings and Eastings with original dataset
#if unit is one from triplex or yard cabin put meannorthing, easting from spreadsheet, otherwise put in northing easting from
#original dataset
YC_Tri_Coord<-rename(YC_Tri_Coord, QuadratID2 = QuadID)
DataAllSites2<-filter(DataAllSites, UnitType == 'Quadrat/Unit')
DataAllSites3<-left_join(DataAllSites2, YC_Tri_Coord, by="QuadratID2") %>%
  add_column(meannorthing="") %>%
  add_column(meaneasting="")


DataAllSites3$meannorthing<-ifelse((DataAllSites3$ProjectID == '1400' | DataAllSites3$ProjectID == '1404'),
                                 DataAllSites3$meannorthing.y,
                                 DataAllSites3$meannorthing.x)

DataAllSites3$meaneasting<-ifelse((DataAllSites3$ProjectID == '1400' | DataAllSites3$ProjectID == '1404'),
                                DataAllSites3$meaneasting.y,
                                DataAllSites3$meaneasting.x)


DataAllSites4 <-
  select (DataAllSites3, 1:7, 10:13, 16:33, 23:33, 44:45) 

#figure out how to replace NAs with zeros only for certain columns
replace_na(list(16:28 = 0))

#Get rid of the NAs in Total column
Site6ALL[is.na(Site6ALL)] <- 0

#This step adds 0.5 to all counts for kriging! 
Site6ALL[,6] <- Site6ALL[,6] + 0.5


write.csv(DataAllSites, 'DataAllSites.csv')

                        