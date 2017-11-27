#R code for analysis of lithic assemblages from First Quarter STP Survey at the Hermitage
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

LithicSTPData<-dbGetQuery(DRCcon,'
SELECT
                       "public"."tblContext"."ContextID",
                       Sum("i"."Quantity") AS "Count",
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
                       "public"."tblProject"."ProjectID" in (\'1409\') AND ("k"."GenArtifactForm" LIKE (\'Flake%\') OR
                       "k"."GenArtifactForm" LIKE (\'Point%\') OR "k"."GenArtifactForm" = (\'Shatter\') OR "k"."GenArtifactForm" IN (\'Abrader\', \'Adze\', \'Biface\', \'Blank, stone tool\', \'Gorget\',\'Uniface\') OR ("k"."GenArtifactForm" = \'Tool, unidentified\' AND "ma"."GenArtifactMaterialType" IN (\'Chert/Flint, grey/black\', \'Chert/Flint, other\', \'Stone, unidentified\', \'Stone, unid sedimentary\', \'Stone, unid metamorphic\')))
                       GROUP BY
                       "public"."tblContext"."ContextID",
                       "k"."GenArtifactForm",
                       "i"."Weight",
                       "i"."Notes"
                       
                       ORDER BY
                       "public"."tblContext"."ContextID" ASC
                       ')


require(dplyr)
require(tidyr)

#Have to get rid of forms with commas because they will become column names later in code
LithicSTPData$Form[LithicSTPData$Form == "Flake, cortical"] <- "Flake_cortical"
LithicSTPData$Form[LithicSTPData$Form == "Point, corner notched"] <- "Point_CornerNotched"
LithicSTPData$Form[LithicSTPData$Form == "Point, stemmed"] <- "Point_Stemmed"
LithicSTPData$Form[LithicSTPData$Form == "Point, unidentified"] <- "Point_Unid"


FQSpatialData <-dbGetQuery(DRCcon,'
                        SELECT
                            "public"."tblContext"."ContextID",
                            "public"."tblContext"."STPNorthing",
                            "public"."tblContext"."STPEasting"
                            
                            FROM
                            "public"."tblContext"
                            
                            WHERE
                            "public"."tblContext"."ProjectID" = \'1409\'
                            GROUP BY
                            "public"."tblContext"."ContextID",
                            "public"."tblContext"."STPNorthing",
                            "public"."tblContext"."STPEasting"
                            ')
#Rename N and E
colnames(FQSpatialData)<- c("ContextID","Northing","Easting")
#reshape data
LithicSTPData2<-LithicSTPData %>%
  spread(Form, Count)


CombinedLithicSTPData<-left_join(FQSpatialData, LithicSTPData2, by='ContextID') %>%
  select(1:3, 7:13) %>%
  replace_na(list(Biface = 0, Flake = 0, Flake_cortical = 0, 
                  Point_CornerNotched = 0,
                  Point_Stemmed = 0, Point_Unid = 0, Shatter = 0)) %>%
  group_by(ContextID, Northing, Easting) %>%
  summarise(Count=sum(Biface,Flake,Flake_cortical,Point_CornerNotched,Point_Stemmed,Point_Unid,Shatter)) %>%
  mutate(Count = Count + .5)

duplicated(CombinedLithicSTPData$ContextID)
  
write.csv(CombinedLithicSTPData, 'CombinedSTPData_AllLithics.csv')

#-------------------------------------------ceramic Analysis----------------------------------------------------
CeramicSTPData<-dbGetQuery(DRCcon,'
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
                         ("public"."tblContext"."ProjectID" = \'1409\' and "public"."tblCeramicMaterial"."CeramicMaterial" = \'Refined EW\') 
                         Group by "public"."tblContext"."ContextID",
                         "public"."tblProjectName"."ProjectName",
                         "public"."tblCeramicMaterial"."CeramicMaterial"   
                          ORDER BY
                         "public"."tblContext"."ContextID" ASC
                         ')

#No need to reshape, only one form

CombinedREWSTPData<-left_join(FQSpatialData, CeramicSTPData, by='ContextID') %>%
  replace_na(list(REW_Count = 0)) %>%
  group_by(ContextID, Northing, Easting) %>%
  summarise(REW_Count=sum(REW_Count)) %>%
  mutate(REW_Count = REW_Count + .5)

duplicated(CombinedCeramicSTPData$ContextID)

write.csv(CombinedREWSTPData, 'CombinedSTPData_REW.csv')

#-------------------------------------------Glass Analysis----------------------------------------------------
GlassSTPData<-dbGetQuery(DRCcon,'
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
                         LEFT JOIN "public"."tblContextFeatureType" ON "public"."tblContext"."FeatureTypeID" = "public"."tblContextFeatureType"."FeatureTypeID"                           WHERE
                           ("public"."tblContext"."ProjectID" = \'1409\' and "public"."tblGlassMaterial"."GlassMaterial" = \'Non-Lead\') 
                           Group by "public"."tblContext"."ContextID",
                           "public"."tblProjectName"."ProjectName",
                           "public"."tblGlassMaterial"."GlassMaterial"   
                           ORDER BY
                           "public"."tblContext"."ContextID" ASC
                           ')

#No need to reshape, only one form

CombinedGlassSTPData<-left_join(FQSpatialData, GlassSTPData, by='ContextID') %>%
  replace_na(list(Glass_Count = 0)) %>%
  group_by(ContextID, Northing, Easting) %>%
  summarise(Glass_Count=sum(Glass_Count)) %>%
  mutate(Glass_Count = Glass_Count + .5)

duplicated(CombinedGlassSTPData$ContextID)

write.csv(CombinedGlassSTPData, 'CombinedSTPData_Glass.csv')
