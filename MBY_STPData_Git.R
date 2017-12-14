#R code for analysis of prehistoric tools (Points, Scrapers, Biface, Groundstone etc.) from MBY Hermitage
#Created by: EAB 11/27/2017
#Last Update: EAB 11/27/2017
#load the library
library(DBI)
require(RPostgreSQL)
library(RODBC)

####Query for prehistoric data from DAACS####
# tell DBI which driver to use
pgSQL <- dbDriver("PostgreSQL")
# establish the connection
DRCcon<-dbConnect(pgSQL, host=connection$host, port=connection$port,
                  dbname=connection$dbname,
                  user=connection$user, password=connection$password)

MBYPrehistoricSTPData<-dbGetQuery(DRCcon,'
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
                          "public"."tblProject"."ProjectID" in (\'1408\') AND ("k"."GenArtifactForm" LIKE (\'Flake%\') OR "k"."GenArtifactForm" LIKE (\'Point%\') OR "k"."GenArtifactForm" = (\'Shatter\') OR "k"."GenArtifactForm" IN (\'Shatter\', \'Abrader\', \'Adze\', \'Biface\', \'Blank, stone tool\', \'Gorget\',\'Uniface\',\'Tool, unidentified\', \'Scraper\', \'Grinding Stone\', \'Drill\',\'Core\',\'Hammerstone\'))
                          GROUP BY
                          "public"."tblContext"."ContextID",
                          "k"."GenArtifactForm",
                          "i"."Weight",
                          "i"."Notes"
                          
                          ORDER BY
                          "public"."tblContext"."ContextID" ASC
                          ')

#summarize data by form to see what forms are present
MBYPrehistoricSTPDataSum<-group_by(MBYPrehistoricSTPData, Form) %>% 
  summarise(Count=sum(Count))

####Lithic Analysis####
require(dplyr)
require(tidyr)

#Have to get rid of forms with commas because they will become column names later in code
MBYPrehistoricSTPData$Form[MBYPrehistoricSTPData$Form == "Flake, cortical"] <- "Flake_cortical"
MBYPrehistoricSTPData$Form[MBYPrehistoricSTPData$Form == "Flake, retouched"] <- "Flake_retouched"
MBYPrehistoricSTPData$Form[MBYPrehistoricSTPData$Form == "Point, unidentified"] <- "Point_Unid"

#reshape data
MBYPrehistoricSTPData2<-MBYPrehistoricSTPData %>%
  spread(Form, Count)



MBYSTPSpatialData <-dbGetQuery(DRCcon,'
                        SELECT
                            "public"."tblContext"."ContextID",
                            "public"."tblContext"."STPNorthing",
                            "public"."tblContext"."STPEasting"
                            
                            FROM
                            "public"."tblContext"
                            
                            WHERE
                            "public"."tblContext"."ProjectID" = \'1408\'
                            GROUP BY
                            "public"."tblContext"."ContextID",
                            "public"."tblContext"."STPNorthing",
                            "public"."tblContext"."STPEasting"
                            ')
#Rename N and E
colnames(MBYSTPSpatialData)<- c("ContextID","Northing","Easting")

#read in Coordinate data transformed to Plantation Grid
YC_Tri_CoordSTPs<-read.csv(file="MBY_STP_PlantationGridTranslation.csv", header=TRUE, sep=",")
MBYSTPSpatialData2<-left_join(MBYSTPSpatialData, YC_Tri_CoordSTPs, by= c("Northing" = "Northing", "Easting" = "Easting")) 

#create dataset for kriging, join lithics data and spatial data, select columns, replace NAs with 0s and then add .5
MBYSTPPrehistoricData<-left_join(MBYSTPSpatialData2, MBYPrehistoricSTPData2, by='ContextID') %>%
  select(ContextID, NorthingPlantation, EastingPlantation, Biface, Core, Flake, Flake_cortical, Flake_retouched,
         Point_Unid, Shatter) %>%
  replace_na(list(Biface = 0, Core = 0, Flake = 0, Flake_cortical = 0, Flake_retouched = 0,
                  Point_Unid = 0, Shatter = 0)) %>%
  group_by(ContextID, NorthingPlantation, EastingPlantation) %>%
  summarise(Count=sum(Biface,Flake,Flake_cortical,Flake_retouched,Point_Unid,Shatter)) %>%
  mutate(Count = Count + .5)

#check for duplicate ContextIDs
duplicated(MBYSTPPrehistoricData$ContextID)

#write out file for ArcGIS
write.csv(MBYSTPPrehistoricData, 'MBYSTPPrehistoricData.csv')


####Ceramic Analysis####
MBYSTPCeramicData<-dbGetQuery(DRCcon,'
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
                        ("public"."tblProject"."ProjectID" in (\'1408\')))
                        Group by "public"."tblContext"."ContextID",
                        "public"."tblProjectName"."ProjectName",
                        "public"."tblCeramicMaterial"."CeramicMaterial"                        
                        Order by "public"."tblContext"."ContextID"
                        ')


MBYSTPCeramicData2<-left_join(MBYSpatialData2, MBYSTPCeramicData, by='ContextID') %>%
  select(1,4:5,10) %>%
  replace_na(list(REW_Count=0)) %>%
  mutate(REW_Count = REW_Count + .5)

#check for duplicate ContextIDs
duplicated(MBYSTPCeramicData2$ContextID)

#write out file for ArcGIS
write.csv(MBYSTPCeramicData2, 'MBYSTPCeramicData2.csv')


####Glass Analysis####

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
                         ("public"."tblContext"."ProjectID" = \'1408\' and "public"."tblGlassMaterial"."GlassMaterial" = \'Non-Lead\') 
                         Group by "public"."tblContext"."ContextID",
                         "public"."tblProjectName"."ProjectName",
                         "public"."tblGlassMaterial"."GlassMaterial"   
                         ORDER BY
                         "public"."tblContext"."ContextID" ASC
                         ')

#No need to reshape, only one form

MBYSTPGlassData<-left_join(MBYSpatialData2, GlassSTPData, by='ContextID') %>%
  select(1,4:5,10) %>%
  replace_na(list(Glass_Count = 0)) %>%
  mutate(Glass_Count = Glass_Count + .5)

#check for duplicate ContextIDs
duplicated(MBYSTPGlassData$ContextID)

#write out file for ArcGIS
write.csv(MBYSTPGlassData, 'MBYSTPGlassData.csv')
