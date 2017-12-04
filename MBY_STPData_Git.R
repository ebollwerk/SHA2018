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

MBYLithicSTPData<-dbGetQuery(DRCcon,'
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
                          "public"."tblProject"."ProjectID" in (\'1408\') AND ("k"."GenArtifactForm" LIKE (\'Flake%\') OR
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
MBYLithicSTPData$Form[MBYLithicSTPData$Form == "Flake, cortical"] <- "Flake_cortical"
MBYLithicSTPData$Form[MBYLithicSTPData$Form == "Flake, retouched"] <- "Flake_retouched"
MBYLithicSTPData$Form[MBYLithicSTPData$Form == "Point, unidentified"] <- "Point_Unid"

#reshape data
MBYLithicSTPData2<-MBYLithicSTPData %>%
  spread(Form, Count)



MBYSpatialData <-dbGetQuery(DRCcon,'
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
colnames(MBYSpatialData)<- c("ContextID","Northing","Easting")

#read in Coordinate data transformed to Plantation Grid
YC_Tri_CoordSTPs<-read.csv(file="MBY_STP_PlantationGridTranslation.csv", header=TRUE, sep=",")
MBYSpatialData2<-left_join(MBYSpatialData, YC_Tri_CoordSTPs, by= c("Northing" = "Northing", "Easting" = "Easting")) 

#create dataset for kriging, join lithics data and spatial data, select columns, replace NAs with 0s and then add .5
MBYSTPLithicData<-left_join(MBYSpatialData2, MBYLithicSTPData2, by='ContextID') %>%
  select(1,4:5,11:16) %>%
  replace_na(list(Biface = 0, Flake = 0, Flake_cortical = 0, Flake_retouched = 0,
                  Point_Unid = 0, Shatter = 0)) %>%
  group_by(ContextID, NorthingPlantation, EastingPlantation) %>%
  summarise(Count=sum(Biface,Flake,Flake_cortical,Flake_retouched,Point_Unid,Shatter)) %>%
  mutate(Count = Count + .5)

#check for duplicate ContextIDs
duplicated(MBYSTPLithicData$ContextID)

#write out file for ArcGIS
write.csv(MBYSTPLithicData, 'MBYSTPLithicData.csv')


