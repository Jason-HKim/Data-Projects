# Databricks notebook source
# importing necessary libraries
import pyspark.sql.functions as f
import pandas as pd

# COMMAND ----------

# bucks = ("/FileStore/tables/bucks.csv")
# raptors = ("/FileStore/tables/raptors.csv")
# celtics = ("/FileStore/tables/celtics.csv")
# pacers = ("/FileStore/tables/pacers.csv")
# heat = ("/FileStore/tables/heat.csv")
# sixers = ("/FileStore/tables/sixers.csv")
# nets = ("/FileStore/tables/nets.csv")
# magic = ("/FileStore/tables/magic.csv")

# filelist = [bucks, raptors, celtics, pacers, heat, sixers, nets, magic]
# filelist[0]

# COMMAND ----------

# assigning csv files on dbfs to team variable
# bucks = ("/FileStore/tables/bucks.csv")
# raptors = ("/FileStore/tables/raptors.csv")
# celtics = ("/FileStore/tables/celtics.csv")
# pacers = ("/FileStore/tables/pacers.csv")
# heat = ("/FileStore/tables/heat.csv")
# sixers = ("/FileStore/tables/76ers.csv")
# nets = ("/FileStore/tables/nets.csv")
# magic = ("/FileStore/tables/magic.csv")

# reading in all necessary csv files into DataFrames
# bucksdf = (spark.read
#            .option("inferSchema", False)
#            .option("header", True)
#            .csv(bucks)
#            .withColumn("Team", f.lit("Bucks")
#           )
# raptorsdf = (spark.read
#            .option("inferSchema", False)
#            .option("header", True)
#            .csv(raptors)
#            .withColumn("Team", f.lit("Raptors")
#           )
# celticsdf = (spark.read
#            .option("inferSchema", False)
#            .option("header", True)
#            .csv(celtics)
#            .withColumn("Team", f.lit("Celtics")
#           )
# pacersdf = (spark.read
#            .option("inferSchema", False)
#            .option("header", True)
#            .csv(pacers)
#            .withColumn("Team", f.lit("Pacers")
#           )
# heatdf = (spark.read
#            .option("inferSchema", False)
#            .option("header", True)
#            .csv(heat)
#            .withColumn("Team", f.lit("Heat")
#           )
# sixersdf = (spark.read
#            .option("inferSchema", False)
#            .option("header", True)
#            .csv(sixers)
#            .withColumn("Team", f.lit("76ers")
#           )
# netsdf = (spark.read
#            .option("inferSchema", False)
#            .option("header", True)
#            .csv(nets)
#            .withColumn("Team", f.lit("Nets")
#           )
# magicdf = (spark.read
#            .option("inferSchema", False)
#            .option("header", True)
#            .csv(magic)
#            .withColumn("Team", f.lit("Magic")
#           )

# filelist = [bucks, raptors, celtics, pacers, heat, sixers, nets, magic]
# teamdfnames = ['bucksdf', 'raptorsdf', 'celticsdf', 'pacersdf', 'heatdf', 'sixersdf', 'netsdf', 'magicdf']

# COMMAND ----------

from functools import reduce
# creating variables for file path and list to be used in following loop
path = "/FileStore/tables/"
filelist = ['bucks', 'raptors', 'celtics', 'pacers', 'heat', 'sixers', 'nets', 'magic']

# assigning csv files to key value pairs in a dictionary and adding a new column with team name
# teamdict={}
# filecounter = 0
appendedfiles = []
for n in filelist:
#   dfname = n + 'df'
  file = (spark.read
             .option("inferSchema", False)
             .option("header", True)
             .csv(path+str(n)+'.csv').withColumn("Team", f.lit(n)))
  appendedfiles.append(file)
  
combineddf = reduce(appendedfiles, union)
#   filecounter+=1
#   if filecounter > 0:
    
#   teamdict[dfname] = file

# COMMAND ----------



# COMMAND ----------

bucksdf = (spark.read
            .option("inferSchema", False)
            .option("header", True)
           .csv(filelist[0]))
display(bucksdf)

# COMMAND ----------

# # assigning new column with team name to each DataFrame
# bucksdf = (bucksdf
#                    .withColumn("Team", f.lit("Bucks"))
#                   )

# raptorsdf = (raptorsdf
#                    .withColumn("Team", f.lit("Raptors"))
#                   )

# celticsdf = (celticsdf
#                    .withColumn("Team", f.lit("Celtics"))
#                   )

# pacersdf = (pacersdf
#                    .withColumn("Team", f.lit("Pacers"))
#                   )

# heatdf = (heatdf
#                    .withColumn("Team", f.lit("Heat"))
#                   )

# sixersdf = (sixersdf
#                    .withColumn("Team", f.lit("76ers"))
#                   )

# netsdf = (netsdf
#                    .withColumn("Team", f.lit("Nets"))
#                   )

# magicdf = (magicdf
#                    .withColumn("Team", f.lit("Magic"))
#                   )

# for k, v in teamdict.iteritems():
#   for team in filelist:
#     teamdict[k] = teamdict[v].withColumn("Team", f.lit(team))
# combining all dataframes after applying necessary transformations
combineddf = bucksdf.unionAll(raptorsdf).unionAll(celticsdf).unionAll(pacersdf).unionAll(heatdf).unionAll(sixersdf).unionAll(netsdf).unionAll(magicdf)
display(combineddf)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# teamlist = [bucksdf, raptorsdf, celticsdf, pacersdf, heatdf, sixersdf, netsdf, magicdf]
testschema = None
testdf = sc.createDataFrame(sc.emptyRDD(), testschema)
for i in teamlist:
  testdf.append(i)

# COMMAND ----------

from pyspark.sql.types import IntegerType, FloatType
list_of_ints = ["Age", "G", "GS", "MP", "FG", "FGA", "3P", "3PA", "2P", "2PA", "FT", "FTA", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS"]
list_of_floats = ["FG%", "3P%", "2P%", "eFG%", "FT%"]

for column in list_of_ints:
  combineddf = (combineddf
                .withColumn(column, f.col(column).cast(IntegerType()))
               )
for column in list_of_floats:
  combineddf = (combineddf
                .withColumn(column, f.col(column).cast(FloatType()))
               )

# COMMAND ----------

from pyspark.sql.functions import desc
combineddf = (combineddf
              .withColumn("Player", f.split("Player", "\\\\").getItem(0))
              .withColumn("Player", f.when((f.col("Player").rlike("Ersan")), f.lit("Ersan İlyasova")).otherwise(f.col("Player")))
              .withColumn("Player", f.when((f.col("Player").rlike("Goran")), f.lit("Goran Dragić")).otherwise(f.col("Player")))
              .withColumn("Player", f.when((f.col("Player").rlike("Luwawu")), f.lit("Timothé Luwawu-Cabarrot")).otherwise(f.col("Player")))
              .withColumn("Player", f.when((f.col("Player").rlike("Musa")), f.lit("Džanan Musa")).otherwise(f.col("Player")))
              .withColumn("Player", f.when((f.col("Player").rlike("Nikola")), f.lit("Nikola Vučević")).otherwise(f.col("Player")))
              .sort(desc("PTS"))
              .drop("Rk")
             )
display(combineddf)

# COMMAND ----------

tsa = f.col("FGA") + 0.44*f.col("FTA")
tspct = f.col("PTS")/(2*(tsa))

gameavgdf = (combineddf
             .withColumn("GameMP", f.col("MP") / f.col("G"))
             .withColumn("GamePTS", f.col("PTS") / f.col("G"))
             .withColumn("GameFG", f.col("FG") / f.col("G"))
             .withColumn("GameFGA", f.col("FGA") / f.col("G"))
             .withColumn("Game3P", f.col("3P") / f.col("G"))
             .withColumn("Game3PA", f.col("3PA") / f.col("G"))
             .withColumn("Game2P", f.col("2P") / f.col("G"))
             .withColumn("Game2PA", f.col("2PA") / f.col("G"))
             .withColumn("GameFT", f.col("FT") / f.col("G"))
             .withColumn("GameFTA", f.col("FTA") / f.col("G"))
             .withColumn("GameORB", f.col("ORB") / f.col("G"))
             .withColumn("GameDRB", f.col("DRB") / f.col("G"))
             .withColumn("GameTRB", f.col("TRB") / f.col("G"))
             .withColumn("GameAST", f.col("AST") / f.col("G"))
             .withColumn("GameSTL", f.col("STL") / f.col("G"))
             .withColumn("GameBLK", f.col("BLK") / f.col("G"))
             .withColumn("GameTOV", f.col("TOV") / f.col("G"))
             .withColumn("GamePF", f.col("PF") / f.col("G"))
             .withColumn("TS%", tspct)
#              .drop("MP", "FG", "FGA", "FG%", "3P", "3PA", "3P%", "2P", "2PA", "2P%", "eFG%", "FT", "FTA", "FT%", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "PF", "PTS", "TOV")
            )
display(gameavgdf.sort(desc("GamePTS")))

# COMMAND ----------

gameavgdf = (gameavgdf
            .withColumn("PPM", f.col("PTS")/f.col("MP"))
            )
display(gameavgdf.sort(desc("PPM")).filter(gameavgdf.GameMP>=5))

# COMMAND ----------

print(gameavgdf[['Player', 'GamePTS']].sort(desc("GamePTS")).take(5))

# COMMAND ----------

avgpie = ((f.col("PTS") + f.col("FG") + f.col("FT") - f.col("FGA") - f.col("FTA") + f.col("DRB") + (f.col("ORB")/2) + f.col("AST") + f.col("STL") + (f.col("BLK")/2) - f.col("PF") - f.col("TOV")) 
                           / (f.col("GamePTS") + f.col("GameFG") + f.col("GameFT") - f.col("GameFGA") - f.col("GameFTA") + f.col("GameDRB") + (f.col("GameORB")/2) + f.col("GameAST") 
                           + f.col("GameSTL") + (f.col("GameBLK")/2) - f.col("GamePF") - f.col("GameTOV")))

simple_per = (f.col("2P")*2 - f.col("2PA")*0.75 + f.col("3P")*3 - f.col("3PA")*0.84 + f.col("FT") - f.col("FTA")*(-.65) + f.col("TRB") + f.col("AST") + f.col("BLK") + f.col("STL") - f.col("TOV")) / f.col("G") 

playerimpactdf = (gameavgdf
                  .withColumn("AVG_PIE", avgpie)
                  .withColumn("Simple_PER", simple_per)
                  .drop("MP", "FG", "FGA", "FG%", "3P", "3PA", "3P%", "2P", "2PA", "2P%", "eFG%", "FT", "FTA", "FT%", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "PF", "PTS", "TOV")
                  .sort(desc("Simple_PER"))
                 )

display(playerimpactdf)

# COMMAND ----------

teamdf = (combineddf
          .groupBy(f.col("Team"))
          .sum("FG", "FGA", "3P", "3PA", "2P", "2PA", "FT", "FTA", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS")
#           .withColumnRenamed("sum(FG)", "tFG")
#           .withColumnRenamed("sum(FGA)", "tFGA")
#           .withColumnRenamed("sum(3P)", "t3P")
#           .withColumnRenamed("sum(3PA)", "t3PA")
#           .withColumnRenamed("sum(2P)", "t2P")
#           .withColumnRenamed("sum(2PA)", "t2PA")
#           .withColumnRenamed("sum(FT)", "tFT")
#           .withColumnRenamed("sum(FTA)", "tFTA")
#           .withColumnRenamed("sum(ORB)", "tORB")
#           .withColumnRenamed("sum(DRB)", "tDRB")
#           .withColumnRenamed("sum(TRB)", "tTRB")
#           .withColumnRenamed("sum(AST)", "tAST")
#           .withColumnRenamed("sum(STL)", "tSTL")
#           .withColumnRenamed("sum(BLK)", "tBLK")
#           .withColumnRenamed("sum(TOV)", "tTOV")
#           .withColumnRenamed("sum(PF)", "tPF")
#           .withColumnRenamed("sum(PTS)", "tPTS")

#           .drop("Age", "G", "GS", "MP", "FG%", "3P%", "2P%", "eFG%", "FT%")
          
#            .agg(sum(f.col("FG")).alias("tFG"), sum(f.col("FGA")).alias("tFGA"), sum(f.col("3P")).alias("t3P"), sum(f.col("3PA")).alias("t3PA"), sum(f.col("2P")).alias("t2P"), sum(f.col("2PA")).alias("t2PA"), sum(f.col("FT")).alias("tFT"), sum(f.col("FTA")).alias("tFTA"), sum(f.col("ORB")).alias("tORB"), sum(f.col("DRB")).alias("tDRB"), sum(f.col("TRB")).alias("tTRB"), sum(f.col("AST")).alias("tAST"), sum(f.col("STL")).alias("tSTL"), sum(f.col("BLK")).alias("tBLK"), sum(f.col("TOV")).alias("tTOV"), sum(f.col("PF")).alias("tPF"), sum(f.col("PTS")).alias("tPTS"))
          
#           .agg(sum("FG")).alias("tFG"), sum("FGA").alias("tFGA"), sum("3P").alias("t3P"), sum("t3PA").alias("t3PA"), sum("2P").alias("t2P"), sum("2PA").alias("t2PA"), sum("FT").alias("tFT"), sum("FTA").alias("tFTA"), sum("ORB").alias("tORB"), sum("DRB").alias("tDRB"), sum("TRB").alias("tTRB"), sum("AST").alias("tAST"), sum("STL").alias("tSTL"), sum("BLK").alias("tBLK"), sum("TOV").alias("tTOV"), sum("PF").alias("tPF"), sum("PTS").alias("tPTS"))
         )
display(teamdf)

# COMMAND ----------

newnames = ["Team", "tFG", "tFGA", "t3P", "t3PA", "t2P", "t2PA", "tFT", "tFTA", "tORB", "tDRB", "tTRB", "tAST", "tSTL", "tBLK", "tTOV", "tPF", "tPTS"]

teamdf = teamdf.toDF(*newnames)

display(teamdf.sort(desc("tPTS")))

# COMMAND ----------

teamdf = (teamdf
         .withColumn("tPPG", f.col("tPTS")/f.col("tGP"))
         )

# COMMAND ----------

teamandplayerdf = combineddf.join(teamdf, "Team", "outer")
display(teamandplayerdf)

# COMMAND ----------



# COMMAND ----------

vop = (f.col("PTS"))/(f.col("FGA") - f.col("ORB") + f.col("TOV") + 0.44*f.col("FTA"))

drb_pctg = f.col("DRB") / f.col("TRB")

factor = 2/3 - ((f.col("AST") * f.col("FT")) / (4*(f.col("FG"))**2))

uper1 = f.col("FG") + (2/3 * f.col("AST") + (2 - (factor*f.col("tAST"))/f.col("tFG"))*f.col("FG") + ((f.col("FT")*(2 - (f.col("tAST")/(3*f.col("tFG"))))))/2 - vop*(f.col("TOV") + drb_pctg*(f.col("FGA") - f.col("FG"))))

uper2 = vop * (0.1936+0.2464*drb_pctg) * (f.col("FTA")-f.col("FT")) - (1-drb_pctg)*(f.col("TRB") - f.col("ORB"))

uper3 = vop * (drb_pctg * f.col("ORB") - f.col("STL") - drb_pctg * f.col("BLK")) + f.col("PF") * ((f.col("FT") * (1-0.44*vop))/f.col("PF"))

uper = ((uper1 - uper2 - uper3)/f.col("MP"))/f.col("G")
# uper = ((f.col("FG") + (2/3 * f.col("AST") + (2 - (factor*f.col("tAST"))/f.col("tFG"))*f.col("FG") + ((f.col("FT")*(2 - (f.col("tAST")/(3*f.col("tFG"))))))/2 - vop*(f.col("TOV") + drb_pctg*(f.col("FGA") - f.col("FG")))) - (vop(0.1936+0.2464*drb_pctg) * (f.col("FT")-f.col("FTA")) - (1-drb_pctg)*(f.col("TRB") - f.col("ORB"))) - (vop(drb_pctg*f.col("ORB") - f.col("STL") - drb_pctg*f.col("BLK")) + f.col("PF")*((f.col("FTM")*(1-0.44*vop))/f.col*"PF")))/f.col("MP"))/f.col("G")

playereff = (teamandplayerdf
            .withColumn("DRB%", drb_pctg)
            .withColumn("uPER", uper)
            .sort(desc("uPER"))
            )

display(playereff)
