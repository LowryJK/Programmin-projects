// In this project I took part in the Basic Tasks 4, 6 and 7 and the Additional Task 2.


// import statements for the entire notebook
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, DoubleType};


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{concat, lit}



// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 1 - Sales data
// MAGIC
// MAGIC The CSV file `assignment/sales/video_game_sales.csv` in the XXXXX contains video game sales data (from [https://www.kaggle.com/datasets/ashaheedq/video-games-sales-2019/data](https://www.kaggle.com/datasets/ashaheedq/video-games-sales-2019/data)). The direct address for the dataset is: `XXX`
// MAGIC
// MAGIC Load the data from the CSV file into a data frame. The column headers and the first few data lines should give sufficient information about the source dataset.
// MAGIC
// MAGIC Only data for sales in the first ten years of the 21st century should be considered in this task, i.e. years 2000-2009.
// MAGIC
// MAGIC Using the data, find answers to the following:
// MAGIC
// MAGIC - Which publisher had the highest total sales in video games in European Union in years 2000-2009?
// MAGIC - What were the total yearly sales, in European Union and globally, for this publisher in year 2000-2009
// MAGIC

// COMMAND ----------

val salesData: DataFrame = spark.read
    .format("csv")
    .option("delimiter", ",")  // optional, since the default delimiter is comma
    .option("header", "true")
    .option("inferSchema", "true")
    .load("XXXXX")

// Filtering years and getting sales data by publisher
val aggregatedSalesData: DataFrame = salesData.filter(salesData("Year") >= 2000 && salesData("Year") <= 2009)
  .groupBy(col("Publisher"))
  .agg(
    sum("EU_Sales").alias("EU_sales"),
    sum("Global_Sales").alias("Global_Sales")
  )
  .orderBy(col("EU_sales").desc)

// Since the DF is sorted descending, we can get the first row
val bestEUPublisher: String = aggregatedSalesData.first().getString(0)

val bestEUPublisherSales: DataFrame = salesData.filter(salesData("Year") >= 2000 && salesData("Year") <= 2009 && salesData("Publisher") === bestEUPublisher)
  .groupBy(col("Year"))
  .agg(
    round(sum("EU_Sales"),2).alias("EU_sales"),
    round(sum("Global_Sales"),2).alias("Global_sales")
  )
  .orderBy(col("Year"))

println(s"The publisher with the highest total video game sales in European Union is: '${bestEUPublisher}'")
println("Sales data for the publisher:")
bestEUPublisherSales.show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 2 - Shot data from NHL matches
// MAGIC
// MAGIC A parquet file in the XXXXX at folder `assignment/nhl_shots.parquet` from [https://moneypuck.com/data.htm](https://moneypuck.com/data.htm) contains information about every shot in all National Hockey League ([NHL](https://en.wikipedia.org/wiki/National_Hockey_League), [ice hockey](https://en.wikipedia.org/wiki/Ice_hockey)) matches starting from season 2011-12 and ending with the last completed season, 2022-23.
// MAGIC
// MAGIC In this task you should load the data with all of the rows into a data frame. This data frame object will then be used in the following basic tasks 3-7.
// MAGIC
// MAGIC ### Background information
// MAGIC
// MAGIC Each NHL season is divided into regular season and playoff season. In the regular season the teams play up to 82 games with the best teams continuing to the playoff season. During the playoff season the remaining teams are paired and each pair play best-of-seven series of games to determine which team will advance to the next phase.
// MAGIC
// MAGIC In ice hockey each game has a home team and an away team. The regular length of a game is three 20 minute periods, i.e. 60 minutes or 3600 seconds. The team that scores more goals in the regulation time is the winner of the game.
// MAGIC
// MAGIC If the scoreline is even after this regulation time:
// MAGIC
// MAGIC - In playoff games, the game will be continued until one of the teams score a goal with the scoring team being the winner.
// MAGIC - In regular season games, there is an extra time that can last a maximum of 5 minutes (300 seconds). If one of the teams score, the game ends with the scoring team being the winner. If there is no goals in the extra time, there would be a shootout competition to determine the winner. These shootout competitions are not considered in this assignment, and the shots from those are not included in the raw data.
// MAGIC
// MAGIC **Columns in the data**
// MAGIC
// MAGIC Each row in the given data represents one shot in a game.
// MAGIC
// MAGIC The column description from the source website. Not all of these will be needed in this assignment.
// MAGIC
// MAGIC | column name | column type | description |
// MAGIC | ----------- | ----------- | ----------- |
// MAGIC | shotID      | integer | Unique id for each shot |
// MAGIC | homeTeamCode | string | The home team in the game. For example: TOR, MTL, NYR, etc. |
// MAGIC | awayTeamCode | string | The away team in the game |
// MAGIC | season | integer | Season the shot took place in. Example: 2009 for the 2009-2010 season |
// MAGIC | isPlayOffGame | integer | Set to 1 if a playoff game, otherwise 0 |
// MAGIC | game_id | integer | The NHL Game_id of the game the shot took place in |
// MAGIC | time | integer | Seconds into the game of the shot |
// MAGIC | period | integer | Period of the game |
// MAGIC | team | string | The team taking the shot. HOME or AWAY |
// MAGIC | location | string | The zone the shot took place in. HOMEZONE, AWAYZONE, or Neu. Zone |
// MAGIC | event | string | Whether the shot was a shot on goal (SHOT), goal, (GOAL), or missed the net (MISS) |
// MAGIC | homeTeamGoals | integer | Home team goals before the shot took place |
// MAGIC | awayTeamGoals | integer | Away team goals before the shot took place |
// MAGIC | homeTeamWon | integer | Set to 1 if the home team won the game. Otherwise 0. |
// MAGIC | shotType | string | Type of the shot. (Slap, Wrist, etc) |
// MAGIC

// COMMAND ----------

val shotsDF: DataFrame = spark.read.parquet("XXXXX")
shotsDF.cache
val numRows: Long = shotsDF.count()
val numCols: Integer = shotsDF.columns.size

println(s"Number of rows: ${numRows}")
println(s"Number of columns: ${numCols}")
shotsDF.printSchema
shotsDF.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 3 - Game data frame
// MAGIC
// MAGIC Create a match data frame for all the game included in the shots data frame created in basic task 2.
// MAGIC
// MAGIC The output should contain one row for each game.
// MAGIC
// MAGIC The following columns should be included in the final data frame for this task:
// MAGIC
// MAGIC | column name    | column type | description |
// MAGIC | -------------- | ----------- | ----------- |
// MAGIC | season         | integer     | Season the game took place in. Example: 2009 for the 2009-2010 season |
// MAGIC | game_id        | integer     | The NHL Game_id of the game |
// MAGIC | homeTeamCode   | string      | The home team in the game. For example: TOR, MTL, NYR, etc. |
// MAGIC | awayTeamCode   | string      | The away team in the game |
// MAGIC | isPlayOffGame  | integer     | Set to 1 if a playoff game, otherwise 0 |
// MAGIC | homeTeamGoals  | integer     | Number of goals scored by the home team |
// MAGIC | awayTeamGoals  | integer     | Number of goals scored by the away team |
// MAGIC | lastGoalTime   | integer     | The time in seconds for the last goal in the game. 0 if there was no goals in the game. |
// MAGIC
// MAGIC All games had at least some shots but there are some games that did not have any goals either in the regulation 60 minutes or in the extra time.
// MAGIC
// MAGIC Note, that for a couple of games there might be some shots, including goal-scoring ones, that are missing from the original dataset. For example, there might be a game with a final scoreline of 3-4 but only 6 of the goal-scoring shots are included in the dataset. Your solution does not have to try to take these rare occasions of missing data into account. I.e., you can do all the tasks with the assumption that there are no missing or invalid data.
// MAGIC

// COMMAND ----------

// Get last shots of each game
val d: DataFrame = shotsDF
  .withColumn("seasonGameID", concat(col("season").cast(StringType), col("game_id").cast(StringType)).as("seasonGameID"))
  .groupBy("seasonGameID")
  .agg(
    max("shotID").alias("lastShotID")
  )

// Get last goaling shots and their times of each game
val d2: DataFrame = shotsDF
  .withColumn("seasonGame_ID", concat(col("season").cast(StringType), col("game_id").cast(StringType)).as("seasonGame_ID"))
  .filter("event = 'GOAL'")
  .groupBy("seasonGame_ID")
  .agg(
    max("shotID").alias("lastGoalShotID"), 
    max("time").alias("lastGoalTime")
  )

// Create main DF
val tempDF: DataFrame = shotsDF
  .withColumn("season_game_id", concat(col("season").cast(StringType), col("game_id").cast(StringType)).as("season_game_id"))
  

val gamesDF: DataFrame = tempDF
  .join(d, tempDF("shotID") === d("lastShotID"), "inner") // Get the situation before the last shot of the game
  .join(d2, tempDF("season_game_id") === d2("seasonGame_ID"), "left") // Get the times of last goaling shot
  .select(
    tempDF("season"),
    tempDF("game_id"),
    tempDF("homeTeamCode"),
    tempDF("awayTeamCode"),
    tempDF("isPlayOffGame"),
    when(col("event") === "GOAL" && col("team") === "HOME", col("homeTeamGoals") + 1) // Update score if the last shot was goal winning
      .otherwise(col("homeTeamGoals")).alias("homeTeamGoals"),
    when(col("event") === "GOAL" && col("team") === "AWAY", col("awayTeamGoals") + 1) // Update score if the last shot was goal winning
      .otherwise(col("awayTeamGoals")).alias("awayTeamGoals"),
    when(col("event") =!= "GOAL" && col("awayTeamGoals") === 0 && col("homeTeamGoals") === 0, lit(0)) // Get the time of the last goal winning shot if it exists
      .otherwise(col("lastGoalTime")).alias("lastGoalTime")
  )
  
val numRows: Long = gamesDF.count()
val numCols: Integer = gamesDF.columns.size
//println(s"Number of rows: ${numRows}") // The same amount as earlier
//println(s"Number of columns: ${numCols}")
//gamesDF.show(82)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 4 - Game wins during playoff seasons
// MAGIC
// MAGIC Create a data frame that uses the game data frame from the basic task 3 and contains aggregated number of wins and losses for each team and for each playoff season, i.e. for games which have been marked as playoff games. Only teams that have played in at least one playoff game in the considered season should be included in the final data frame.
// MAGIC
// MAGIC The following columns should be included in the final data frame:
// MAGIC
// MAGIC | column name    | column type | description |
// MAGIC | -------------- | ----------- | ----------- |
// MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
// MAGIC | teamCode       | string      | The code for the team. For example: TOR, MTL, NYR, etc. |
// MAGIC | games          | integer     | Number of playoff games the team played in the given season |
// MAGIC | wins           | integer     | Number of wins in playoff games the team had in the given season |
// MAGIC | losses         | integer     | Number of losses in playoff games the team had in the given season |
// MAGIC
// MAGIC Playoff games where a team scored more goals than their opponent are considered winning games. And playoff games where a team scored less goals than the opponent are considered losing games.
// MAGIC
// MAGIC In real life there should not be any playoff games where the final score line was even but due to some missing shot data you might end up with a couple of playoff games that seems to have ended up in a draw. These possible "drawn" playoff games can be left out from the win/loss calculations.
// MAGIC

// COMMAND ----------

 """
 val homeWinsDF = gamesDF
    .withColumn("season", (col("season")))
    .withColumn("homeTeamCode", (col("homeTeamCode")))
    .filter(col("isPlayOffGame") === 1) // Filter playoff games
    .groupBy("season", "homeTeamCode")
    .agg(count(when(col("homeTeamGoals") > col("awayTeamGoals"), 1)).alias("homeWins")) // Count the amount of home games won

  // DataFrame for away games won
  val awayWinsDF = gamesDF
    .withColumn("season", (col("season")))
    .withColumn("awayTeamCode", (col("awayTeamCode")))
    .filter(col("isPlayOffGame") === 1) // Filter playoff games
    .groupBy("season",  "awayTeamCode")
    .agg(count(when(col("homeTeamGoals") < col("awayTeamGoals"), 1)).alias("awayWins")) // Count the amount of away games won


  // Merged DataFrame of home and away wins.
  val winsDF = homeWinsDF
    .join(awayWinsDF,
      homeWinsDF("season") === awayWinsDF("season") && homeWinsDF("homeTeamCode") === awayWinsDF("awayTeamCode"),"outer")
    .select(
      coalesce(homeWinsDF("season"), awayWinsDF("season")).as("season"),
      coalesce(homeWinsDF("homeTeamCode"), awayWinsDF("awayTeamCode")).as("teamCodeW"),
      (coalesce(homeWinsDF("homeWins"), lit(0)) + coalesce(awayWinsDF("awayWins"), lit(0))).as("wins") // Counts the total amount of wins, nulls are set to 0
    )
    .orderBy("season", "teamCodeW")
"""
// Filter playoff games
val playoffGamesDF = gamesDF.filter(col("isPlayOffGame") === 1)

// Count home wins
val homeWinsDF = playoffGamesDF
  .groupBy("season", "homeTeamCode")
  .agg(count(when(col("homeTeamGoals") > col("awayTeamGoals"), true)).alias("homeWins")) // Count the amount of home games won

// Count away wins
val awayWinsDF = playoffGamesDF
  .groupBy("season", "awayTeamCode")
  .agg(count(when(col("homeTeamGoals") < col("awayTeamGoals"), true)).alias("awayWins")) // Count the amount of away games won

// Merged DataFrame of home and away wins
val winsDF = homeWinsDF
  .join(awayWinsDF.withColumnRenamed("awayTeamCode", "homeTeamCode"), Seq("season", "homeTeamCode"), "outer")
  .withColumn("wins", coalesce($"homeWins", lit(0)) + coalesce($"awayWins", lit(0)))// Counts the total amount of wins, nulls are set to 0
  .select(col("season"),col("wins"),col("homeTeamCode").as("teamCodeW"))
"""
  // DataFrame for home games lost
  val homeLossesDF = gamesDF
    .withColumn("season", (col("season")))
    .withColumn("homeTeamCode", (gamesDF("homeTeamCode")))
    .filter(col("isPlayOffGame") === 1) // Filter playoff games.
    .groupBy("season", "homeTeamCode")
    .agg(count(when(col("homeTeamGoals") < col("awayTeamGoals"), 1)).alias("homeLosses")) // Count the amount of home games lost

  // DataFrame for away games lost
  val awayLossesDF = gamesDF
    .withColumn("season", (col("season")))
    .withColumn("awayTeamCode", (col("awayTeamCode")))
    .filter(col("isPlayOffGame") === 1) // Filter playoff games
    .groupBy("season", "awayTeamCode")
    .agg(count(when(col("homeTeamGoals") > col("awayTeamGoals"), 1)).alias("awayLosses")) // Count the amount of away games lost

  // Merged DataFrame of home and away losses.
  val lossesDF = homeLossesDF
    .join(awayLossesDF, homeLossesDF("season") === awayLossesDF("season") && homeLossesDF("homeTeamCode") === awayLossesDF("awayTeamCode"), "full_outer")
    .select(
      coalesce(homeLossesDF("season"), awayLossesDF("season")).as("season"),
      coalesce(homeLossesDF("homeTeamCode"), awayLossesDF("awayTeamCode")).as("teamCodeL"),
      (coalesce(homeLossesDF("homeLosses"), lit(0)) + coalesce(awayLossesDF("awayLosses"), lit(0))).as("losses") // Counts the total amount of losses, nulls are set to 0
    )
    .orderBy("season", "teamCodeL")
"""

// DataFrame for home games lost
val homeLossesDF = playoffGamesDF
  .groupBy("season", "homeTeamCode")
  .agg(count(when(col("homeTeamGoals") < col("awayTeamGoals"), 1)).alias("homeLosses"))

// DataFrame for away games lost
val awayLossesDF = playoffGamesDF
  .groupBy("season", "awayTeamCode")
  .agg(count(when(col("homeTeamGoals") > col("awayTeamGoals"), 1)).alias("awayLosses"))

// Merged DataFrame of home and away losses.
val lossesDF = homeLossesDF
  .join(awayLossesDF.withColumnRenamed("awayTeamCode", "homeTeamCode"), Seq("season", "homeTeamCode"), "full_outer")
  .withColumn("losses", coalesce($"homeLosses", lit(0)) + coalesce($"awayLosses", lit(0)))
  .select(col("season"), col("homeTeamCode").as("teamCodeL"), col("losses"))
 
  // DataFrame for both wins and losses and total games in the playoffs for each team that made the playoffs and for seasons 2011-2022
  val playoffDF = winsDF
    .join(lossesDF, Seq("season"), "full_outer")
    //.withColumn("season", (col("season")))
    .withColumn("teamCode", (winsDF("teamCodeW")))
    .withColumn("wins", winsDF("wins"))
    .withColumn("losses", lossesDF("losses"))
    .withColumn("games", col("wins")+col("losses"))
    .filter(winsDF("teamCodeW")===lossesDF("teamCodeL"))
    .select("season", "teamCode", "games", "wins", "losses")
    .orderBy("season")
    

 

  // Counting the row and column amount and showing the dataframe
  val numRows3: Long = playoffDF.count()
  val numCols3: Integer = playoffDF.columns.size
  //println(s"Number of rows: ${numRows3}") // The same amount as earlier
  //println(s"Number of columns: ${numCols3}")
  



// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 5 - Best playoff teams
// MAGIC
// MAGIC Using the playoff data frame created in basic task 4 create a data frame containing the win-loss record for best playoff team, i.e. the team with the most wins, for each season. You can assume that there are no ties for the highest amount of wins in each season.
// MAGIC
// MAGIC The following columns should be included in the final data frame:
// MAGIC
// MAGIC | column name    | column type | description |
// MAGIC | -------------- | ----------- | ----------- |
// MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
// MAGIC | teamCode       | string      | The team code for the best performing playoff team in the given season. For example: TOR, MTL, NYR, etc. |
// MAGIC | games          | integer     | Number of playoff games the best performing playoff team played in the given season |
// MAGIC | wins           | integer     | Number of wins in playoff games the best performing playoff team had in the given season |
// MAGIC | losses         | integer     | Number of losses in playoff games the best performing playoff team had in the given season |
// MAGIC
// MAGIC Finally, fetch the details for the best playoff team in season 2022.
// MAGIC

// COMMAND ----------


val maxWins = playoffDF.groupBy($"season".as("group_season"))
  .agg(max("wins").alias("maxWins"))

val bestPlayoffTeams = playoffDF.alias("p1")
  .join(maxWins.alias("p2"), $"p1.wins" === $"p2.maxWins")
  .groupBy($"p1.season", $"p1.teamCode", $"p1.games", $"p1.wins", $"p1.losses")
  .agg(max("p1.wins"))
  .select($"p1.season", $"p1.teamCode", $"p1.games", $"p1.wins", $"p1.losses")
  .orderBy("season")

bestPlayoffTeams.show()


// COMMAND ----------

val bestPlayoffTeam2022: Row = bestPlayoffTeams.filter($"season" === 2022).head()

println("Best playoff team in 2022:")
println(s"    Team: ${bestPlayoffTeam2022.getAs[String]("teamCode")}")
println(s"    Games: ${bestPlayoffTeam2022.getAs[Long]("games")}")
println(s"    Wins: ${bestPlayoffTeam2022.getAs[Long]("wins")}")
println(s"    Losses: ${bestPlayoffTeam2022.getAs[Long]("losses")}")
println("=========================================================")


// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 6 - Regular season points
// MAGIC
// MAGIC Create a data frame that uses the game data frame from the basic task 3 and contains aggregated data for each team and for each season for the regular season matches, i.e. the non-playoff matches.
// MAGIC
// MAGIC The following columns should be included in the final data frame:
// MAGIC
// MAGIC | column name    | column type | description |
// MAGIC | -------------- | ----------- | ----------- |
// MAGIC | season         | integer     | The season for the data. Example: 2009 for the 2009-2010 season |
// MAGIC | teamCode       | string      | The code for the team. For example: TOR, MTL, NYR, etc. |
// MAGIC | games          | integer     | Number of non-playoff games the team played in the given season |
// MAGIC | wins           | integer     | Number of wins in non-playoff games the team had in the given season |
// MAGIC | losses         | integer     | Number of losses in non-playoff games the team had in the given season |
// MAGIC | goalsScored    | integer     | Total number goals scored by the team in non-playoff games in the given season |
// MAGIC | goalsConceded  | integer     | Total number goals scored against the team in non-playoff games in the given season |
// MAGIC | points         | integer     | Total number of points gathered by the team in non-playoff games in the given season |
// MAGIC
// MAGIC Points from each match are received as follows (in the context of this assignment, these do not exactly match the NHL rules):
// MAGIC
// MAGIC | points | situation |
// MAGIC | ------ | --------- |
// MAGIC | 3      | team scored more goals than the opponent during the regular 60 minutes |
// MAGIC | 2      | the score line was even after 60 minutes but the team scored a winning goal during the extra time |
// MAGIC | 1      | the score line was even after 60 minutes but the opponent scored a winning goal during the extra time or there were no goals in the extra time |
// MAGIC | 0      | the opponent scored more goals than the team during the regular 60 minutes |
// MAGIC
// MAGIC In the regular season the following table shows how wins and losses should be considered (in the context of this assignment):
// MAGIC
// MAGIC | win | loss | situation |
// MAGIC | --- | ---- | --------- |
// MAGIC | Yes | No   | team gained at least 2 points from the match |
// MAGIC | No  | Yes  | team gain at most 1 point from the match |
// MAGIC

// COMMAND ----------

"""
val homePointsRegularTimeDF = gamesDF

    //.withColumn("season", concat(col("season")))
    //.withColumn("homeTeamCode", concat(col("homeTeamCode")))
    .filter(col("isPlayOffGame")===0)
    .withColumn("pointsH", when(col("lastGoalTime") < 3600 && col("homeTeamGoals") > col("awayTeamGoals"), 3).otherwise(0)) //check if home team wins in 60 mins and add 3 points
    .withColumn("pointsH_OT", when(col("lastGoalTime") >= 3600 && col("homeTeamGoals") > col("awayTeamGoals"), 2).otherwise(0)) //check if home team wins in overtime and add 2 points
    .withColumn("pointsH_OT_L", when(col("lastGoalTime") >= 3600  && col("homeTeamGoals") < col("awayTeamGoals"), 1).otherwise(0)) //check if home team loses in overtime and 1 points
    .withColumn("pointsH_SO", when(col("lastGoalTime") < 3600  && col("homeTeamGoals") === col("awayTeamGoals"), 1).otherwise(0)) //if there was no goals in overtime add 1 
    .withColumn("winsH", when((col("pointsH")===3 || col("pointsH_OT")===2),1).otherwise(0))
    .withColumn("lossesH", when((col("pointsH_OT_L")===1 || col("pointsH_SO")===1 || col("winsH")===0),1).otherwise(0))
    .withColumn("homeGoalsScored", col("homeTeamGoals").cast("int"))
    .withColumn("homeGoalsConceded", col("awayTeamGoals").cast("int"))
    .groupBy("season", "homeTeamCode")
    .agg((sum("pointsH") + sum("pointsH_OT") + sum("pointsH_OT_L")+sum("pointsH_SO")).alias("totalPointsH"),
      sum("winsH").alias("totalWinsH"),
      sum("lossesH").alias("totalLossesH"),
      sum("homeGoalsScored").alias("totalGoalsScoredHome"),
      sum("homeGoalsConceded").alias("totalGoalsConcededHome"))
    .select("season", "totalPointsH", "homeTeamCode","totalWinsH","totalLossesH","totalGoalsScoredHome","totalGoalsConcededHome")
"""
//improved version
val homePointsRegularTimeDF = gamesDF
  .filter(col("isPlayOffGame") === 0)
  .withColumn("pointsH", when(col("lastGoalTime") < 3600 && col("homeTeamGoals") > col("awayTeamGoals"), 3) //check if home team wins in 60 mins and add 3 points
                              .when(col("lastGoalTime") >= 3600 && col("homeTeamGoals") > col("awayTeamGoals"), 2)//check if home team wins in overtime and add 2 points
                              .when(col("lastGoalTime") >= 3600 && col("homeTeamGoals") < col("awayTeamGoals"), 1)//check if home team loses in overtime and 1 points
                              .when(col("lastGoalTime") < 3600 && col("homeTeamGoals") === col("awayTeamGoals"), 1)//if there was no goals in overtime add 1 
                              .otherwise(0))
  .groupBy("season", "homeTeamCode")
  .agg(
    sum("pointsH").alias("totalPointsH"),
    sum(when(col("pointsH") === 3 || col("pointsH") === 2, 1)).alias("totalWinsH"),
    sum(when(col("pointsH") === 1 || col("pointsH") === 0, 1)).alias("totalLossesH"),
    sum(col("homeTeamGoals")).alias("totalGoalsScoredHome"),
    sum(col("awayTeamGoals")).alias("totalGoalsConcededHome")
  )
  .select("season", "totalPointsH", "homeTeamCode", "totalWinsH", "totalLossesH", "totalGoalsScoredHome", "totalGoalsConcededHome")
"""
  val awayPointsRegularTimeDF = gamesDF
    .withColumn("season", col("season")) // cast operation is useless here?
    .withColumn("awayTeamCode", col("awayTeamCode")) //cast operation is useless here?
    .filter(col("isPlayOffGame") === 0)
    .withColumn("pointsA", when(col("lastGoalTime") < 3600 && col("homeTeamGoals") < col("awayTeamGoals"), 3).otherwise(0))
    .withColumn("pointsA_OT", when(col("lastGoalTime") >= 3600 && col("homeTeamGoals") < col("awayTeamGoals"), 2).otherwise(0))
    .withColumn("pointsA_OT_L", when(col("lastGoalTime") >= 3600 && col("homeTeamGoals") > col("awayTeamGoals"), 1).otherwise(0))
    .withColumn("pointsA_SO", when(col("lastGoalTime") < 3600  && col("homeTeamGoals") === col("awayTeamGoals"), 1).otherwise(0))
    .withColumn("winsA", when((col("pointsA")===3 || col("pointsA_OT")===2),1).otherwise(0))
    .withColumn("lossesA", when((col("pointsA_OT_L")===1 || col("pointsA_SO")===1 || col("winsA") === 0),1).otherwise(0))
    .withColumn("awayGoalsScored", concat(col("awayTeamGoals")).cast("int"))
    .withColumn("awayGoalsConceded", concat(col("homeTeamGoals")).cast("int"))
    .groupBy("season", "awayTeamCode")
    .agg((sum("pointsA") + sum("pointsA_OT") + sum("pointsA_OT_L")+sum("pointsA_SO")).alias("totalPointsA")
      ,
      sum("winsA").alias("totalWinsA"),
      sum("lossesA").alias("totalLossesA"),
      sum("awayGoalsScored").alias("totalAwayGoalsScored"),
      sum("awayGoalsConceded").alias("totalAwayGoalsConceded")
    )
    .select("season", "totalPointsA", "awayTeamCode","totalWinsA","totalLossesA","totalAwayGoalsConceded","totalAwayGoalsScored")
"""
//optimized solution 
val awayPointsRegularTimeDF = gamesDF
  .filter(col("isPlayOffGame") === 0)
  .withColumn("pointsA",
    when(col("lastGoalTime") < 3600 && col("homeTeamGoals") < col("awayTeamGoals"), 3) //checks if away team wins, add 3 points
      .when(col("lastGoalTime") >= 3600 && col("homeTeamGoals") < col("awayTeamGoals"), 2) //checks if away team wins in OT, add 2 points
      .when(col("lastGoalTime") >= 3600 && col("homeTeamGoals") > col("awayTeamGoals"), 1) //checks if away team loses OT add 1 point
      .when(col("lastGoalTime") < 3600 && col("homeTeamGoals") === col("awayTeamGoals"), 1) //checks if no goals OT add 1 point
      .otherwise(0)
  )
  .withColumn("winsA", when((col("pointsA") === 3 || col("pointsA") === 2), 1).otherwise(0)) //calculate wins
  .withColumn("lossesA", when((col("pointsA") === 1 || col("pointsA") === 0), 1).otherwise(0)) // calculate losses
  .groupBy("season", "awayTeamCode")
  .agg(
    sum("pointsA").alias("totalPointsA"),
    sum("winsA").alias("totalWinsA"),
    sum("lossesA").alias("totalLossesA"),
    sum("awayTeamGoals").alias("totalAwayGoalsScored"), 
    sum("homeTeamGoals").alias("totalAwayGoalsConceded") 
  )
  .select("season", "totalPointsA", "awayTeamCode", "totalWinsA", "totalLossesA", "totalAwayGoalsConceded", "totalAwayGoalsScored")

"""
  val pointsRegularTimeDF = homePointsRegularTimeDF
    .join(awayPointsRegularTimeDF, homePointsRegularTimeDF("season") === awayPointsRegularTimeDF("season") && homePointsRegularTimeDF("homeTeamCode") === awayPointsRegularTimeDF("awayTeamCode"), "full_outer")
    .select(
      coalesce(homePointsRegularTimeDF("season"), awayPointsRegularTimeDF("season")).as("season"),
      coalesce(homePointsRegularTimeDF("homeTeamCode"), awayPointsRegularTimeDF("awayTeamCode")).as("teamCodeP"),
      (homePointsRegularTimeDF("totalPointsH") + awayPointsRegularTimeDF("totalPointsA")).alias("pointsRegular"),
      (homePointsRegularTimeDF("totalGoalsScoredHome") + awayPointsRegularTimeDF("totalAwayGoalsScored")).alias("totalGoalsScored"),
      (homePointsRegularTimeDF("totalGoalsConcededHome") + awayPointsRegularTimeDF("totalAwayGoalsConceded")).alias("totalGoalsConceded"),
      (homePointsRegularTimeDF("totalWinsH") + awayPointsRegularTimeDF("totalWinsA")).alias("wins"),
      (homePointsRegularTimeDF("totalLossesH") + awayPointsRegularTimeDF("totalLossesA")).alias("losses"),
      (col("wins")+col("losses")).alias("games")
    )
    """
  //optimized version
 val pointsRegularTimeDF = homePointsRegularTimeDF.as("homePoints")
  .join(awayPointsRegularTimeDF.as("awayPoints"),
    homePointsRegularTimeDF("season") === awayPointsRegularTimeDF("season") &&
      homePointsRegularTimeDF("homeTeamCode") === awayPointsRegularTimeDF("awayTeamCode"),
    "full_outer"
  )
  .select(
    coalesce($"homePoints.season", $"awayPoints.season").as("season"),
    coalesce($"homePoints.homeTeamCode", $"awayPoints.awayTeamCode").as("teamCodeP"),
    ($"homePoints.totalPointsH" + $"awayPoints.totalPointsA").alias("pointsRegular"),
    ($"homePoints.totalGoalsScoredHome" + $"awayPoints.totalAwayGoalsScored").alias("totalGoalsScored"),
    ($"homePoints.totalGoalsConcededHome" + $"awayPoints.totalAwayGoalsConceded").alias("totalGoalsConceded"),
    ($"homePoints.totalWinsH" + $"awayPoints.totalWinsA").alias("wins"), // calculates total wins per season
    ($"homePoints.totalLossesH" + $"awayPoints.totalLossesA").alias("losses"), //calculates total losses per season
    (col("wins")+col("losses")).alias("games") //calculate games 
  )


  val regularSeasonDF = pointsRegularTimeDF //optimized by removing unnecessary .withcolumn operations
    .withColumnRenamed("teamCodeP", "teamCode")
    .withColumn("points", $"pointsRegular".cast("int"))
    .select(
    $"season", $"teamCode", $"games", $"wins", $"losses",
    $"totalGoalsScored".as("goalsScored"),
    $"totalGoalsConceded".as("goalsConceded"),
    $"points"
  )


// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 7 - The worst regular season teams
// MAGIC
// MAGIC Using the regular season data frame created in the basic task 6, create a data frame containing the regular season records for the worst regular season team, i.e. the team with the least amount of points, for each season. You can assume that there are no ties for the lowest amount of points in each season.
// MAGIC
// MAGIC Finally, fetch the details for the worst regular season team in season 2022.
// MAGIC

// COMMAND ----------

val minPointsDF = regularSeasonDF
    .groupBy($"season".alias("minSeason"))
    .agg(min("points").alias("minPoints"))

  val worstRegularTeams = regularSeasonDF 
    .join(minPointsDF, regularSeasonDF("season") === minPointsDF("minSeason") && regularSeasonDF("points")===minPointsDF("minPoints"))
    .select(regularSeasonDF("season"), regularSeasonDF("teamCode"), regularSeasonDF("games"), regularSeasonDF("wins"), regularSeasonDF("losses"), regularSeasonDF("goalsScored"), regularSeasonDF("goalsConceded"), regularSeasonDF("points"))
    .orderBy(col("season"))
  worstRegularTeams.show()




// COMMAND ----------


  val worstRegularTeam2022: Row = worstRegularTeams.filter($"season" === 2022).head()

println("Worst regular season team in 2022:")
println(s"    Team: ${worstRegularTeam2022.getAs[String]("teamCode")}")
println(s"    Games: ${worstRegularTeam2022.getAs[Long]("games")}")
println(s"    Wins: ${worstRegularTeam2022.getAs[Long]("wins")}")
println(s"    Losses: ${worstRegularTeam2022.getAs[Long]("losses")}")
println(s"    Goals scored: ${worstRegularTeam2022.getAs[Long]("goalsScored")}")
println(s"    Goals conceded: ${worstRegularTeam2022.getAs[Long]("goalsConceded")}")
println(s"    Points: ${worstRegularTeam2022.getAs[Long]("points")}")


// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional tasks
// MAGIC
// MAGIC The implementation of the basic tasks is compulsory for every group.
// MAGIC
// MAGIC Doing the following additional tasks you can gain course points which can help in getting a better grade from the course (or passing the course).
// MAGIC Partial solutions can give partial points.
// MAGIC
// MAGIC The additional task 1 will be considered in the grading for every group based on their solutions for the basic tasks.
// MAGIC
// MAGIC The additional tasks 2 and 3 are separate tasks that do not relate to any other task in the assignment. The solutions used in these other additional tasks do not affect the grading of additional task 1. Instead, a good use of optimized methods can positively affect the grading of each specific task, while very non-optimized solutions can have a negative effect on the task grade.
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Task 1 - Optimized solutions to the basic tasks (2 points)
// MAGIC
// MAGIC Use the tools Spark offers effectively and avoid unnecessary operations in the code for the basic tasks.
// MAGIC
// MAGIC A couple of things to consider (**NOT** even close to a complete list):
// MAGIC
// MAGIC - Consider using explicit schemas when dealing with CSV data sources.
// MAGIC - Consider only including those columns from a data source that are actually needed.
// MAGIC - Filter unnecessary rows whenever possible to get smaller datasets.
// MAGIC - Avoid collect or similar expensive operations for large datasets.
// MAGIC - Consider using explicit caching if some data frame is used repeatedly.
// MAGIC - Avoid unnecessary shuffling (for example sorting) operations.
// MAGIC
// MAGIC It is okay to have your own test code that would fall into category of "ineffective usage" or "unnecessary operations" while doing the assignment tasks. However, for the final Moodle submission you should comment out or delete such code (and test that you have not broken anything when doing the final modifications).
// MAGIC
// MAGIC Note, that you should not do the basic tasks again for this additional task, but instead modify your basic task code with more efficient versions.
// MAGIC
// MAGIC You can create a text cell below this one and describe what optimizations you have done. This might help the grader to better recognize how skilled your work with the basic tasks has been.
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC - avoiding sorting
// MAGIC - delete unnecessary operations for example .withcolumn operations
// MAGIC - including only needed columns
// MAGIC - simplifying column operations, reducing unnecessary aggregations

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Task 2 - Unstructured data (2 points)
// MAGIC
// MAGIC You are given some text files with contents from a few thousand random articles both in English and Finnish from Wikipedia. Content from English articles are in the XXXXX at folder `assignment/wikipedia/en` and content from Finnish articles are at folder `assignment/wikipedia/fi`.
// MAGIC
// MAGIC Some cleaning operations have already been done to the texts but the some further cleaning is still required.
// MAGIC
// MAGIC The final goal of the task is to get the answers to following questions:
// MAGIC
// MAGIC - What are the ten most common English words that appear in the English articles?
// MAGIC - What are the five most common 5-letter Finnish words that appear in the Finnish articles?
// MAGIC - What is the longest word that appears at least 150 times in the articles?
// MAGIC - What is the average English word length for the words appearing in the English articles?
// MAGIC - What is the average Finnish word length for the words appearing in the Finnish articles?
// MAGIC
// MAGIC For a word to be included in the calculations, it should fulfill the following requirements:
// MAGIC
// MAGIC - Capitalization is to be ignored. I.e., words "English", "ENGLISH", and "english" are all to be considered as the same word "english".
// MAGIC - An English word should only contain the 26 letters from the alphabet of Modern English. Only exception is that punctuation marks, i.e. hyphens `-`, are allowed in the middle of the words as long as there are no two consecutive punctuation marks without any letters between them.
// MAGIC - The only allowed 1-letter English words are `a` and `i`.
// MAGIC - A Finnish word should follow the same rules as English words, except that three additional letters, `å`, `ä`, and `ö`, are also allowed, and that no 1-letter words are allowed. Also, any word that contains "`wiki`" should not be considered as Finnish word.
// MAGIC
// MAGIC Some hints:
// MAGIC
// MAGIC - Using an RDD or a Dataset (in Scala) might make the data cleaning and word determination easier than using DataFrames.
// MAGIC - It can be assumed that in the source data each word in the same line is separated by at least one white space (` `).
// MAGIC - You are allowed to remove all non-allowed characters from the source data at the beginning of the cleaning process.
// MAGIC - It is advisable to first create a DataFrame/Dataset/RDD that contains the found words, their language, and the number of times those words appeared in the articles. This can then be used as the starting point when determining the answers to the given questions.
// MAGIC

// COMMAND ----------

 
      val englishLetters: String = "abcdefghijklmnopqrstuvwxyz"
      val finnishLetters: String = englishLetters + "åäö"
      val whiteSpace: String = " "
      val punctuationMark: Char = '-'
      val twoPunctuationMarks: String = "--"
      val allowedEnglishOneLetterWords: List[String] = List("a", "i")
      val wikiStr: String = "wiki"
      val allowedEngChars: Set[Char] = (englishLetters + punctuationMark).toSet
      val englishStr: String = "English"
      val finnishStr: String = "Finnish"

      // Creates English RDD and splits lines by white spaces and changes words to lowercase and removes unallowed characters
      val engTxtFilesRDD = spark.sparkContext.textFile("XXXXX")
      val engWordsRDD = engTxtFilesRDD
        .flatMap(line=>line.split(" "))
        .map(word=>word.toLowerCase.filter(allowedEngChars.contains))

      // The necessary filters
      val engFilteredRDD = engWordsRDD
        .filter(word => (word.length > 1 || (word=="a" || word=="i"))
          && !word.startsWith(punctuationMark.toString) && !word.endsWith(punctuationMark.toString))

      //Counts for each word
      val engWordCountRDD = engFilteredRDD
        .map(word => (word, 1))
        .reduceByKey(_ + _)

      // Sort the words by their count
      val sortedEngWordCountsRDD = engWordCountRDD
        .map(pair => pair.swap)
        .sortByKey(ascending = false)
        .map(pair => pair.swap)

      // Top 10 English words by count
      val top10EngWords = sortedEngWordCountsRDD
        .take(10)

      // DF for top10 English words
      val commonWordsEn: DataFrame = spark
        .createDataFrame(top10EngWords)
        .select("_1", "_2")
        .withColumnRenamed("_1", "word")
        .withColumnRenamed("_2", "count")



      println("The ten most common English words that appear in the English articles:")
      commonWordsEn.show()

      // Creates Finnish RDD and splits lines by white spaces and changes words to lowercase and removes unallowed characters
      val finTxtFilesRDD = spark.sparkContext.textFile("XXXXX")
      val allowedFinChars: Set[Char] = (finnishLetters + punctuationMark).toSet
      val finWordsRDD = finTxtFilesRDD
        .flatMap(line => line.split(" "))
        .map(word => word.toLowerCase.filter(allowedFinChars.contains))

      // The necessary filters
      val finFilteredRDD = finWordsRDD
        .filter(word => word.length > 1
          && !word.contains("wiki")
          && !word.startsWith(punctuationMark.toString) && !word.endsWith(punctuationMark.toString))

      // Words that are the length of 5
      val finWordCount5RDD = finFilteredRDD
        .filter(word => word.length == 5)
        .map(word => (word, 1))
        .reduceByKey(_ + _)

      // Sort the 5-length words by their count
      val sortedFinWordCounts5RDD = finWordCount5RDD
        .map(pair => pair.swap)
        .sortByKey(ascending = false)
        .map(pair => pair.swap)

      // Get the top 5 words with 5-length
      val top5FinWords = sortedFinWordCounts5RDD
        .take(5)

      // Creates DF for top5 Finnish 5-length words
      val common5LetterWordsFi: DataFrame = spark
        .createDataFrame(top5FinWords)
        .select("_1", "_2")
        .withColumnRenamed("_1", "word")
        .withColumnRenamed("_2", "count")

      println("The five most common 5-letter Finnish words that appear in the Finnish articles:")
      common5LetterWordsFi.show()


      val finWordCountRDD = finFilteredRDD
        .map(word => (word, 1))
        .reduceByKey(_ + _)

      val sortedFinWordCountsRDD = finWordCountRDD
        .map(pair => pair.swap)
        .sortByKey(ascending = false)
        .map(pair => pair.swap)

      // Count the longest Finnish word with at least 150 appearances
      val longestWordFin = sortedFinWordCountsRDD
        .filter(count => count._2 > 150)
        .sortBy(count => -count._1.length)
        .first()
      val longestWord: String = longestWordFin._1

      println(s"The longest word appearing at least 150 times is '${longestWord}'")

      // Count the amount of words and the total length of words and then calculate the avg Finnish word length
      val totalCountFin = finFilteredRDD.count()
      val totalLengthFin = finFilteredRDD
        .map(pair => pair.length).reduce(_+_)
      val avgFin = totalLengthFin.toDouble / totalCountFin
      val avgFinString: String = f"$avgFin%.2f"

      // Count the amount of words and the total length of words and then calculate the avg English word length
      val totalCountEng = engFilteredRDD.count()
      val totalLengthEng = engFilteredRDD
        .map(pair => pair.length).reduce(_ + _)
      val avgEng = totalLengthEng.toDouble / totalCountEng
      val avgEngString: String = f"$avgEng%.2f"

      // Create data for averagedWordLengths DF
      val lengthData = Seq(
        ("Finnish", avgFinString),
        ("English", avgEngString)
      )

      // Create columns for averagedWordLengths DF
      val columns = Seq(
        "language",
        "average_word_length"
      )

      // Put the data and columns to the DF
      val averageWordLengths: DataFrame = spark
        .createDataFrame(lengthData).toDF(columns: _*)

      println("The average word lengths:")
      averageWordLengths.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Task 3 - K-Means clustering (2 points)
// MAGIC
// MAGIC You are given a dataset containing the locations of building in Finland. The dataset is a subset from [https://www.avoindata.fi/data/en_GB/dataset/postcodes/resource/3c277957-9b25-403d-b160-b61fdb47002f](https://www.avoindata.fi/data/en_GB/dataset/postcodes/resource/3c277957-9b25-403d-b160-b61fdb47002f) limited to only postal codes with the first two numbers in the interval 30-44 ([postal codes in Finland](https://www.posti.fi/en/zip-code-search/postal-codes-in-finland)). The dataset is in the XXXXX at folder `assignment/buildings.parquet`.
// MAGIC
// MAGIC [K-Means clustering](https://en.wikipedia.org/wiki/K-means_clustering) algorithm is an unsupervised machine learning algorithm that can be used to partition the input data into k clusters. Your task is to use the Spark ML library and its K-Means clusterization algorithm to divide the buildings into clusters using the building coordinates `latitude_wgs84` and `longitude_wgs84` as the basis of the clusterization. You should implement the following procedure:
// MAGIC
// MAGIC 1. Start with all the buildings in the dataset.
// MAGIC 2. Divide the buildings into seven clusters with K-Means algorithm using `k=7` and the longitude and latitude of the buildings.
// MAGIC 3. Find the cluster to which the Sähkötalo building from the Hervanta campus is sorted into. The building id for Sähkötalo in the dataset is `102363858X`.
// MAGIC 4. Choose all the buildings from the cluster with the Sähkötalo building.
// MAGIC 5. Find the cluster center for the chosen cluster of buildings.
// MAGIC 6. Calculate the largest distance from a building in the chosen cluster to the chosen cluster center. You are given a function `haversine` that you can use to calculate the distance between two points using the latitude and longitude of the points.
// MAGIC 7. While the largest distance from a building in the considered cluster to the cluster center is larger than 3 kilometers run the K-Means algorithm again using the following substeps.
// MAGIC     - Run the K-Means algorithm to divide the remaining buildings into smaller clusters. The number of the new clusters should be one less than in the previous run of the algorithm (but should always be at least two). I.e., the sequence of `k` values starting from the second run should be 6, 5, 4, 3, 2, 2, ...
// MAGIC     - After using the algorithm again, choose the new smaller cluster of buildings so that it includes the Sähkötalo building.
// MAGIC     - Find the center of this cluster and calculate the largest distance from a building in this cluster to its center.
// MAGIC
// MAGIC As the result of this process, you should get a cluster of buildings that includes the Sähkötalo building and in which all buildings are within 3 kilometers of the cluster center.
// MAGIC
// MAGIC Using the final cluster, find the answers to the following questions:
// MAGIC
// MAGIC - How many buildings in total are in the final cluster?
// MAGIC - How many Hervanta buildings are in this final cluster? (A building is considered to be in Hervanta if their postal code is `33720`)
// MAGIC
// MAGIC Some hints:
// MAGIC
// MAGIC - Once you have trained a KMeansModel, the coordinates for the cluster centers, and the cluster indexes for individual buildings can be accessed through the model object (`clusterCenters`, `summary.predictions`).
// MAGIC - The given haversine function for calculating distances can be used with data frames if you turn it into an user defined function.
// MAGIC

// COMMAND ----------

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
// some helpful constants
val startK: Int = 7
val seedValue: Long = 1

// the building id for Sähkötalo building at Hervanta campus
val hervantaBuildingId: String = "102363858X"
val hervantaPostalCode: Int = 33720

val maxAllowedClusterDistance: Double = 3.0


// returns the distance between points (lat1, lon1) and (lat2, lon2) in kilometers
// based on https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128
def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val R: Double = 6378.1  // radius of Earth in kilometers
    val phi1 = scala.math.toRadians(lat1)
    val phi2 = scala.math.toRadians(lat2)
    val deltaPhi = scala.math.toRadians(lat2 - lat1)
    val deltaLambda = scala.math.toRadians(lon2 - lon1)

    val a = scala.math.sin(deltaPhi * deltaPhi / 4.0) +
        scala.math.cos(phi1) * scala.math.cos(phi2) * scala.math.sin(deltaLambda * deltaLambda / 4.0)

    2 * R * scala.math.atan2(scala.math.sqrt(a), scala.math.sqrt(1 - a))
}
def haversineUDF: UserDefinedFunction = udf(haversine _)

// COMMAND ----------

val filePath = "XXXXX"
val buildingsDF = spark.read.parquet(filePath)

var coordinatesDF = buildingsDF
  .select("building_id", "latitude_wgs84", "longitude_wgs84", "postal_code") //selecting coordinates
  .distinct()

val specificBuilding = coordinatesDF.filter(col("building_id")===hervantaBuildingId)

var maxDist: Double = Double.PositiveInfinity
var K: Int = 7
var buildingsFeatures: DataFrame = null
var isFirstPrediction: Boolean = true
var predictions: DataFrame = null
var predictions_On_All: DataFrame = null
var specificBuildingFeatures: DataFrame = null
var maxDistanceFromCenter:DataFrame = null

//iterating through kmeans until maximum distance between center of the cluster and building is under 3km
while(maxDist > 3.0){

val kmeans = new KMeans().setK(K).setSeed(seedValue) //setting kmeans with K
//checkss if first iteration
if (K==7){
val assembler = new VectorAssembler()
  .setInputCols(Array("latitude_wgs84", "longitude_wgs84"))
  .setOutputCol("features")
buildingsFeatures = assembler.transform(coordinatesDF) //prepare dataset to vector format
specificBuildingFeatures = assembler.transform(specificBuilding)

K = K-1
}
else{
  
if(K>2){
K = K-1
}
buildingsFeatures = coordinatesDF

}


buildingsFeatures = buildingsFeatures.select("building_id","latitude_wgs84","longitude_wgs84","features","postal_code")

val model = kmeans.fit(buildingsFeatures) // fitting coordinates dataframe 

predictions = model.transform(specificBuildingFeatures) //sähkötalo clusters
predictions_On_All = model.transform(buildingsFeatures) //get all clusters


val cluster = predictions.select("prediction").head().getInt(0) //get wanted cluster


val clusterCenters = model.clusterCenters //get all cluster centers

val center = clusterCenters(cluster) //center for wanted cluster


coordinatesDF = predictions_On_All.filter($"prediction" === cluster)//df with initial cluster


//use center building and harvesine to calculate maximum distance in cluster
maxDistanceFromCenter = coordinatesDF.withColumn("centerlat", lit(center(0)))
.withColumn("centerlon", lit(center(1)))
.withColumn("distance", //calculate distance
  haversineUDF(
    col("centerlat"), col("centerlon"),
    col("latitude_wgs84").cast("double"), col("longitude_wgs84").cast("double")
  )
)
.select("latitude_wgs84", "longitude_wgs84","distance", "postal_code","distance")


maxDist = maxDistanceFromCenter.agg(max("distance").alias("max_distance")).first().getAs[Double]("max_distance")



}


val finalCluster: DataFrame = coordinatesDF

val clusterBuildingCount: Long = coordinatesDF.count()
val clusterHervantaBuildingCount: Long = coordinatesDF
  .filter(col("postal_code") === hervantaPostalCode) 
  .agg(count("*").alias("count")) 
  .first() 
  .getLong(0) 

println(s"Buildings in the final cluster: ${clusterBuildingCount}")
print(s"Hervanta buildings in the final cluster: ${clusterHervantaBuildingCount} ")
println(s"(${scala.math.round(10000.0*clusterHervantaBuildingCount/clusterBuildingCount)/100.0}% of all buildings in the final cluster)")
println("===========================================================================================")



// COMMAND ----------



