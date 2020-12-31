# Simple EDA for the 2020 NBA Eastern Conference Playoff Team Statistics
### Notes:
1) Dataframes were created from csv data pulled from [basketball-reference.com](https://www.basketball-reference.com/playoffs/NBA_2020.html).
2) This analysis was conducted using a Databricks Notebook to create analysis and visualizations of findings.

## Initial Findings

* Unsurprisingly, the highest scorer throughout the 2020 Eastern Conference Playoffs was Jimmy Butler (467 pts), followed closely by Jayson Tatum (437 pts).
* On the other hand, the highest **_per game_** scorer was Joel Embiid (30 ppg, 4 GP), followed by Nikola Vučević (28 ppg, 5 GP). Jimmy Butler ranked 5th, with 22.24 ppg, over 21 games.
* The highest statistically significant per minute point scorer (with at least 5 minutes played) was Giannis Antetokounmpo, with 0.866 points scored per minute played.
  - Note: the highest absolute per minute point scorer was Tacko Fall, with 1 points/min, however, he only played a total of 3 minutes across 2 games, with 1 made FG attempt, so I've chosen to exclude this stat.
* The top 3 players with the highest per game Simple PER's (Player Efficiency Rating), were Joel Embiid (37.853 sPER), followed by Giannis Antetokounmpo (36.367 sPER), and Jimmy Butler (29.763 PER)
  - Note: This Simple PER calculation is based on [Rusty LaRue's Simple PER article](http://www.rustylarue.com/more-than-94rsquo/player-efficiency-stats). This equation is, by no means an accurate representation of John Hollinger's much more complicated official PER rating, but it produces an adequate representation of player efficiency for the purposes of this EDA project.
