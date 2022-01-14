# gayle

classification models for t20 cricket and betfair odds streaming

## description

two classification models to predict the winner of t20 matches (no ties). the first is a feature based model using aggregated cricket statistics about players in the two teams such as batting average, bowling economy etc.. the second is a player based model that uses the players presence in each team as a feature in addition to a few others such as how long ago and where the match took place. 

the project also features some code to backtest the models against historical betfair data and stream odds live from betfair. both of these make use of the excellent [betfairlightweight](https://github.com/liampauling/betfair) package.

### before running the model

* run cricsheet_fetch to download the cricsheet data
* run data_wrangle to wrangle the data for both models

## acknowledgments
betfairlightweight by liampauling
* [betfairlightweight](https://github.com/liampauling/betfair)

data from cricsheet (also excellent)
* [cricsheet](www.cricsheet.org)