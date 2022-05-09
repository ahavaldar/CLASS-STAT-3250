##
## File: assignment12.py (STAT 3250)
## Topic: Assignment 12
##


##  In this assignment we revisit past NCAA men's basketball tournaments 
##  (including the glorious 2019 edition) using data from the file 
##
##      'ncaa.csv':  NCAA Men's Tournament Scores, 1985-2019
##
##  The organization of the file is fairly clear.  Each record has information
##  about one game, including the year, the teams, the final score, and each 
##  team's tournament seed.  

##  Two important points:
##    1) Each team is assigned a "seed" at the start of the tournament.  The
##       teams thought to be better are assigned smaller number seeds.  (So 
##       the best teams are assigned 1 and the worst assigned 16.)  In this 
##       assignment a "lower seed" refers to a worse team and hence larger 
##       seed number, with the opposite meaning for "higher seed". 
##    2) All questions refer only to the data in this in 'ncaa.csv' so you
##       don't need to worry about tournaments prior to 1985.

##  Note: The data set is from Data.World, with the addition of the 2019
##  tournament provided by your dedicated instructor. (There was no 2020
##  tournament and the 2021 tournament didn't turn out to your instructor's
##  liking so that data is omitted.)

##  Submission Instructions: Submit your code in Gradescope under 
##  'Assignment 12 Code'.  The autograder will evaluate your answers to
##  Questions 1-8.  You will also generate a separate PDF for the graphs
##  in Questions 9-11, to be submitted in Gradescope under 'Assignment 12 Graphs'.
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

ncaa = pd.read_csv("ncaa.csv")

## 1.  Find all schools that have won the championship. Report your results in
##     a Series that has the schools as index and number of championships for
##     values, sorted alphabetically by school.
q1sub = ncaa[ncaa['Region Name'] == 'Championship']         # subsetting championship games

team0 = q1sub.loc[(q1sub['Score']>q1sub['Score.1']), 'Team']            # getting winners under "team"
team1 = q1sub.loc[(q1sub['Score.1']>q1sub['Score']), 'Team.1']          # getting winners under "team.1"
teamtot = pd.concat([team0, team1], axis = 0)                           # concat
won = teamtot.groupby(teamtot).count().sort_index(ascending=True)       # count and sort

q1 = won  # Series of champions and counts


## 2.  Determine all schools that have been in the tournament at least 25 times.
##     Report your results as a Series with schools as index and number of times
##     in the tournament as values, sorted alphabetically by school.
ncaa1 = ncaa[ncaa['Round']== 1]                     # subsetting the first round

team0gb = ncaa1['Team'].groupby(ncaa1['Team']).count()          # counting the number of teams under team
team1gb = ncaa1['Team.1'].groupby(ncaa1['Team.1']).count()      # counting the number of teams under team1
q2tot = pd.concat([team0gb, team1gb], axis = 0).reset_index()                           # concat
q2ct = q2tot.groupby(q2tot['index']).sum().sort_index(ascending=True)           # sum and sort
q2ct = q2ct.squeeze()           # convert to series
q2ct = q2ct[q2ct>=25]           # subset

q2 = q2ct  # Series of schools and tournament appearance counts


## 3.  Find all years when the school that won the tournament was seeded 
##     3 or lower. (Remember that "lower" seed means a bigger number!) Give  
##     a DataFrame with years as index and corresponding school and seed
##     as columns (from left to right).  Sort by year from least to most recent.
q1sub
q3sub = q1sub[(q1sub['Seed'] >=3) | (q1sub['Seed.1']>=3)]       # subsetting games that have a participant lower than 3 seed
temp0 = q3sub.loc[(q3sub['Score']>q3sub['Score.1']), ['Year', 'Team', 'Seed']]            # getting winners under "team"
temp1 = q3sub.loc[(q3sub['Score.1']>q3sub['Score']), ['Year','Team.1', 'Seed.1']]          # getting winners under "team.1"
temp0 = temp0[temp0['Seed'] >=3]             # getting winners seeded 3 or lower
temp1 = temp1[temp1['Seed.1']>=3]                   # getting winners seeded 3 or lower
temp0 = temp0.set_index(temp0['Year']).drop(['Year'], axis=1)       # setting index
temp1 = temp1.set_index(temp1['Year']).drop(['Year'], axis=1)       # setting index
temp1 = temp1.rename(columns = {'Team.1':'Team', 'Seed.1':'Seed'})      # rename cols to concat
temp2 = pd.concat([temp0, temp1], axis = 0).sort_index(ascending=True)      # concat and sort

    
q3 = temp2  # DataFrame of years, schools, seeds


## 4.  Determine the average tournament seed for each school.  Make a Series
##     of all schools that have an average seed of 5.0 or higher (that is,
##     the average seed number is <= 5.0).  The Series should have schools
##     as index and average seeds as values, sorted alphabetically by
##     school
ncaa1
q4sub1 = ncaa1[['Team', 'Seed']]                    # subsetting team
q4sub2 = ncaa1[['Team.1', 'Seed.1']]                # subsetting team1
q4sub2 = q4sub2.rename(columns = {'Team.1':'Team', 'Seed.1':'Seed'})      # rename cols to concat
q4df = pd.concat([q4sub1, q4sub2], axis = 0)        # concat
q4mean = q4df.groupby(q4df['Team']).mean()          # calculating mean

q4mean = q4mean[q4mean['Seed']<=5].sort_index(ascending=True)              # schools with avg seed 5 or higher
q4mean = q4mean.squeeze()
q4 = q4mean  # Series of schools and average seed


## 5.  For each tournament round, determine the percentage of wins by the
##     higher seeded team. (Ignore games of teams with the same seed.)
##     Give a Series with round number as index and percentage of wins
##     by higher seed as values sorted by round in order 1, 2, ..., 6. 
##     (Remember, a higher seed means a lower seed number.)

temp30 = ncaa[ncaa['Seed']!= ncaa['Seed.1']]                    # getting rid of games where the seeds are the same
q5df = temp30[['Round', 'Seed', 'Score', 'Score.1', 'Seed.1']]
temp3 = q5df.loc[(q5df['Seed']<q5df['Seed.1']) & (q5df['Score']>q5df['Score.1']), 'Round']      # subsetting where the higher seed won
temp4 = q5df.loc[(q5df['Seed.1']<q5df['Seed']) & (q5df['Score.1']>q5df['Score']), 'Round']
temp5 = pd.concat([temp3, temp4], axis = 0)        # concat (this is all the games in which the higher seed won)
temp6 = temp30[['Round']]     
temp6 = temp6.squeeze()
wingb = temp5.groupby(temp5).count()            # total games won by higher seed for each round
totgb = temp6.groupby(temp6).count()            # total games for each round
q5perc = (wingb / totgb) *100                   # calculating percentage



q5 = q5perc  # Series of round number and percentage higher seed wins


## 6.  For each seed 1, 2, 3, ..., 16, determine the average number of games
##     won per tournament by a team with that seed.  Give a Series with seed 
##     number as index and average number of wins as values, sorted by seed 
##     number 1, 2, 3, ..., 16. (Hint: There are 35 tournaments in the data set
##     and each tournamentstarts with 4 teams of each seed.  We are not 
##     including "play-in" games which are not part of the data set.)
q6df = ncaa[['Seed', 'Score', 'Score.1', 'Seed.1']]             # subset
temp7 = q6df.loc[(q6df['Score']>q6df['Score.1']), 'Seed']           # finding seeds that won
temp8 = q6df.loc[(q6df['Score.1']>q6df['Score']), 'Seed.1']         # finding seeds that won 
temp9 = pd.concat([temp7, temp8], axis = 0)                           # concat
temp10 = temp9.groupby(temp9).count()                       # counting number of wins for each seed across all tournaments
temp10 = temp10 / 35                                        # getting avg wins per tournament (since 35 tournaments divide by 35)
temp10 = temp10 /4    

q6 = temp10  # Series of seed and average number of wins


## 7.  For each year's champion, determine their average margin of victory 
##     across all of their games in that year's tournament. Find the champions
##     that have an average margin of victory of at least 15. Give a DataFrame 
##     with year as index and champion and average margin of victory as columns
##     (from left to right), sorted by from highest to lowest average victory 
##     margin.

temp11 = q1sub.loc[(q1sub['Score']>q1sub['Score.1']), ['Year','Team']]            # getting winners under "team"
temp12 = q1sub.loc[(q1sub['Score.1']>q1sub['Score']), ['Year','Team.1']]          # getting winners under "team.1"
temp12 = temp12.rename(columns = {'Team.1' : 'Team'})
temp13 = pd.concat([temp11, temp12], axis=0)                                # concat
temp13 = temp13.set_index(temp13['Year']).drop(['Year'], axis=1)            # set index to year then drop the duplicate column
q7df = ncaa[['Year', 'Score', 'Team', 'Team.1', 'Score.1']]                 # subsetting original df
q7df = q7df.set_index(q7df['Year'])
q7df['Champ'] = temp13                                                      # new column with the champion for each year
q7df = q7df[(q7df['Team']==q7df['Champ'])|(q7df['Team.1'] == q7df['Champ'])]                # getting only the games the champion played in
q7df['Margin'] = abs(q7df['Score'] - q7df['Score.1'])                       # getting margin of victory 
q7mean = q7df['Margin'].groupby([q7df['Year'], q7df['Champ']]).mean()       # calculating avg margin of victory
q7ans = pd.DataFrame(q7mean).reset_index()                      
q7ans = q7ans.set_index(q7ans['Year']).drop(['Year'], axis=1)   
q7ans = q7ans[q7ans['Margin']>=15].sort_values(by=['Margin'], ascending=False)           # subsetting and sorting


q7 = q7ans  # DataFrame of years, schools, average margin of victory


## 8.  Determine the 2019 champion.  Use code to extract the correct school,
##     not your knowledge of college backetball history.
temp13 = q1sub.loc[(q1sub['Score']>q1sub['Score.1']), ['Year','Team']]            # getting winners under "team"
temp14 = q1sub.loc[(q1sub['Score.1']>q1sub['Score']), ['Year','Team.1']]          # getting winners under "team.1"
temp14 = temp14.rename(columns = {'Team.1' : 'Team'})
temp15 = pd.concat([temp13, temp14], axis=0)     
champ = temp15[temp15['Year']==2019]                    # extracting winner of 2019 tourney
champ = champ.to_string()                           # converting to string
champ = champ.split()[-1]                           # splitting and getting last entry which is winner

q8 = champ  # 2019 champion!


##  Questions 9-11: These require the creation of several graphs. In addition to 
##  the code in your Python file, you will also upload a PDF document (not Word!)
##  containing your graphs (be sure they are labeled clearly).  Include the
##  required code in this file and put your graphs in a PDF document for separate
##  submission.  All graphs should have an appropriate title and labels for
##  the axes.  For these questions the only output required are the graphs.
##  When your PDF is ready submit it under 'Assignment 12 Graphs' in Gradescope.


## 9.  For each year of the tournament, determine the average margin of
##     victory for each round.  Then make a histogram of these averages,
##     using 16 bins and a range of [0,32].
q9df = ncaa[['Year', 'Round', 'Score', 'Score.1']]          # subsetting
q9df['margin'] = abs(q9df['Score'] - q9df['Score.1'])       # calculating margin of victory
temp16 = q9df['margin'].groupby([q9df['Year'], q9df['Round']]).mean()       # getting mean margin of victory per round per year
temp16 = pd.DataFrame(temp16).reset_index()                      
temp17 = temp16.set_index(temp16['Year']).drop(['Year'], axis=1)            # setting index


plt.hist(temp17['margin'], bins=16, range=[0,32], color='orange', edgecolor='blue')   # specifying values
plt.title('Histogram of Avg Margin of Victory')
plt.ylabel("Counts")
plt.xlabel("Avg Margin of Victory")
plt.savefig("q9.pdf", format="pdf", bbox_inches="tight")
plt.show()



## 10. Produce side-by-side box-and-whisker plots, one using the Round 1
##     margin of victory for games where the higher seed wins, and one
##     using the Round 1 margin of victory for games where the lower
##     seed wins.  (Remember that higher seed = lower seed number.)
##     Orient the boxes vertically with the higher seed win data on the 
##     left.
q10df = ncaa[['Round', 'Seed', 'Score', 'Score.1','Seed.1']]          # subsetting
q10df = q10df[q10df['Round']==1]
q10df['margin'] = abs(q9df['Score'] - q9df['Score.1'])          # getting margin of victory

# HIGHER SEED
temp18 = q10df.loc[(q10df['Seed']<q10df['Seed.1']) & (q10df['Score']>q10df['Score.1']), 'margin']      # subsetting where the higher seed won round 1

# LOWER SEED
temp21 = q10df.loc[(q10df['Seed.1']>q10df['Seed']) & (q10df['Score.1']>q10df['Score']), 'margin']       # subsetting where lower seed won round 1

data = [temp18, temp21]             
plt.boxplot(data, sym='*') 
plt.xticks([1, 2], ['Higher', 'Lower']) # Specifies data group
plt.xlabel("Winning Seed")
plt.ylabel("Margin of Victory")
plt.title("Margin of Victory by Higher/Lower Seed Winning")
plt.savefig("q10.pdf", format="pdf", bbox_inches="tight")
plt.show()


## 11. Produce a bar chart for the number of Round 2 victories by seed.
##     The bars should proceed left to right by seed number 1, 2, 3, ...
q11df = ncaa[['Round', 'Seed', 'Score', 'Score.1', 'Seed.1']]       # subsetting
q11df = q11df[q11df['Round']==2]                        # subsetting round 2
temp22 = q11df.loc[(q11df['Score']>q11df['Score.1']), 'Seed']       # getting seed of winner
temp23 = q11df.loc[(q11df['Score.1']>q11df['Score']), 'Seed.1']
temp23 = pd.concat([temp22, temp23], axis=0)                # concat
temp24 = temp23.groupby(temp23).count()                     # count number of victories by seed
seeds = temp24.index

plt.bar(seeds, temp24, color='purple', edgecolor='white')
plt.xlabel("Seed")
plt.ylabel("Count")
plt.title("Number of Victories by Seed in Round 2")
plt.savefig("q11.pdf", format="pdf", bbox_inches="tight")
plt.show()
