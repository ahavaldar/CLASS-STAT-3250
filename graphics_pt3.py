##
## Topic: Graphics (Part 3)
##

import pandas as pd
import matplotlib.pyplot as plt

cars = pd.read_csv("cars.csv")
cars

#### Histograms (part 2)

# We can make a histogram with bars color coded to indicate the values
# of a categorical variable.  For instance, here's what we get
# for a histogram of car weights color coded to indicate the fuel type.
x1 = cars.loc[cars['Fuel']=='gas','Weight']
x2 = cars.loc[cars['Fuel']=='diesel','Weight']
plt.hist([x1, x2], color=['blue', 'orange'])
plt.show()

# We can add a legend to the graph that explains what the colors mean,
# along with the usual labels and title.
plt.hist([x1, x2], color=['blue', 'orange'])
plt.xlabel("Weight (lbs)")
plt.ylabel("Frequency")
plt.legend(['Gas', 'Diesel'])
plt.title('Weights by Gas-vs-Diesel')
plt.show()

#### Bar Charts

# Bar charts are suitable for displaying counts of categorical variables.   
# For instance, here is a bar chart displaying the number of each Style
# of car in the data set.
counts = cars['Style'].value_counts()  # number of each Style
styles = counts.index  # list of styles, in order of 'counts'
plt.bar(styles,counts)
plt.show()

# The usual addition of plot title and lables, plus some color
counts = cars['Style'].value_counts()  # number of each Style
styles = counts.index  # list of styles, in order of 'counts'
plt.bar(styles, counts, color='green', edgecolor='black')
plt.xlabel("Style")
plt.ylabel("Count")
plt.title("Car Counts by Style")
plt.show()

#### Box-and-whisker Plots

# Box-and-whisker plots (or just "box plot") are a standardized way of  
# displaying the distribution of data based on the five number summary:  
# minimum, first quartile, median, third quartile, and maximum.  Here is a 
# box plot of the Lengths of the cars.
plt.boxplot(cars['Length'])
plt.show()

# We can eliminate the outliers
plt.boxplot(cars['Length'], sym='')
plt.show()

# We can change outlier point symbols
plt.boxplot(cars['Length'], sym='rs') # 'rs' = red square
plt.show()

# We can have horizontal boxes
plt.boxplot(cars['Length'], sym='rs', vert=False) 
plt.show()

# We can change whisker length; value = multiple of IQR beyond the 1st and
# 3rd quartiles (end of box); default is 1.5.  We also add labels and a title
plt.boxplot(cars['Length'], sym='rs', vert=False, whis=0.75) 
plt.xlabel("Length (in)")
plt.yticks([1],['']) # This supresses the '1'
plt.title("Length of Cars")
plt.show()

# We can have side-by-side box plots.  We start by defining subsets of the
# data.
data1 = cars.loc[cars['Drive']=='rwd','Length']
data2 = cars.loc[cars['Drive']=='fwd','Length']
data3 = cars.loc[cars['Drive']=='4wd','Length']
data = [data1, data2, data3]

# Now we're ready to plot
plt.boxplot(data, sym='b^') # 'b^' = blue triangle
plt.xticks([1, 2, 3], ['RWD', 'FWD', '4WD']) # Specifies data group
plt.xlabel("Drive Type")
plt.ylabel("Length (in)")
plt.title("Car Length by Drive Type")
plt.show()




