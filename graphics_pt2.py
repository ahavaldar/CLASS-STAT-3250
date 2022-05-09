##
## Topic: Graphics (Part 2)
##

import pandas as pd
import matplotlib.pyplot as plt

cars = pd.read_csv("cars.csv")
cars

#### Scatter Plots

# A basic plot
x = cars['Weight']      # Define the x-axis data
y = cars['Engine Size'] # Define the y-axis data
plt.scatter(x, y)       # Generate the plot
plt.show()              # Display the plot

# Change the size and color of the points
area = 20 # Specify the size of the points
color = 'magenta' # point color; most standard colors seem available
plt.scatter(x, y, s = area, c = color)
plt.show()

# Add labels to the x-axis and y-axis, and a title
plt.title('Car Scatter Plot')
plt.xlabel('Weight (lbs)')
plt.ylabel('Engine Displacement (in^3)')
plt.scatter(x, y, s = area, c = color)
plt.show()

# Coloring points based on the variable 'Fuel'
colors = pd.Series(len(x)*['blue']) # Initialize vector to all 'blue'
colors[cars['Fuel'] == 'diesel'] = 'orange'
print(colors)
# This leave 'colors' as a list colored blue or orange depending on the
# fuel type.  Now we can plot

plt.scatter(x, y, s = area, c = colors)
plt.show()  # Note that we lost the labels and title; add them back

plt.title('Car Scatter Plot')
plt.xlabel('Weight (lbs)')
plt.ylabel('Engine Displacement (in^3)')
plt.scatter(x, y, s = area, c = colors)
plt.show()

# Changing the marker (point) shape
plt.title('Car Scatter Plot')
plt.xlabel('Weight (lbs)')
plt.ylabel('Engine Displacement (in^3)')
plt.scatter(x, y, marker = '^', s = area, c = colors)
plt.show()

# You can find a list of available markers at
#
# https://matplotlib.org/api/markers_api.html

# It's possible to use markers to indicate a categorical variable, but
# it requires one graph for each level.  Below we do this for 'Drive'
x1 = x[cars['Drive'] == 'rwd']  # Here we divide the x and y variables
x2 = x[cars['Drive'] == 'fwd']  # based on the value of 'Drive'
x3 = x[cars['Drive'] == '4wd']
y1 = y[cars['Drive'] == 'rwd']
y2 = y[cars['Drive'] == 'fwd']
y3 = y[cars['Drive'] == '4wd']

plt.scatter(x1, y1, marker='o', s = area, c = 'red')
plt.scatter(x2, y2, marker='^', s = area, c = 'red')
plt.scatter(x3, y3, marker='s', s = area, c = 'red')
plt.title('Car Scatter Plot')
plt.xlabel('Weight (lbs)')
plt.ylabel('Engine Displacement (in^3)')
plt.show()

# To use markers for one categorical variable and colors for another,
# we combine the two approaches: Divide up the x-variabel, the y-variable
# (which is done above) and the color vector.  The color vector is already
# blue and orange depending on the variable 'Fuel'.
c1 = colors[cars['Drive'] == 'rwd']  # Here we divide the color vector
c2 = colors[cars['Drive'] == 'fwd']  # based on the value of 'Drive'
c3 = colors[cars['Drive'] == '4wd']

plt.scatter(x1, y1, marker='o', s = area, c = c1)
plt.scatter(x2, y2, marker='^', s = area, c = c2)
plt.scatter(x3, y3, marker='s', s = area, c = c3)
plt.legend(['Rear WD', 'Front WD', 'Four WD'])
plt.title('Car Scatter Plot')
plt.xlabel('Weight (lbs)')
plt.ylabel('Engine Displacement (in^3)')
plt.show()

#### Histograms (part 1)

# Let's make a histogram of the variable 'Length' for our data.
x = cars['Length']
plt.hist(x)  # This gives histogram with default values
plt.show()

# We can adjust the number of bins and the range of x-values with
# optional arguments.
plt.hist(x, bins=14, range=[140,210])   
plt.show()

# We change the color of the rectangles, and give them a boundary
plt.hist(x, bins=14, range=[140,210], color='red', edgecolor='black')   
plt.show()

# Finally, we can label the axes and give the plot a title
plt.hist(x, bins=14, range=[140,210], color='red', edgecolor='black')   
plt.title('Car Lengths')
plt.ylabel("Counts")
plt.xlabel("Length (in)")
plt.show()

