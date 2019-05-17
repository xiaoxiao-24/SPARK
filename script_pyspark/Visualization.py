# ------------------------------------
# Using Visualizations: distplot
# ------------------------------------
# Understanding the distribution of our dependent variable is very important and can impact the type of model or preprocessing we do. A great way to do this is to plot it, however plotting is not a built in function in PySpark, we will need to take some intermediary steps to make sure it works correctly. In this exercise you will visualize the variable the 'LISTPRICE' variable, and you will gain more insights on its distribution by computing the skewness.

#The matplotlib.pyplot and seaborn packages have been imported for you with aliases plt and sns.



# Sample 50% of the dataframe df with sample() making sure to not use replacement and setting the random seed to 42.
# Convert the Spark DataFrame to a pandas.DataFrame() with toPandas().
# Select a single column and sample and convert to pandas
# sample(): sample(withReplacement, fraction, seed=None)
sample_df = df.select(['LISTPRICE']).sample(False, 0.5, 42)
pandas_df = sample_df.toPandas()

# Plot a distribution plot using seaborn's distplot() method.
# Plot distribution of pandas_df and display plot
sns.distplot(pandas_df)
plt.show()

# what is skewness 偏度衡量
# Import the skewness() function from pyspark.sql.functions and compute it on the aggregate of the 'LISTPRICE' column with the agg() method. Remember to collect() your result to evaluate the computation.
# Import skewness function
from pyspark.sql.functions import skewness

# Compute and print skewness of LISTPRICE
print(df.agg({'LISTPRICE': 'skewness'}).collect())

# ------------------------------------
# Using Visualizations: lmplot
# ------------------------------------
# Creating linear model plots helps us visualize if variables have relationships with the dependent variable. If they do they are good candidates to include in our analysis. If they don't it doesn't mean that we should throw them out, it means we may have to process or wrangle them before they can be used.

# seaborn is available in your workspace with the customary alias sns.

# Select a the relevant columns and sample
# Using the loaded data set df filter it down to the columns 'SALESCLOSEPRICE' and 'LIVINGAREA' with select().
# Sample 50% of the dataframe with sample() making sure to not use replacement and setting the random seed to 42.
sample_df = df.select(['SALESCLOSEPRICE', 'LIVINGAREA']).sample(False, 0.5, 42)

# Convert to pandas dataframe
pandas_df = sample_df.toPandas()

# Linear model plot of pandas_df
# Using 'SALESCLOSEPRICE' as your dependent variable and 'LIVINGAREA' as your independent, plot a linear model plot using seaborn lmplot().
sns.lmplot(x='SALESCLOSEPRICE', y='LIVINGAREA', data=pandas_df)
plt.show()
