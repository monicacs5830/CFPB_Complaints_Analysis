# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# + id="J9-hNPUKyuFN"
# !wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
# !tar xf spark-3.1.1-bin-hadoop3.2.tgz
# !pip install -q findspark

# + id="_WSfC_aItA_4"
# !wget -q https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.32.0/spark-bigquery-with-dependencies_2.12-0.32.0.jar


# + colab={"base_uri": "https://localhost:8080/"} id="EqjtivYTJ8EK" outputId="2d9c86d2-571f-4971-f385-858765d989a2"
# !wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

# -

# !pip install google-cloud-bigquery


# !pip install gcsfs


# + colab={"base_uri": "https://localhost:8080/"} id="RtUHnDor1I6n" outputId="05fea22d-b0ab-4113-d741-41ae5419e9e9"
# !java -version
# -

# !spark-submit --version

# + colab={"base_uri": "https://localhost:8080/"} id="-Nshj1NbIhvZ" outputId="41b3f45a-4f5c-4f54-c6af-576868de0a91"
# !pip install dash-bootstrap-components

# + id="XoaXzNGeyT4Z"
# JUPYTER
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"
os.environ["SPARK_HOME"] = "/Users/monicachandramurthy/Downloads/spark-3.1.1-bin-hadoop3.2"
# -

# Importing the necessary libraries for data manipulation and visualization
from pyspark.sql import functions as F
import gcsfs
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
# from pyngrok import ngrok
import dash
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, year, count
from pyspark.sql.functions import col
from pyspark.sql.functions import when
import pandas as pd
from scipy.stats import chi2_contingency
import dash_bootstrap_components as dbc
from dash import dash_table
import pandas as pd
import pickle
import random

# + id="vVCFU5batUB2"
import findspark
findspark.init("/Users/monicachandramurthy/Downloads/spark-3.1.1-bin-hadoop3.2")# SPARK_HOME
from pyspark.sql import SparkSession
from google.cloud import bigquery

# + id="bRG1WtFKAfEb"
# Authenticating with the Google Cloud account and setting up the BigQuery client:
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'data608-391503-e77b21b1d01c.json' #Google app credential json file
os.environ['NO_GCE_CHECK'] = 'True'  # GCE_Check Error
os.environ['GOOGLE_CLOUD_PROJECT'] = 'data608-391503' #Project ID
client = bigquery.Client()




# + id="GfcXdTSWBEXp"
# with open('/content/data608-391503-e77b21b1d01c.json', 'r') as f:
#     print(f.read())


# + colab={"base_uri": "https://localhost:8080/"} id="SnrRgBP-JmUA" outputId="47dcf75b-144e-43b3-c767-d5e8d2519d74"
from pyspark.sql import SparkSession

# Starting Spark session
spark = SparkSession.builder \
    .appName("Data 608 - CFPB Project") \
    .config("spark.driver.memory","16G") \
    .config("spark.jars", "spark-bigquery-with-dependencies_2.12-0.32.0.jar") \
    .getOrCreate()
    #.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    #.config("spark.kryoserializer.buffer.max", "32G") \
    

# Providing the Google Cloud project and bucket
bucket = "cfpbcomplaint"
spark.conf.set('temporaryGcsBucket', bucket)

# Directly loading the data from BigQuery into a Spark DataFrame
df_spark = spark.read.format('bigquery') \
    .option('table', 'bigquery-public-data.cfpb_complaints.complaint_database') \
    .load()

# Viewing the DataFrame schema
df_spark.printSchema()

# + colab={"base_uri": "https://localhost:8080/"} id="U-hF4nEnpZ8f" outputId="4e4aebd9-d3b8-4111-a3d4-7f60cefa9ec7"
df_spark.count()

# + id="kCHKzJZTvmR9"
# # Accessing the data from GCP
# dataset_ref = client.dataset("cfpb_complaints", project="bigquery-public-data")
# cfpb_complaints_table = dataset_ref.table('complaint_database')
# df_complaints = client.get_table(cfpb_complaints_table)


# + id="kDT1lOHJFb3I"
# # Using the bigquery to fetch the data from GCP
# query = """
# SELECT *
# FROM `bigquery-public-data.cfpb_complaints.complaint_database`
# LIMIT 100000
# """
# df_complaints = client.query(query).to_dataframe()


# + colab={"base_uri": "https://localhost:8080/"} id="jm-D8UCBv5F1" outputId="e2d35dc2-cb79-45db-d656-ead3a1d714f4"
# # EDA: count the values in each category / describing the data
# df_spark.groupBy('product').count().show()
# df_spark.groupBy('subproduct').count().show()
# df_spark.groupBy('issue').count().show()
# df_spark.groupBy('subissue').count().show()
# df_spark.groupBy('company_response_to_consumer').count().show()
# df_spark.groupBy('consumer_disputed').count().show()
df_spark.describe().show()
# -


# #### DATA CLEANING AND DATA WRANGLING

# + colab={"base_uri": "https://localhost:8080/"} id="pkrC1sIAJhmg" outputId="bf256875-c930-47d8-900c-2b7bf1ccd76e"
from pyspark.sql.functions import when, count
from pyspark.sql.functions import col
# Function to check for missing values
def count_missing(df):
    # DataFrame with the count of missing values for each column in the spark df
    missing_counts_df = df.select([count(when(col(c).isNull(), 1)).alias(c) for c in df.columns])

    return missing_counts_df


missing_counts_df = count_missing(df_spark)
missing_counts_df.show()


# + id="ihla-HjzzEGc"
# Handling missing values

from pyspark.sql.functions import when, lit

# Replacing missing values in 'subproduct', 'state', 'zip_code' with 'Unknown'
df_spark = df_spark.fillna({'subproduct': 'Unknown', 'subissue': 'Unknown', 'state': 'Unknown'})

# Replacing missing values in 'consumer_disputed' with 'Not Provided'
# df_spark = df_spark.fillna({'consumer_disputed': 'Not Provided'})

# Dropping columns 'tags' and 'company_public_response' as not required for our analysis
df_spark = df_spark.drop('tags', 'zip_code', 'company_public_response', 'consumer_consent_provided', 'complaint_id')

# For NLP analysis, creating a new DataFrame with non-null 'consumer_complaint_narrative'
#df_nlp = df_spark.filter(df_spark.consumer_complaint_narrative.isNotNull())

# Remove rows with missing 'company_response_to_consumer'
df_spark = df_spark.filter(df_spark.company_response_to_consumer.isNotNull())


# + colab={"base_uri": "https://localhost:8080/"} id="jn0aHIYEGQLi" outputId="4c16d9e3-5d5e-4c60-859b-db042504b5fb"
# checking after data cleaning
missing_counts_df = count_missing(df_spark)
missing_counts_df.show()

# + [markdown] id="8v9eg5qV1Z63"
# **USING SPARK**

# + [markdown] id="R0ATOkZOvH9C"
# ## **1a) How are complaints distributed by products?**

# +
# Data preparation and plot creation for page 1
product_subproduct_state_df_spark = df_spark.groupBy("product", "subproduct", "state").agg(count("state").alias("Counts_Complaints"))
product_subproduct_state_df = product_subproduct_state_df_spark.toPandas()
products = product_subproduct_state_df['product'].unique().tolist()
product_to_subproduct = product_subproduct_state_df.groupby('product')['subproduct'].unique().apply(list).to_dict()

products_df_spark = df_spark.groupBy("product").agg(F.count("product").alias("Counts_Complaints")).orderBy(F.desc("Counts_Complaints"))
products_df_pandas = products_df_spark.toPandas()
fig1a = px.bar(products_df_pandas, x='product', y='Counts_Complaints', color='product')
fig1a.update_layout(
    autosize=False,
    width=1500,
    height=800,
    title_text = 'Distribution of Complaint Types (Product)')

# + [markdown] id="iDMSsCVgvT06"
# The graph above displays the number of complaints associated with various financial product categories. The product category that received the most complaints was "credit reporting, credit repair services, or other personal consumer reports." This category had 1.6 million complaints, indicating that consumers faced numerous issues and challenges with credit reporting, repair services, and other similar personal consumer reports.
#
# Following closely behind was the product category of debt collection, which received more than 0.4 million complaints. Debt collection practices are often a contentious issue, and the significant number of complaints highlights the difficulties consumers encountered with debt collectors.
#
# Next in line was the mortgage product category, with just below 0.4 million complaints. Mortgages being a significant financial commitment for consumers, it is not surprising to observe a substantial number of complaints in this area.
#
# The remaining product categories had a considerably lower number of complaints, all below 0.2 million counts. This suggests that compared to credit reporting, debt collection, and mortgages, other product categories had fewer reported issues or complaints from consumers.
#
# Notably, the virtual currency product category received the fewest complaints in the dataset (almost none). This implies that virtual currencies, such as cryptocurrencies, were relatively less problematic for consumers, at least based on the reported complaints.
#
# Overall, the graph provides valuable insights into consumer concerns and pain points across various financial product categories. It highlights the areas where companies and regulators may need to focus their efforts to improve consumer experiences and address potential issues more effectively.

# + colab={"base_uri": "https://localhost:8080/"} id="uGg3tg4m5Np0" outputId="14bee3a6-7b22-44f4-99f2-17eef2addc2c"
# # !pip install dash dash-core-components dash-html-components


# + colab={"base_uri": "https://localhost:8080/", "height": 671} id="SKYKQWKe2Lsi" outputId="284df4ec-9a92-413e-cdf4-1e4876c2f6dd"
# import dash
# import dash_core_components as dcc
# import dash_html_components as html
# from dash.dependencies import Input, Output
# import plotly.graph_objects as go

# # Question 1d.How are complaints distributed by product and sub-product? Is there any variation in this distribution by state?
# product_subproduct_state_df_spark = df_spark.groupBy("product", "subproduct", "state").agg(F.count("state").alias("Counts_Complaints"))
# product_subproduct_state_df = product_subproduct_state_df_spark.toPandas()

# # creating a list of unique products and a dictionary of products to subproducts
# products = product_subproduct_state_df['product'].unique().tolist()
# product_to_subproduct = product_subproduct_state_df.groupby('product')['subproduct'].unique().apply(list).to_dict()

# # initializing the Dash app
# app = dash.Dash(__name__)

# # setting up the app layout
# app.layout = html.Div([
#     html.H2("Interactive Choropleth Map of Complaint Types by State"),
#     html.H3("Select Product:"),
#     dcc.Dropdown(
#         id='product-dropdown',
#         options=[{'label': product, 'value': product} for product in products],
#         value=products[0]
#     ),
#     html.H3("Select Subproduct:"),
#     dcc.Dropdown(
#         id='subproduct-dropdown'
#     ),
#     dcc.Graph(id='choropleth-graph')
# ])

# # updating the subproduct dropdown options based on the product dropdown value
# @app.callback(
#     Output('subproduct-dropdown', 'options'),
#     Input('product-dropdown', 'value')
# )
# def update_subproduct_dropdown(selected_product):
#     subproducts = product_to_subproduct[selected_product]
#     return [{'label': subproduct, 'value': subproduct} for subproduct in subproducts]

# # updating the graph based on the product and subproduct dropdown values
# @app.callback(
#     Output('choropleth-graph', 'figure'),
#     Input('product-dropdown', 'value'),
#     Input('subproduct-dropdown', 'value')
# )
# def update_graph(selected_product, selected_subproduct):
#     df_subproduct = product_subproduct_state_df[(product_subproduct_state_df['product'] == selected_product) & (product_subproduct_state_df['subproduct'] == selected_subproduct)]
#     fig1b = go.Figure(data=go.Choropleth(
#         locations=df_subproduct['state'],
#         z=df_subproduct['Counts_Complaints'],
#         locationmode='USA-states',
#         name=f"{selected_product}: {selected_subproduct}"
#     ))
#     fig1b.update_layout(
#         geo_scope='usa',  # limit map scope to USA
#         #title_text = 'Interactive Choropleth Map of Complaint Types by State',
#     )
#     return fig1b

# if __name__ == '__main__':
#     app.run_server(debug=True)


# + [markdown] id="FH4cOGhsxL0-"
# ## **Question 2a: How is the distribution of issues and sub-issues in the complaints?**

# +
# Data preparation and plot creation for page 2
issue_df = df_spark.groupby('issue').count().orderBy('count', ascending=False).toPandas()
issue_subissue_df = df_spark.groupby('issue', 'subissue').count().orderBy('count', ascending=False).toPandas()
issue_counts = issue_subissue_df.groupby('issue')['count'].sum()
issue_counts_sorted = issue_counts.sort_values(ascending=False)
issues = issue_counts_sorted.index.tolist()
issue_to_subissue = issue_subissue_df.groupby('issue')['subissue'].unique().apply(list).reindex(issues).to_dict()
top_issues = df_spark.groupby('issue').count().sort(desc('count')).limit(10)
issue_subissue_df_top = df_spark.join(top_issues, on='issue', how='inner').groupby('issue', 'subissue').count().sort(desc('count')).limit(15).toPandas()

# Making a dictionary to map full issues to abbreviations
abbreviations = {
    'Incorrect information on your report': 'Incorrect Info',
    'Problem with a credit reporting company\'s investigation into an existing problem': 'Investigation Problem',
    'Improper use of your report': 'Improper Use',
    'Loan modification,collection,foreclosure': 'Loan Modification',
    'Attempts to collect debt not owed': 'Unowed Debt Collection',
    'Loan servicing, payments, escrow account': 'Loan Servicing',
    'Trouble during payment process': 'Payment Trouble',
    'Written notification about debt': 'Debt Notification',
}

# Applying the abbreviations to the dataframe
issue_subissue_df_top['issue_abbrev'] = issue_subissue_df_top['issue'].map(abbreviations)

# Creating the plot with abbreviations
fig2b = px.bar(issue_subissue_df_top, y='issue_abbrev', x='count', color='subissue', orientation='h',
             title='Counts of Complaints by Issue and Subissue',
             labels={'count':'Count of Complaints', 'issue_abbrev':'Issue'},
             hover_data=['issue']) # include the original issue as a hover-over

fig2b.update_layout(yaxis={'categoryorder': 'total ascending'})


# +
# display(issue_subissue_df_top)

# + [markdown] id="IFIw0NyGxWoM"
# Based on the graph, an analysis of the distribution of complaints across various issue and subissue categories reveals the following key findings:
#
# Among all the issues, the most common complaint category was "incorrect information on one's report," with a significant count of 800,000 complaints. This suggests that a large number of consumers faced challenges due to inaccuracies in their credit reports.
#
# The second most prevalent issue was related to credit reporting companies' investigations into existing problems, which garnered over 400,000 complaints. Consumers expressed concerns about the effectiveness and thoroughness of these investigations.
#
# Another prominent issue was the "improper use of the report," with more than 300,000 complaints. This indicates that consumers encountered instances where their credit reports were utilized inappropriately.
#
# Furthermore, the issue of "attempting to collect debt not owed" received significant attention, with more than 100,000 complaints. This suggests that a considerable number of consumers faced collection attempts for debts they did not owe.
#
# On the other end of the spectrum, the issue with the least number of complaints was related to "written notification about debt," with less than 100,000 complaints. This issue seems to have affected relatively fewer consumers compared to other problems.
#
# Examining subissues within the category of "incorrect information on the report," complaints primarily revolved around "information belonging to someone else," accounting for over 500,000 complaints. Additionally, there were notable complaints about "incorrect account information" and "incorrect status" on credit reports, with 99,000 and 98,000 complaints, respectively.
#
# Conversely, the subissue of "incorrect personal information" received the least number of complaints, with only 50,000 reported cases.
#
# In summary, the graph's analysis sheds light on the most prevalent issues and subissues faced by consumers, emphasizing the importance of addressing inaccuracies in credit reports and the need for improved practices by credit reporting companies.

# + [markdown] id="KVVcMVDVUKs7"
# ## **Is there any relationship between the method used to submit the complaint and the response from the company or the outcome of the dispute?**

# +
# Data preparation and plot creation for page 3
# grouping the data by 'submitted_via' and 'company_response_to_consumer' and counting the instances
method_df = df_spark.groupBy('submitted_via', 'company_response_to_consumer').agg(count('*').alias('Count_Response'))

# ordering highest to least
method_df = method_df.orderBy('Count_Response', ascending=False)

method_df_pd = method_df.toPandas()

fig3 = px.bar(method_df_pd, x='submitted_via', y='Count_Response',
             color='company_response_to_consumer',
             title='Counts of Company Responses by Submission Method',
             labels={'Count_Response':'Count of Responses',
                     'submitted_via':'Submission Method',
                     'company_response_to_consumer':'Company Response'})
fig3.show()

# + [markdown] id="j0Rw54xOUPL3"
# According to the graph above, it illustrates the number of company responses to complaints, categorized by different methods of complaint submission. The submission method that received the highest number of company responses was through the web.
#
# For all types of submission methods, the most common company response was to close the complaints with an explanation. This indicates that a significant proportion of complaints were resolved by providing consumers with a detailed explanation of the actions taken or the reasons behind the issues raised.
#
# The next most frequent company response was to close the complaints with non-monetary relief. This suggests that many complaints were resolved by offering remedies or solutions that did not involve monetary compensation but aimed to address the concerns and provide some form of resolution.
#
# Responses categorized as "in progress" or "closed with monetary relief" followed closely after non-monetary relief. This implies that some complaints required ongoing attention or were ultimately resolved with financial compensation to the affected consumers.
#
# However, there were notable issues with certain submission methods. Specifically, complaints submitted via referral, phone, postal mail, or fax had low number of responses and web referral or email complaints were not responded to at all by the company. This highlights a concerning lack of responsiveness from companies to complaints submitted through these channels.
#
# In summary, the graph reveals that web submissions were the most common method for consumers to submit complaints, and most of these complaints received responses in the form of explanations or non-monetary relief. Nevertheless, there was a need for improvement in addressing complaints submitted via web referral or email, as companies failed to respond to those submissions.

# + colab={"base_uri": "https://localhost:8080/"} id="Ihx3P2r_Xtt2" outputId="f78aa794-676f-40c1-cefe-8fdede64db45"
# checking association using contingency table
from scipy.stats import chi2_contingency

# Create a cross-tabulation (contingency table)
contingency_table = pd.crosstab(method_df_pd['submitted_via'], method_df_pd['company_response_to_consumer'])

# Perform the Chi-Square Test of Independence
chi2, p, dof, expected = chi2_contingency(contingency_table)

# Print the results
print("chi2 statistic", chi2)
print("p-value", p)

# + [markdown] id="xZVerykSJ1iT"
# The p-value indicates the probability of observing a chi2 statistic as extreme as the one calculated (or more extreme) under the null hypothesis. If the p-value is less than the significance level (typically 0.05), we reject the null hypothesis and conclude that there is a significant relationship between the submission method and the company response.
# In the above case, p-value is high (0.99), which means we fail to reject the null hypothesis. This suggests that there's no statistically significant relationship between the method used to submit the complaint (submitted_via) and the response from the company (company_response_to_consumer).

# + [markdown] id="-Z3QZgh4cPwI"
# ## **4) Trend Identification: Distribution of complaints received over time**

# + [markdown] id="76GSmDomcnGR"
# ## **Can we discern any trends in how the CFPB handles cases or how companies respond over time?**
# ### **4a: Analyzing total complaints over time**

# +
# Data preparation and plot creation for page 4
# 4a: analyzing total complaints over time
complaints_over_time = df_spark.groupBy(df_spark.date_received).count().orderBy(df_spark.date_received).toPandas()
# Monthly moving average
complaints_over_time['monthly_mva'] = complaints_over_time['count'].rolling(window=30).mean()

# Base line plot
fig4a = go.Figure()
fig4a.add_trace(go.Scatter(x=complaints_over_time['date_received'], y=complaints_over_time['count'],
                           mode='lines', name='Complaints', line=dict(color='#FA9A85', width=0.5, dash='dash')))
fig4a.add_trace(go.Scatter(x=complaints_over_time['date_received'], y=complaints_over_time['monthly_mva'],
                           mode='lines', name='Monthly Moving Average', line=dict(color='darkred', width=2)))
fig4a.update_layout(title='Trend in complaints received over time',
                   xaxis_title='Date Received',
                   yaxis_title='Number of Complaints')


fig4a.show()

# + [markdown] id="-QrKcCB3cq3j"
# The graph presents a clear illustration of the complaint trends from 2012 to March 2023. Over this period, the number of complaints received has shown remarkable consistency, with a noticeable surge observed starting from 2020.
#
# Notably, the year 2023 stands out as the peak, with the highest number of complaints recorded during this period. This suggests that consumer complaints reached their highest point in that particular year.
#
# Overall, the graph portrays a consistent pattern of complaints over time, followed by a significant increase in recent years, reaching a peak in 2023. This trend could indicate various factors influencing consumer experiences or a potential rise in consumer awareness and reporting during that period. Further analysis would be required to understand the drivers behind this notable shift in complaint volumes.
# -

# #### 4b: Analyzing types of company responses over time

# 4b: analyzing types of company responses over time
responses_over_time = df_spark.filter(df_spark.issue.isNotNull()).groupBy(df_spark.date_received, df_spark.company_response_to_consumer).count().orderBy(df_spark.date_received).toPandas()
# Pivot table for better data structure
pivot_response_df = responses_over_time.pivot(index='date_received', columns='company_response_to_consumer', values='count').fillna(0)
pivot_response_df_mva = pivot_response_df.rolling(window=30).mean()  # Monthly moving average
fig4b = go.Figure()
for col in pivot_response_df_mva.columns:
    if str(col).lower() != 'nan':
        fig4b.add_trace(go.Scatter(x=pivot_response_df_mva.index, y=pivot_response_df_mva[col], mode='lines', name=col))
fig4b.update_layout(title="Trend in Company Responses Over Time (Moving Average)",
                   xaxis_title="Date Received",
                   yaxis_title="Number of Responses (Moving Average)",
                   legend_title="Company Response Type")

# + [markdown] id="f5JyrlawdsHQ"
# The graph provides a comprehensive overview of the trends in company responses to consumer complaints from 2012 to 2023. Below are the examinations of the key findings:
#
# 1. **In Progress Responses:** In 2023, there were some "in progress" responses recorded, likely indicating ongoing resolution efforts. Since these responses were not closed at the time of data collection, their numbers were limited to the year 2022.
#
# 2. **Untimely Responses:** Throughout the entire time period, untimely responses remained consistently low in number, suggesting that companies generally responded to complaints in a timely manner.
#
# 3. **Responses Closed with or without Relief:** There was a notable peak in the number of responses closed with or without relief in 2012, followed by a decline to zero, which remained consistent until 2022. This could indicate a change in company practices or data collection methods over the years.
#
# 4. **Responses Closed with Monetary Relief:** The number of responses closed with monetary relief ranged between 20 and 50 (moving average) from 2012 to 2023, showing a consistent approach by companies in offering financial compensation to resolve certain complaints.
#
# 5. **Responses Closed with Non-Monetary Relief:** From 2012 to the end of 2021, responses closed with non-monetary relief remained relatively low, with an average of around 100. However, there was a significant increase in this category starting in 2022, with the number of responses peaking below 1000. This could indicate an increasing focus on providing non-financial remedies to address consumer grievances.
#
# 6. **Responses Closed with Explanation:** The number of responses closed with an explanation displayed a gradual increase from 2012, starting from zero. The trend peaked in 2023, with a moving average of just below 2000, indicating that companies were more actively providing detailed explanations to consumers in response to their complaints.
#
# 7. **Simply Closed Responses:** The number of responses simply closed was relatively high, ranging from 10 to 16 (moving average) from 2012. However, these responses dropped to zero before 2018 and remained consistently low until 2023. This suggests a potential shift in how companies handled certain types of complaints during this period.
#
# Overall, the graph above highlights the trends in company responses to consumer complaints over the years. While some response categories remained relatively stable, there were significant changes in others, indicating evolving practices and a potential focus on more thorough explanations and non-monetary remedies in recent times. The data offers valuable insights into how companies addressed consumer complaints and grievances during this decade-long period.

# + [markdown] id="Uas9lwwvilMz"
# ## **4c: Analyzing complaints by product over time**
# -

# 4c: analyzing complaints by product over time
product_complaints_over_time = df_spark.groupBy(df_spark.date_received, df_spark.product).count().orderBy(df_spark.date_received).toPandas()
pivot_df = product_complaints_over_time.pivot(index='date_received', columns='product', values='count').fillna(0)
pivot_df_mva = pivot_df.rolling(window=30).mean()  # Monthly moving average


# + colab={"base_uri": "https://localhost:8080/", "height": 542} id="Bz1Hyz3teDEV" outputId="72cfc458-5cb8-48ee-a6e2-f2e979e8b599"
# # 4c: analyzing complaints by product over time
# # Pivot table for better data structure
# pivot_df = product_complaints_over_time.pivot(index='date_received', columns='product', values='count').fillna(0)
# pivot_df_mva = pivot_df.rolling(window=30).mean()  # Monthly moving average

# # Pivot table for better data structure
# fig4c = go.Figure()

# # Add all lines, but make them invisible
# for col in pivot_df_mva.columns:
#     fig4c.add_trace(go.Scatter(visible=False, x=pivot_df_mva.index, y=pivot_df_mva[col], mode='lines', name=col))

# # Make the first line visible
# fig4c.data[0].visible = True

# # Create dropdown options
# dropdown_options = []
# for i, col in enumerate(pivot_df_mva.columns):
#     visibility = [i==j for j in range(len(pivot_df_mva.columns))]
#     option = dict(label=col,
#                   method='update',
#                   args=[{'visible': visibility, 'title': f'Trend in Complaints for {col} Over Time (Moving Average)'}])
#     dropdown_options.append(option)

# # Update layout
# fig4c.update_layout(
#     updatemenus=[
#         dict(
#             active=0,
#             buttons=dropdown_options,
#         )
#     ],
#     title=f"Trend in Complaints for {pivot_df_mva.columns[0]} Over Time (Moving Average)",
#     xaxis_title="Date Received",
#     yaxis_title="Number of Complaints (Moving Average)",
#     legend_title="Product",
# )

# fig4c.show()


# + [markdown] id="mwYktdAZiovU"
#

# + [markdown] id="IOmr1loZtEZm"
# ## **4d: analyzing complaints by issue over time**

# +
# 4d: analyzing complaints by issue over time
yearly_issues = df_spark.groupBy(year("date_received").alias("year"), df_spark.issue).count().alias("count").orderBy("year", df_spark.issue)
yearly_max = yearly_issues.groupBy("year").agg({"count": "max"}).withColumnRenamed("max(count)", "max_complaints")
issue_complaints_over_time = yearly_issues.alias("yi").join(yearly_max.alias("ym"), (yearly_issues["year"] == yearly_max["year"]) & (yearly_issues["count"] == yearly_max["max_complaints"])).select("yi.year", "yi.issue", "yi.count").orderBy("yi.year").toPandas()

# 4d: analyzing complaints by issue over time
fig4d = px.bar(issue_complaints_over_time, x='year', y='count',
             color='issue',
             title='Issue with the highest number of complaints by year',
             labels={'year':'Year', 'count':'Number of Complaints', 'issue':'Issue Type'})
fig4d.show()

# + [markdown] id="86ZWLQ9EtIOC"
# The graph provides a detailed overview of the top 4 complaint issues with the highest number of complaints each year from 2012 to 2023. Let's summarize the findings:
#
# 1) **2012 and 2013:** The highest number of complaints in these years were related to loan modification, debt collection, and foreclosure issues. The number of complaints for each issue was close, with loan modification having 22,140 complaints and debt collection with 29,138 complaints.
#
# 2) **2014 to 2016:** During this period, the main complaint issue was "incorrect information on credit report." The number of complaints gradually increased from 21,581 in 2014 to 25,460 in 2015, and further to 32,570 in 2016.
#
# 3) **2017 to 2022:** "Incorrect information on one's report" emerged as the primary concern for consumers in these years. The number of complaints exhibited a steady upward trend, rising from 37,969 in 2017 to 196,491 in 2020. Although there was a slight decrease in 2021 (165,128 complaints), it reached a peak in 2022 with 229,641 complaints.
#
# 4) **2023 (up to the current date):** As of the present data, the most complained issue in 2023 is "improper use of one's report," with 74,883 complaints so far.
#
# Overall, the graph indicates that concerns related to credit reporting, such as incorrect information on reports, have consistently dominated the top complaint categories for consumers over the years. The data also suggests a notable increase in the number of complaints in recent years, particularly regarding issues with credit reports, signaling a growing area of consumer frustration and attention.

# + [markdown] id="b7cgIcgIfGgt"
# ## **Does the resolution method, whether monetary or nonmonetary relief influence the likelihood of a customer dispute?**

# +
#Data Preparation for Page 5
# Question 5b: Outcome Impact: Does the resolution method, whether monetary or nonmonetary relief, influence the likelihood of a customer dispute?
# fitering the dataframe where 'consumer_complaint_narrative', 'company_response_to_consumer' and 'consumer_disputed' are not NULL
response_counts_df = df_spark.filter(
    df_spark.company_response_to_consumer.isNotNull()
).groupBy("company_response_to_consumer").agg(count("company_response_to_consumer").alias("Count"))

response_counts_pandas_df = response_counts_df.toPandas()

# Plotting
fig5a = px.pie(response_counts_pandas_df,
             values='Count',
             names='company_response_to_consumer',
             title='Composition of Different Company Response Types')

# pie chart to donut chart by adding a hole parameter
fig5a.update_traces(hole=.3)

# + [markdown] id="Y17N2U4ffe0B"
# The graph presents the distribution of different types of company responses to consumer complaints. The majority of company responses, accounting for 75.5%, were **closed with explanation**. This indicates that most complaints were addressed by providing consumers with detailed explanations or clarifications.
#
# The next highest percentage of company responses, at 16.3%, fell under **closed with non-monetary relief**. This suggests that a significant portion of complaints were resolved by offering remedies or solutions that did not involve financial compensation.
#
# Only a small fraction, approximately 3.74%, of the complaints were **closed with monetary relief**. This means that a relatively small number of complaints were resolved with financial compensation provided to the affected consumers.
#
# Based on the data, 2.99% of the complaint reports were still **in progress**, with the likelihood that most of these responses were either in the process of being resolved or had not been closed at the time of data collection.
#
# Reports **closed with or without relief** and those with **untimely company response** each constituted less than 1% of the overall company response types. This suggests that these response categories were relatively less common in addressing consumer complaints.
#
# In summary, the graph indicates that the majority of consumer complaints received thorough attention through explanations from companies, while a notable portion received non-monetary relief. The relatively low percentage of complaints closed with monetary relief suggests that financial compensation was not the primary resolution approach. Additionally, the presence of complaints still "in progress" underscores the ongoing efforts to address consumer concerns. Overall, the data provides valuable insights into how companies handled various types of consumer complaints.
# -

# #### 5b: How does Company's response impact the dispute rate

# +
from pyspark.sql.functions import col
consumer_complaint_narrative_df = df_spark.filter(
    (df_spark.consumer_complaint_narrative.isNotNull()) &
    (df_spark.company_response_to_consumer.isNotNull()) &
    (df_spark.consumer_disputed.isNotNull())
)


# Count of the responses when the complaint narrative exists
response_narrative_count_df = consumer_complaint_narrative_df.groupBy(
    df_spark.company_response_to_consumer.alias("Response"),
    df_spark.consumer_disputed.alias("Dispute")
).count().alias("Count_response")

# rename the 'count' column to 'Count_response'
response_narrative_count_df = response_narrative_count_df.withColumnRenamed('count', 'Count_response')
# total counts for each type of response
total_counts = response_narrative_count_df.groupBy('Response').count().withColumnRenamed("count", "Total")
# dispute counts for each type of response
dispute_counts = response_narrative_count_df.filter(response_narrative_count_df['Dispute'] == True).groupBy('Response').count().withColumnRenamed("count", "Disputes")
# Join the total and dispute dataframes
joined_df = total_counts.join(dispute_counts, on='Response', how='left')
# dispute rates
joined_df = joined_df.withColumn('Dispute_rate_(in_percentage)', (joined_df['Disputes'] / joined_df['Total']) * 100)
# filtered out 'Untimely response' because % dispute is negligible
filtered_df = response_narrative_count_df.filter(col('Response') != 'Untimely response')
# convert the boolean column to string
filtered_df = filtered_df.withColumn(
    "Dispute",
    when(col("Dispute") == True, "True").otherwise("False")
)

# Plotting
fig5b = px.pie(filtered_df.toPandas(),
             values='Count_response',
             names='Dispute',
             color='Dispute',
             title='Percentage of Disputes and Non-Disputes for Each Response Type',
             facet_col='Response',
             facet_col_wrap=2,
             color_discrete_map={'True': '#DC3912', 'False': '#2CA02C'})

fig5b.update_traces(textinfo='percent+label')

# + [markdown] id="g9BeM9LLb9wP"
# Based on the graph above, we can observe the following dispute rates for different resolution methods:
#
# - Closed: Approximately 26.5% of complaints that were simply "Closed" ended up in a dispute.
# - Closed with explanation: About 24.2% of complaints that were "Closed with explanation" resulted in a dispute.
# - Closed with monetary relief: Only about 11.3% of complaints that were "Closed with monetary relief" resulted in a dispute.
# - Closed with non-monetary relief: About 12.4% of
# complaints that were "Closed with non-monetary relief" ended up in a dispute.
# - Untimely response: The dataset doesn't provide enough information to calculate a dispute rate for "Untimely response".
#
#
# From the available data, it appears that complaints resolved with **monetary relief** have a lower likelihood of resulting in a dispute compared to other resolution methods (11.3%). Conversely, complaints **closed with explanation** or simply **closed** have relatively higher dispute rates (24.2% and 26.5%, respectively).
#
#
# However, it is essential to conduct further statistical analysis and consider additional factors to establish a more comprehensive understanding of the relationship between resolution methods and dispute likelihood. Factors such as the nature of the complaint, company response quality, and consumer satisfaction might play significant roles in determining the dispute rates. Further research and analysis would be necessary to draw more definitive conclusions.

# + [markdown] id="jWvvL7X564gi"
# ### NLP - Response Prediction:

# +
# # !pip install pyngrok

# +
# #!ngrok config add-authtoken 2T2yTe61iUcOu8rXFEnDxUxy038_4EEELWwxkUSMpjnPG2Ha5

# +
# Data for Page 6
# Load model
with open('Complement_NB_model.pkl', 'rb') as best_model:
    Complement_NB = pickle.load(best_model)

# Load test data
url='https://raw.githubusercontent.com/AliHaghighat1/Consumer-Complaints/main/Narrative_test.csv'   
Narrative_test_set=pd.read_csv(url)
# -

# ## Building a Dashboard

# + colab={"base_uri": "https://localhost:8080/", "height": 1000} id="Nu0QltGZ384O" outputId="646e9804-efea-4d49-c5bf-932c029b431e"
# Initializing Dashboard
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.ZEPHYR], suppress_callback_exceptions=True)
# define the sidebar
sidebar = dbc.Nav(
    [
        dbc.NavLink("Home", href="/", active="exact"),
        dbc.NavLink("Products and Subproducts Distribution", href="/page-1", active="exact"),
        dbc.NavLink("Issues and Subissues Distribution", href="/page-2", active="exact"),
        dbc.NavLink("Complaints by Submission Method", href="/page-3", active="exact"),
        dbc.NavLink("Trends in Complaints in CFPB", href="/page-4", active="exact"),
        dbc.NavLink("Outcome Impact Analysis", href="/page-5", active="exact"),
        dbc.NavLink("Prediction of Company's Response", href="/page-6", active="exact"),
#         dbc.NavLink("Sentiment Analysis", href="/page-6", active="exact"),
    ],
    vertical="md",
    pills=True,
)
# Layouts
# Button style with padding
button_style = {'margin': '20px', 'margin-left': '20px', 'margin-right': '20px'}
# Heading stype with padding
heading_style = {'margin': '15px', 'margin-left': '10px', 'margin-right': '10px'}

sample_data = df_spark.limit(5).toPandas()  # limit data to 5 rows and convert to pandas

# Defining the homepage with introduction, sample data and buttons
homepage = html.Div([
    html.H1(" Understanding Financial Complaint Outcomes : A Data-Driven Approach", style= heading_style),
    html.Br(),
    html.Img(src='https://www.consumerfinance.gov/static/img/logo_237x50@2x.1a8febf782f9.png', style={'display': 'block', 'margin-left': 'auto', 'margin-right': 'auto'}),
    html.Br(),
    html.P("""
    The financial industry plays a key role in supporting personal and business financial goals. 
    Like any sector, it sees its fair share of customer dissatisfaction, which, when analyzed, can offer crucial feedback and insights into customer expectations and problem areas. 
    This project aims to analyze financial product complaints, predict their outcomes, and uncover insights into consumer behavior and possible improvements for financial services.
    """),  
    html.H2("Dataset"),
    html.Br(),
    html.Br(),
    html.P("""
    We use the Consumer Financial Protection Bureau (CFPB) Complaint Database, hosted on Google's BigQuery public datasets. 
    This 2.15GB dataset consists of over 3.4 million rows of data on consumer complaints related to financial products and services reported to the CFPB from 2011 to 2023. 
    Key variables include 'date_received', 'product', 'issue', 'consumer_complaint_narrative', 'company_name', 'state', 'company_response_to_consumer', 'timely_response', and 'consumer_disputed'. 
    The latter two will serve as response variables for complaint distribution and satisfaction analysis.
    """),
    html.Br(),
    html.H5("Below is a snapshot of our data:"),
    dbc.Card(
        dbc.CardBody(
            dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in sample_data.columns],
                data=sample_data.to_dict('records'),
                style_table={'overflowX': 'auto'},  # Enable horizontal scrolling
            ),
        ), style={'margin': '20px'}, className= "bg-primary mb-3"
    ),
    html.Br(),
    dbc.Container( 
        dbc.Row(
            html.Div(className='nav-buttons', children=[
                dbc.Button("Next Section", color="primary", href='/page-1', className="mr-1"),
            ], style={'textAlign': 'center', 'margin': '20px'}), justify='center'
        ), fluid=True
    ),
])
# Layout for page 1
page_1 = html.Div([
    dcc.Link('Go to Home', href='/'),
    html.Br(),
    html.H3("Complaint Distribution"),
    html.Br(),
    html.H4("How are complaints distributed by product and sub-product? Is there any variation in this distribution by state?"),
    html.Br(),
    dcc.Graph(figure=fig1a),
    html.Br(),
    html.P("The graph illustrates complaint numbers for different financial product categories. 'Credit reporting, credit repair services, or other personal consumer reports' received the most complaints (1.6 million), indicating consumer issues. 'Debt collection' followed with over 0.4 million complaints, reflecting challenges faced with debt collectors. 'Mortgage' had just below 0.4 million complaints, unsurprising due to its significant financial commitment. Other categories had fewer complaints, all below 0.2 million counts, suggesting fewer issues compared to credit reporting, debt collection, and mortgages. Interestingly, 'virtual currency' had the fewest complaints, implying relative consumer satisfaction. The graph offers valuable insights for companies and regulators to enhance consumer experiences and address potential issues effectively."),
    html.Br(),
    html.H4("Interactive Choropleth Map of Complaint Types by State"),
    html.H5("Select a Product:"),
    dcc.Dropdown(
        id='product-dropdown',
        options=[{'label': product, 'value': product} for product in products],
        value=products[0]
    ),
    html.H5("Select a Subproduct:"),
    dcc.Dropdown(
        id='subproduct-dropdown'
    ),
    dcc.Graph(id='choropleth-graph'),
    html.Br(),
    html.Br(),
#     dbc.Button("Previous Section", color="primary", href='/page-4', className="mr-1", style=button_style),
    html.Br(),
    dbc.Container( 
        dbc.Row(
            html.Div(className='nav-buttons', children=[
#                 dbc.Button("Previous Section", color="primary", href='/page-1', className="mr-1"),   
                dbc.Button("Home", color="primary", href='/', className="mr-1"),
                dbc.Button("Next Section", color="primary", href='/page-2', className="mr-1", style={'margin-left': '20px'}),
            ], style={'textAlign': 'center', 'margin': '20px'}), justify='center'
        ), fluid=True
    ),
])

second_question_key_findings = """
Key Findings:
- "Incorrect information on one's report" was the most common issue with 800,000 complaints, indicating credit report inaccuracies.
- The second most common issue was related to investigations by credit reporting companies, with over 400,000 complaints, highlighting concerns about their effectiveness.
- "Improper use of the report" had 300,000 complaints, showing credit reports are sometimes used inappropriately.
- "Attempting to collect debt not owed" received over 100,000 complaints, indicating collection attempts for debts consumers didn't owe.
- "Written notification about debt" had the fewest complaints, less than 100,000.

Sub-issues in "incorrect information on the report":
- "Information belonging to someone else" had over 500,000 complaints.
- "Incorrect account information" and "incorrect status" had 99,000 and 98,000 complaints, respectively.
- "Incorrect personal information" received the least complaints with only 50,000 cases.

In summary, the analysis shows the need for addressing credit report inaccuracies and improving practices by credit reporting companies.
"""

# Layout for page 2
page_2 = html.Div([
    dcc.Link('Go to Home', href='/'),
    html.Br(),
    html.H3("Issue Prevalence"),
    html.Br(),
    html.H4("What are the most frequently mentioned issues and sub-issues in the complaints?"),
    html.Br(),
    html.H4("Interactive Plot for Distribution of Issue Types"),
    html.Br(),
    html.H5("Select an Issue:"),
    dcc.Dropdown(
        id='issue-dropdown',
        options=[{'label': issue, 'value': issue} for issue in issues],
        value=issues[0]
    ),
    dcc.Graph(id='bar-graph'),
    dcc.Graph(figure=fig2b),
    html.Br(),
    html.P(second_question_key_findings, style={'font-size': '18px'}),
    html.Br(),
    html.Br(),
    dbc.Container( 
        dbc.Row(
            html.Div(className='nav-buttons', children=[
                dbc.Button("Previous Section", color="primary", href='/page-1', className="mr-1"),   
                dbc.Button("Home", color="primary", href='/', className="mr-1", style={'margin-left': '20px'}),
                dbc.Button("Next Section", color="primary", href='/page-3', className="mr-1", style={'margin-left': '20px'}),
            ], style={'textAlign': 'center', 'margin': '20px'}), justify='center'
        ), fluid=True
    ),
])

#Layout for page 3
page_3 = html.Div([
    dcc.Link('Go to Home', href='/'),
    html.Br(),
    html.H3("Submission Method Impact"),
    html.Br(),
    html.H4("Is there any relationship between the method used to submit the complaint and the response from the company or the outcome of the dispute? "),
    html.Br(),
    dcc.Graph(figure=fig3),
    html.Br(),
    html.P(
        """
        According to the graph above, it illustrates the number of company responses to complaints, categorized by different methods of complaint submission. The submission method that received the highest number of company responses was through the web.
        For all types of submission methods, the most common company response was to close the complaints with an explanation. This indicates that a significant proportion of complaints were resolved by providing consumers with a detailed explanation of the actions taken or the reasons behind the issues raised.
        The next most frequent company response was to close the complaints with non-monetary relief. This suggests that many complaints were resolved by offering remedies or solutions that did not involve monetary compensation but aimed to address the concerns and provide some form of resolution.
        Responses categorized as "in progress" or "closed with monetary relief" followed closely after non-monetary relief. This implies that some complaints required ongoing attention or were ultimately resolved with financial compensation to the affected consumers.
        However, there were notable issues with certain submission methods. Specifically, complaints submitted via referral, phone, postal mail, or fax had low numbers of responses, and web referral or email complaints were not responded to at all by the company. This highlights a concerning lack of responsiveness from companies to complaints submitted through these channels.
        In summary, the graph reveals that web submissions were the most common method for consumers to submit complaints, and most of these complaints received responses in the form of explanations or non-monetary relief. Nevertheless, there was a need for improvement in addressing complaints submitted via web referral or email, as companies failed to respond to those submissions.
        """
    ),
    html.Br(),
    html.Br(),
    html.H4("Chi-Square Test of Independence to determine whether there is a significant relationship between the method used to submit complaints (submitted_via) and the response from the company (company_response_to_consumer)."),
    html.Br(),
    html.P("Null Hypothesis: There is no significant relationship between the two variables"),
    html.Br(),
    html.P("Alternative Hypothesis: There is a significant relationship."),
    html.Br(),
    html.Div(id='chi-square-test'),
    html.Br(),
    html.Br(),
    dbc.Container( 
        dbc.Row(
            html.Div(className='nav-buttons', children=[
                dbc.Button("Previous Section", color="primary", href='/page-2', className="mr-1"),   
                dbc.Button("Home", color="primary", href='/', className="mr-1", style={'margin-left': '20px'}),
                dbc.Button("Next Section", color="primary", href='/page-4', className="mr-1", style={'margin-left': '20px'}),
            ], style={'textAlign': 'center', 'margin': '20px'}), justify='center'
        ), fluid=True
    ),
])

# Layout for page 4
page_4_layout = html.Div([
    dcc.Link('Go to Home', href='/'),
    html.Br(),
    html.H3("Trend Identification"),
    html.Br(),
    html.H4("Can we discern any trends in how the CFPB handles cases or how companies respond over time? Are there unusually high instances of responses for any specific product or issue category?"),
    html.Br(),
    html.H4("Analyzing total complaints over time"),
    dcc.Graph(
        id='graph_4a',
        figure={
            'data': [
                go.Scatter(x=complaints_over_time['date_received'], y=complaints_over_time['count'],
                           mode='lines', name='Actual Frequency of Complaints'),
                go.Scatter(x=complaints_over_time['date_received'], y=complaints_over_time['monthly_mva'],
                           mode='lines', name='Monthly Moving Average of Complaints')
            ],
            'layout': go.Layout(title='Trend in complaints received over time')
        }
    ),
    html.Br(),
    html.P("""The above graph
    portrays a consistent pattern of complaints over time, followed by a significant increase in recent years, reaching a peak in 2023. 
    This trend could indicate various factors influencing consumer experiences or a potential rise in consumer awareness and reporting during that period. 
    Further analysis would be required to understand the drivers behind this notable shift in complaint volumes."""),
    html.Br(),
    html.H4("Analyzing Company Responses Over Time"),
    dcc.Graph(figure=fig4b),
    html.Br(),
    html.P("""The graph above highlights the trends in company responses to consumer complaints over the years. 
    While some response categories remained relatively stable, there were significant changes in others, indicating evolving practices and a potential focus on more thorough explanations and non-monetary remedies in recent times. 
    The data offers valuable insights into how companies addressed consumer complaints and grievances during this decade-long period."""),
    html.Br(),
    html.H4("Analyzing Complaints by Product Types Over Time"),
    dcc.Dropdown(
        id='dropdown_4c',
        options=[
            {'label': i, 'value': i} for i in pivot_df_mva.columns
        ],
        value=pivot_df_mva.columns[0]
    ),
    dcc.Graph(
        id='graph_4c'
    ),
    html.H4("Analyzing issues over time"),
    dcc.Graph(figure=fig4d),
    html.Br(),
    html.P("""The graph indicates that concerns related to credit reporting, such as incorrect information on reports, have consistently dominated the top complaint categories for consumers over the years. 
    The data also suggests a notable increase in the number of complaints in recent years, particularly regarding issues with credit reports, signaling a growing area of consumer frustration and attention."""),
    html.Br(),
    dbc.Container( 
        dbc.Row(
            html.Div(className='nav-buttons', children=[
                dbc.Button("Previous Section", color="primary", href='/page-3', className="mr-1"),   
                dbc.Button("Home", color="primary", href='/', className="mr-1", style={'margin-left': '20px'}),
                dbc.Button("Next Section", color="primary", href='/page-5', className="mr-1", style={'margin-left': '20px'}),
            ], style={'textAlign': 'center', 'margin': '20px'}), justify='center'
        ), fluid=True
    ),
])

# Layout for Page 5
page_5_layout = html.Div([
    dcc.Link('Go to Home', href='/'),
    html.Br(),
    html.H3("Outcome Impact"),
    html.Br(),
    html.H4(": Does the resolution method, whether monetary or nonmonetary relief, influence the likelihood of a customer dispute?"),
    html.Br(),
    dcc.Graph(id='page-5-graph-a', figure=fig5a),
    html.Br(),
    html.P("""
    The above graph indicates that the majority of consumer complaints received thorough attention through explanations from companies, while a notable portion received non-monetary relief. 
    The relatively low percentage of complaints closed with monetary relief suggests that financial compensation was not the primary resolution approach. 
    Additionally, the presence of complaints still "in progress" underscores the ongoing efforts to address consumer concerns. 
    Overall, the data provides valuable insights into how companies handled various types of consumer complaints."""),
    html.Br(),
    dcc.Graph(id='page-5-graph-b', figure=fig5b),
    html.Br(),
    html.P("""From the above graph, it can be observed that complaints resolved with monetary relief have a lower likelihood of resulting in a dispute compared to other resolution methods (11.3%). 
    Conversely, complaints closed with explanation or simply closed have relatively higher dispute rates (24.2% and 26.5%, respectively).
    """),
    html.Br(),
    dbc.Container( 
        dbc.Row(
            html.Div(className='nav-buttons', children=[
                dbc.Button("Previous Section", color="primary", href='/page-4', className="mr-1"),   
                dbc.Button("Home", color="primary", href='/', className="mr-1", style={'margin-left': '20px'}),
                dbc.Button("Next Section", color="primary", href='/page-6', className="mr-1", style={'margin-left': '20px'}),
            ], style={'textAlign': 'center', 'margin': '20px'}), justify='center'
        ), fluid=True
    ),
])

import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output, State

# Layout for Page 6
page_6_layout = html.Div([
    dbc.Container(
        dbc.Row(
            [
                html.H3("The Company's Response Prediction Based On the Consumer Narrative", className="text-center"),
                html.P("The narratives are selected randomly from the test dataset when you click on Generate Prediction button below", className="text-center"),
                dbc.Button("Generate Prediction", id="prediction-button", className="mr-1", style={'margin': '20px'}),
            ], className="py-5 text-center"
        ), fluid=True
    ),
    dbc.Container(
        [
            dbc.Row(
                [
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    html.H4("Consumer Narrative", className="card-title"),
                                ),
                                dbc.CardBody(
                                    [
                                        html.P(id="narrative", className="card-text"),
                                    ]
                                )
                            ], className="bg-light mb-3"
                        ), md=4
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    html.H4("Predicted Response", className="card-title"),
                                ),
                                dbc.CardBody(
                                    [
                                        html.H5(id="prediction", className="card-text"),
                                    ]
                                )
                            ], className="bg-light mb-3"
                        ), md=4
                    ),
                    dbc.Col(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    html.H4("Probability Of The Response", className="card-title"),
                                ),
                                dbc.CardBody(
                                    [
                                        html.H5(id="prob_max", className="card-text"),
                                    ]
                                )
                            ], className="bg-light mb-3"
                        ), md=4
                    ),
                ]
            ),
        ], fluid=True
    ),
    #dbc.Button("Generate prediction", id="prediction-button", className="mr-1", style={'margin': '20px'}),
    html.Br(),
    html.Br(),
    dbc.Container( 
        dbc.Row(
            html.Div(className='nav-buttons', children=[
                dbc.Button("Previous Section", color="primary", href='/page-5', className="mr-1"),   
                dbc.Button("Home", color="primary", href='/', className="mr-1", style={'margin-left': '20px'}),
            ], style={'textAlign': 'center', 'margin': '20px'}), justify='center'
        ), fluid=True
    ),
])

# html.Div([
#     dbc.Button("Generate prediction", id="prediction-button", className="mr-1", style={'margin': '20px'}),
#     dash_table.DataTable(
#         id='prediction-table',
#         columns=[{"name": "Narratives", "id": "Narratives"}, {"name": "Prediction", "id": "Prediction"}, {"name": "Probabiliy", "id": "Probabiliy"}],
#     ),
# ])



# Updating the page content based on the URL
@app.callback(Output('page-content', 'children'),
              Input('url', 'pathname'))
def display_page(pathname):
    if pathname == '/':
        return homepage
    elif pathname == '/page-1':
        return page_1
    elif pathname == '/page-2':
        return page_2
    elif pathname == '/page-3':
        return page_3
    elif pathname == '/page-4':
        return page_4_layout
    elif pathname == '/page-5':
        return page_5_layout
    elif pathname == '/page-6':
        return page_6_layout
#     elif pathname == '/page-6':
#         return page_6_layout
    else:
        return dbc.Jumbotron([
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised...")
        ])

# Callbacks for page 1
@app.callback(
    Output('subproduct-dropdown', 'options'),
    Input('product-dropdown', 'value')
)
def update_subproduct_dropdown(selected_product):
    subproducts = product_to_subproduct[selected_product]
    return [{'label': subproduct, 'value': subproduct} for subproduct in subproducts]

@app.callback(
    Output('choropleth-graph', 'figure'),
    Input('product-dropdown', 'value'),
    Input('subproduct-dropdown', 'value')
)
def update_graph(selected_product, selected_subproduct):
    df_subproduct = product_subproduct_state_df[(product_subproduct_state_df['product'] == selected_product) & (product_subproduct_state_df['subproduct'] == selected_subproduct)]
    fig1b = go.Figure(data=go.Choropleth(
        locations=df_subproduct['state'],
        z=df_subproduct['Counts_Complaints'],
        locationmode='USA-states',
        name=f"{selected_product}: {selected_subproduct}"
    ))
    fig1b.update_layout(
        geo_scope='usa')
    return fig1b

# Callbacks for page 2
@app.callback(
    Output('bar-graph', 'figure'),
    Input('issue-dropdown', 'value')
)
def update_graph(selected_issue):
    df_issue = issue_subissue_df[(issue_subissue_df['issue'] == selected_issue)]
    fig2a = px.bar(df_issue, x='subissue', y='count', labels={'subissue':'Sub-Issues', 'count':'Count'})
    fig2a.update_layout(title_text=f"Distribution of sub-issues for {selected_issue}")
    return fig2a

# Callbacks for page 3
@app.callback(Output('chi-square-test', 'children'),
              #Output('chi-square-test-dispute-table', 'children'),
              Input('url', 'pathname')
             )
def perform_chi_square_test(pathname):
    if pathname == '/page-3':
        # Create a cross-tabulation (contingency table)
        contingency_table = pd.crosstab(method_df_pd['submitted_via'], method_df_pd['company_response_to_consumer'])

        # Perform the Chi-Square Test of Independence
        chi2, p, dof, expected = chi2_contingency(contingency_table)
        
#         # Create a Dash DataTable from the contingency table
#         table = dash_table.DataTable(
#             data=contingency_table.reset_index().to_dict('records'),
#             columns=[{'name': i, 'id': i} for i in contingency_table.reset_index().columns]
#         )
        
        # Test result interpretation
        if p < 0.05:
            interpretation = "The p-value is less than 0.05, we reject the null hypothesis and conclude that there is a significant relationship between the submission method and the company response."
        else:
            interpretation = "The p-value is greater than 0.05, we fail to reject the null hypothesis. This suggests that there's no statistically significant relationship between the method used to submit the complaint (submitted_via) and the response from the company (company_response_to_consumer)."

        # Return formatted string
        return [
            #html.H3("Contingency Table"),
            #table,  # Display the contingency table
            html.Br(),
#             html.H3("Chi-Square Test Result"),
#             html.P(f"Chi2 Statistic: {chi2}, P-value: {p}"),
            html.Div(className="card text-white bg-primary mb-3", style={"max-width": "80rem"}, children=[
            html.Div(className="card-header", children="Chi-Square Test Results"),
            html.Div(className="card-body", children=[
            #html.H4(className="card-title", children="Chi-Square Test Results"),
            html.P(className="card-text", children=f"Chi-square statistic: {chi2}, p-value: {p}, Degrees of freedom: {dof}"),
            html.P("Interpretation: " + interpretation)# Replace X, Y,  with the actual results
        ]),
    ])
        ]
    else:
        return ""
    
# # Callbacks for page 4
@app.callback(
    Output(component_id='graph_4c', component_property='figure'),
    [Input(component_id='dropdown_4c', component_property='value')]
)
def update_graph(selected_product):
    fig4c = go.Figure()
    fig4c.add_trace(go.Scatter(x=pivot_df_mva.index, y=pivot_df_mva[selected_product], mode='lines', name=selected_product))
    fig4c.update_layout(
        title=f"Trend in Complaints for {selected_product} Over Time (Moving Average)",
        xaxis_title="Date Received",
        yaxis_title="Number of Complaints (Moving Average)",
        legend_title="Product",
    )
    return fig4c

# # # Callbacks for page 6
@app.callback(
    [
        Output('narrative', 'children'),
        Output('prediction', 'children'),
        Output('prob_max', 'children')
    ],
    [Input('prediction-button', 'n_clicks')],
)
def generate_prediction(n_clicks):
    if n_clicks is None:  # if the button was never clicked
        # Return some default values
        return "No Narrative yet", "No Prediction yet", "No Probability yet"
    else:  # only generate prediction when button is clicked
        Responce_dict = {}
        # Picking a random number
        random_number=random.randint(0, Narrative_test_set.shape[0])
        # Selecting one Narrative
        narrative=Narrative_test_set.iloc[random_number, 0]
        text_to_list = [narrative]
        Responce_dict["Narratives"] = narrative
        # Predicting the reponce
        prediction = Complement_NB.predict(text_to_list)
        prediction = prediction[0]
        Responce_dict["Prediction"] = prediction
        # Storing the probability of the reponce
        prob = Complement_NB.predict_proba(text_to_list)
        prob_max = max(prob[0])*100
        prob_max = f"{prob_max:.2f}"
        Responce_dict["Probabiliy"] = prob_max
        # return data in format for DataTable
        return narrative, prediction, prob_max

# App layout
app.layout = dbc.Container(
    [
        dcc.Location(id='url', refresh=False),
        dbc.Row(
            [
                dbc.Col([sidebar], style={'padding': 0}, md=2),  # remove padding from sidebar column
                dbc.Col(html.Div(id='page-content', style=heading_style), md=9)  # content will take up the rest 9 columns
            ],
            style={'margin': 0, 'padding': 0}  # remove margins and padding from the row
        )
    ],
    style={'padding': 0},  # remove padding from the container
    fluid=True  # set container width to the full available width
)



if __name__ == '__main__':
    app.run_server(debug=True)
#Launch http://127.0.0.1:8050 on your browser
#calling app.run_server(), start the ngrok server
# url = ngrok.connect(8050).public_url
# print('Running on:', url)

# if __name__ == '__main__':
#     app.run_server(port=8050)
# -


