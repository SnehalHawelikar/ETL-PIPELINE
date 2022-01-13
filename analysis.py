import pandas as pd 
import numpy as np 
from sklearn.model_selection import train_test_split
from sklearn import linear_model, metrics
import warnings


warnings.filterwarnings("ignore")


df = pd.read_csv('Global_Superstore.csv', encoding = "ISO-8859-1")

print(df.dtypes)

#Data preprocessing 

df['Order_Date'] = pd.to_datetime(df['Order_Date'])
df['Ship_Date'] = pd.to_datetime(df['Ship_Date'])  

df['Profit'] = df['Profit'].astype(str)
df['Profit'] = df['Profit'].str.replace(",","")
df['Profit'] = df['Profit'].str.replace("$","")
df['Profit'] = df['Profit'].astype(float)

df['Sales'] = df['Sales'].astype(str)
df['Sales'] = df['Sales'].str.replace(",","")
df['Sales'] = df['Sales'].str.replace("$","")
df['Sales'] = df['Sales'].astype(float)

#Business Question : 
#Build a model to predict the profits for product category as 'Technology'

a = ['Technology']
df.drop(df[~df['Category'].isin(a)].index, inplace = True)

df.drop(['Order_Date', 'Ship_Date','Ship_Mode','Customer_Name','Segment','City', 'State', 'Region','Product_ID','Product_Name','Category', 'Sub_Category','Order_Priority'], axis=1, inplace=True)

df = pd.get_dummies(df, columns=['Market'], prefix=['Market'])
df = pd.get_dummies(df, columns=['Order_ID'], prefix=['Order_ID'])
df = pd.get_dummies(df, columns=['Customer_ID'], prefix=['Customer_ID'])
df = pd.get_dummies(df, columns=['Country'], prefix=['Country'])

X = df.loc[:, df.columns != 'Profit']
y = df.loc[:, df.columns == 'Profit']

X_train, X_test, y_train, y_test = train_test_split(X,y,test_size = 0.3, random_state = 1)

# create linear regression object
reg = linear_model.LinearRegression()
 
# train the model using the training sets
reg.fit(X_train, y_train)
 
# regression coefficients
print('Coefficients: ', reg.coef_)
 
# variance score: 1 means perfect prediction
print('Variance score: {}'.format(reg.score(X_test, y_test)))



