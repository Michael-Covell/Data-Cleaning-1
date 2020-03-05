#First, I created 2 DataFrames from the 2 datasets I began with.
import pandas as pd #version 0.23.4
df1 = pd.read_csv('1.txt', sep='\t')
df2 = pd.read_csv('2.txt', sep='\t')
print(df1.head())
print(df2.head())

#df1 is the larger dataset and contains initial inspection outcomes, broker names ("BROKER ") and client names ("FULLNAME")
#df2 contains client names ("Client") and follow-up inspection outcomes ("Result of Inspection")
#Because I wanted to merge the two DataFrames on client names, I ran the following code to eliminate irregularities due to whitespace, capitalization, and commas

df1['FULLNAME'] = df1['FULLNAME'].str.strip().str.lower().str.replace(',', '')
df2['Client'] = df2['Client'].str.strip().str.lower().str.replace(',', '')

#From df1, I dropped all rows in which broker names and/or client names were missing because the data in these rows wouldn't be used in the final frequency analysis
#From df2, I dropped all rows in which client names were missing because these rows cannot be merged with df1

df1 = df1.dropna(subset=['BROKER ', 'FULLNAME'])
df2 = df2.dropna(subset=['Client'])

#From df1 and df2, I dropped all rows in which there was no pass or fail result. This eliminates null values and outcomes of "NO ACCESS" or "must fix"
#To account for the possibility that pass/fail results may vary in whitespace or capitalization, I cleaned this data first using strip() and lower()
df1['ARC OUTCOME '] = df1['ARC OUTCOME '].str.strip().str.lower()
df1_bools = df1['ARC OUTCOME '].isin(['pass','fail'])
df1 = df1[df1_bools]
df2['Result of Inspection'] = df2['Result of Inspection'].str.strip().str.lower()
df2_bools = df2['Result of Inspection'].isin(['pass', 'fail'])
df2 = df2[df2_bools]

#Next, I merged the DataFrames and printed its shape as a pretest to the reformating strategy I apply below (I expect the number of rows to be fewer after the reformatting)
df3 = df1.merge(df2, left_on='FULLNAME',right_on='Client',how='outer')
print('Before reformatting client names, the merged DataFrame contains', str(df3.shape[0]),'rows.')

#The following code extracts two new DataFrames that contain mismatching names
#These DataFrames will be used to build a reformatting script
#df4 contains rows that have Broker names but no follow-up inspection results (i.e., data originally from df1 containing names that failed to match names in df2)
#df5 contains rows that have follow-up inspection results but no Broker names (i.e., data originally from df2 containing names that failed to match names in df1)
df4 = df3[df3['BROKER '].notnull()]
df4 = df4[df4['Result of Inspection'].isnull()]
df5 = df3[df3['BROKER '].isnull()]
df5 = df5[df5['Result of Inspection'].notnull()]

#*******************************__REFORMATTING_SCRIPT__********************************************

#The following includes a conditional (if) statement nested within a for loop, which is nest within another for loop
#It builds a dictorary of irregularly formatted names in which keys are the names originally from df1 and the values are the names originally from df2

d = dict()
for value1 in df4['FULLNAME']:
  words1 = str(value1).split() #str() is used because bug-testing revealed that some values were floats
  for value2 in df5['Client']:
    words2 = value2.split()
    if words1[0] == words2[-1] and words1[-1] == words2[0]:
      d[value1] = value2

#The following reports the number of irregularly formatted names and presents the names in a dictionary
print(len(d),'irregularly formatted names were found.'+'\n'+'Here they are:'+'\n',d)

#The next bit of code traverses the dictionary and the original set of names df2 (the follow-up inspection results)
#Names in df2 that match the values in the dictionary are replaced with their corresponding keys
#Keys (names from df1) are assumed to be correctly formatted
for key in d:
  for name in df2['Client']:
    if name == d[key]:
      df2.replace(to_replace=name,value=key,inplace=True)

#Finally, I perform a pretest-posttest of the strategy to examine the reduction in rows after the reformatting strategy is executed
print('Before reformatting names, the merged DataFrame contained', str(df3.shape[0]), 'rows')
df3 = df1.merge(df2, left_on='FULLNAME',right_on='Client',how='outer')
print('After reformatting names, the merged DataFrame contained', str(df3.shape[0]),'rows')

#**********************__FREQUENCIES___*****************************************************************

#The next section concatenates two groupby objects to present frequencies within rows for each broker

Freq1 = df3.groupby(['BROKER ','ARC OUTCOME ']).size().unstack(fill_value=0)
Freq2 = df3.groupby(['BROKER ','Result of Inspection']).size().unstack(fill_value=0)
Freq3 = pd.concat([Freq1,Freq2], axis=1,sort=False)

#This relabels and reorders columns in order to make the final table more intuitive
#pass1 and pass2 correspond to the initial inspections
#pass2 and fail2 correspond to the follow-up inspections
Freq3.columns = ['fail1',	"pass1", 'fail2', 'pass2']
Freq3 = Freq3[["pass1",'fail1', 'pass2','fail2']]

Freq3.to_csv('output.txt', sep='\t')
print('done')
