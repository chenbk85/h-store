import random as random

no_of_rows = 1000000;

f = open('DataInput.csv','w');

col1 = range(0,no_of_rows);
col2 = col1;
col3 = range(0,no_of_rows);
random.shuffle(col3);

for i in range(0,no_of_rows):
  row = str(col1[i]) + ',' + str(col2[i]) + ',' + str(col3[i]) + '\n';
  f.write(row);

f.close();
