import pandas as pd

filenames =['people_1.txt','people_2.txt']
with open('people.txt', 'w') as outfile:
     for x in filenames:
         with open(x) as infile:
              for line in infile:
                  outfile.write(line)

people = pd.read_csv('people.txt', delimiter = '\t')
people['FirstName'] = people['FirstName'].str.lower().str.replace(' ','')
people['LastName'] = people['LastName'].str.lower().str.replace(' ','')
people['Email'] = people['Email'].str.replace(' ','')
people['Phone'] = people['Phone'].str.replace('-','')
people['Address'] = people['Address'].str.replace('No.','').str.replace('#','')
people = people.drop_duplicates()
people = people.reset_index()
print(people)
people.to_csv('ParsedPeople.csv')

