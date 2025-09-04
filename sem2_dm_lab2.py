import os
import warnings

import numpy as np
import pandas as pd
from SALib.analyze import sobol
from SALib.sample import saltelli
import matplotlib
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

name = 'age_data_2005.xls'
path = os.getcwd() + "\\" + name

sheets = pd.ExcelFile(path).sheet_names

header = 5

both_1950_data = pd.read_excel(path, header=header, sheet_name=sheets[0])
both_2010_data = pd.read_excel(path, header=header, sheet_name=sheets[1])
m_1950_data = pd.read_excel(path, header=header, sheet_name=sheets[3])
m_2010_data = pd.read_excel(path, header=header, sheet_name=sheets[4])
f_1950_data = pd.read_excel(path, header=header, sheet_name=sheets[6])
f_2010_data = pd.read_excel(path, header=header, sheet_name=sheets[7])


def filtered_dataset(dataset, country_code=643):
	# Convert all columns to numeric, coercing errors to NaN
	for col in dataset.columns:
		if col not in ['Index', 'Variant', 'Major area, region, country or area*', 'Notes', 'Country code',
		               'Reference date (as of 1 July)']:
			dataset[col] = pd.to_numeric(dataset[col], errors='coerce')
	
	return dataset[dataset['Country code'] == country_code].drop(
		columns=['Index', 'Variant', 'Major area, region, country or area*', 'Notes', 'Country code']
	).rename(columns={'Reference date (as of 1 July)': 'Year'}).set_index('Year')


def get_farcility_rate(year=2005):
	return both_1950.loc[year, '0 - 4'] / f_1950.loc[
		year, ['15 - 19', '20 - 24', '25 - 29', '30 - 34', '35 - 39']].sum()


def get_girl_prob(year):
	return pd.Series(f_1950.loc[year, category[0]] / both_1950.loc[year, category[0]])


both_1950 = filtered_dataset(both_1950_data)
both_2010 = filtered_dataset(both_2010_data)
m_1950 = filtered_dataset(m_1950_data)
m_2010 = filtered_dataset(m_2010_data)
f_1950 = filtered_dataset(f_1950_data)
f_2010 = filtered_dataset(f_2010_data)

category = both_1950.columns

years = []
for i in range(21):
	years.append(1950 + i * 5)


def model(sr_dict, year):
	survival_rate = sr_dict[:-2]
	fertility = sr_dict[-2]
	boy_probability = sr_dict[-1]
	prev_year = years[np.argwhere(np.asarray(years) == year)[0][0] - 1]
	
	f_0 = f_1950.loc[year, category[0]] = fertility * f_1950.loc[
		prev_year, ['15 - 19', '20 - 24', '25 - 29', '30 - 34', '35 - 39']].sum() * (1 - boy_probability)
	f_1 = f_1950.loc[year, category[1:]] = survival_rate * f_1950.loc[prev_year].shift(periods=1)
	m_0 = m_1950.loc[year, category[0]] = fertility * m_1950.loc[
		prev_year, ['15 - 19', '20 - 24', '25 - 29', '30 - 34', '35 - 39']].sum() * boy_probability
	m_1 = m_1950.loc[year, category[1:]] = survival_rate * m_1950.loc[prev_year].shift(periods=1)
	both_1950.loc[year, category[0]] = f_0 + m_0
	both_1950.loc[year, category[1:]] = f_1 + m_1
	return both_1950.loc[year].sum()


fartility_list = []
girl_prob_list = []

for i in range(len(years[:12])):
	fartility_list.append(get_farcility_rate(years[i]))
	girl_prob_list.append(get_girl_prob(years[i]))

sr_dict = []

for i in range(len(category)):
	temp = []
	for j in range(len(years[:12]) - 1):
		# Convert to float instead of int to handle decimal values
		value1 = both_1950.loc[years[j], category[i]]
		value2 = both_1950.loc[years[j + 1], category[i]]
		
		# Skip if either value is NaN or zero
		if pd.isna(value1) or pd.isna(value2) or value2 == 0:
			continue
		
		ratio = float(value1) / float(value2)
		temp.append(ratio)
	
	if temp:  # Only add bounds if we have valid values
		sr_dict.append([np.min(temp), np.max(temp)])
	else:
		# Add default bounds if no valid values found
		sr_dict.append([0.8, 1.2])

sr_dict.append([np.min(fartility_list), np.max(fartility_list)])
sr_dict.append([np.min(girl_prob_list), np.max(girl_prob_list)])

problem = {
	'num_vars': len(sr_dict),
	'names': [f'param_{i}' for i in range(len(sr_dict))],  # Fixed parameter names
	'bounds': sr_dict
}


def evaluate(param_values):
	Y = []
	X = []
	for params in param_values:
		sr_dict = params
		
		for year in years[11:]:
			res = model(sr_dict, year)
			Y.append(res)
			X.append(year)
	return np.array(Y), np.array(X)


# Generate samples
print('Generate sample')
param_values = saltelli.sample(problem, 100)
print('Sample has been generated')
# Run model (example)
print('Start evaluate')
Y, X = evaluate(param_values)
print('Evaluate has been finished')
# Perform analysis
print('Start analysis')
Si = sobol.analyze(problem, Y, print_to_console=False)
print('Analysis has been finished')
# Print the first-order sensitivity indices
print("__________________")
print(Si['S1'])

plt.figure(figsize=(12, 7))
plt.scatter(X, Y, label='me')
plt.scatter(both_2010.index, both_2010.sum(axis=1), label='UN', c='Red', marker='^', linewidths=5)
plt.ylabel('Total population')
plt.xlabel('year')
plt.legend()
plt.show()