import os
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from SALib.analyze import sobol
from SALib.sample import saltelli

warnings.filterwarnings("ignore")


# Load and preprocess data
def load_and_preprocess_data(file_name, country_code=643):
	"""Load and preprocess demographic data for a specific country."""
	path = os.path.join(os.getcwd(), file_name)
	sheets = pd.ExcelFile(path).sheet_names
	
	# Load all relevant sheets
	both_1950_data = pd.read_excel(path, header=5, sheet_name=sheets[0])
	both_2010_data = pd.read_excel(path, header=5, sheet_name=sheets[1])
	male_1950_data = pd.read_excel(path, header=5, sheet_name=sheets[3])
	male_2010_data = pd.read_excel(path, header=5, sheet_name=sheets[4])
	female_1950_data = pd.read_excel(path, header=5, sheet_name=sheets[6])
	female_2010_data = pd.read_excel(path, header=5, sheet_name=sheets[7])
	
	def filter_dataset(dataset):
		"""Filter dataset for specific country and clean columns."""
		# Convert numeric columns
		for col in dataset.columns:
			if col not in ['Index', 'Variant', 'Major area, region, country or area*',
			               'Notes', 'Country code', 'Reference date (as of 1 July)']:
				dataset[col] = pd.to_numeric(dataset[col], errors='coerce')
		
		# Filter for country and clean up
		return (dataset[dataset['Country code'] == country_code]
		        .drop(columns=['Index', 'Variant', 'Major area, region, country or area*',
		                       'Notes', 'Country code'])
		        .rename(columns={'Reference date (as of 1 July)': 'Year'})
		        .set_index('Year'))
	
	return (
		filter_dataset(both_1950_data),
		filter_dataset(both_2010_data),
		filter_dataset(male_1950_data),
		filter_dataset(male_2010_data),
		filter_dataset(female_1950_data),
		filter_dataset(female_2010_data)
	)


# Load data
(both_1950, both_2010, male_1950, male_2010, female_1950, female_2010) = load_and_preprocess_data('age_data_2005.xls')

# Define age categories
age_categories = both_1950.columns
available_years = both_1950.index.tolist()
print(f"Available years in dataset: {available_years}")


# Calculate fertility rate for women aged 20-39
def calculate_fertility_rate(year):
	"""Calculate fertility rate for women aged 20-39."""
	if year not in female_1950.index:
		return np.nan
	
	women_20_39 = female_1950.loc[year, ['20 - 24', '25 - 29', '30 - 34', '35 - 39']].sum()
	if women_20_39 > 0:
		return both_1950.loc[year, '0 - 4'] / women_20_39
	return np.nan


# Calculate boy/girl ratio for newborns
def calculate_gender_ratio(year):
	"""Calculate the ratio of boys to girls among newborns."""
	if year not in female_1950.index:
		return np.nan
	
	girls_newborn = female_1950.loc[year, '0 - 4']
	boys_newborn = male_1950.loc[year, '0 - 4']
	if girls_newborn > 0:
		return boys_newborn / girls_newborn
	return 1.05  # Default biological ratio


# Calculate survival rates for each age group
def calculate_survival_rates():
	"""Calculate survival rates for each age group and gender."""
	male_survival_rates = {}
	female_survival_rates = {}
	
	# Use only available years that have data for consecutive time periods
	valid_years = [year for year in available_years if year + 5 in available_years]
	
	for i, category in enumerate(age_categories[:-1]):  # Skip the last category (no next group)
		next_category = age_categories[i + 1]
		
		# Calculate for males
		male_rates = []
		for year in valid_years:
			next_year = year + 5
			if (year in male_1950.index and next_year in male_1950.index and
				male_1950.loc[year, category] > 0 and male_1950.loc[next_year, next_category] > 0):
				rate = male_1950.loc[next_year, next_category] / male_1950.loc[year, category]
				male_rates.append(rate)
		
		if male_rates:
			male_survival_rates[category] = [np.min(male_rates), np.max(male_rates)]
		else:
			male_survival_rates[category] = [0.8, 1.2]  # Default range
		
		# Calculate for females
		female_rates = []
		for year in valid_years:
			next_year = year + 5
			if (year in female_1950.index and next_year in female_1950.index and
				female_1950.loc[year, category] > 0 and female_1950.loc[next_year, next_category] > 0):
				rate = female_1950.loc[next_year, next_category] / female_1950.loc[year, category]
				female_rates.append(rate)
		
		if female_rates:
			female_survival_rates[category] = [np.min(female_rates), np.max(female_rates)]
		else:
			female_survival_rates[category] = [0.8, 1.2]  # Default range
	
	return male_survival_rates, female_survival_rates


# Calculate all necessary parameters
male_survival, female_survival = calculate_survival_rates()

fertility_rates = [calculate_fertility_rate(year) for year in available_years]
gender_ratios = [calculate_gender_ratio(year) for year in available_years]

# Filter out NaN values
fertility_rates = [rate for rate in fertility_rates if not np.isnan(rate)]
gender_ratios = [ratio for ratio in gender_ratios if not np.isnan(ratio)]

print(f"Fertility rates range: {np.min(fertility_rates):.4f} - {np.max(fertility_rates):.4f}")
print(f"Gender ratios range: {np.min(gender_ratios):.4f} - {np.max(gender_ratios):.4f}")

# Prepare parameter bounds for sensitivity analysis
parameter_bounds = []
for category in age_categories[:-1]:  # Survival rates for each age group
	# Use average of male and female survival rates
	avg_min = (male_survival[category][0] + female_survival[category][0]) / 2
	avg_max = (male_survival[category][1] + female_survival[category][1]) / 2
	parameter_bounds.append([avg_min, avg_max])

# Add fertility rate bounds
parameter_bounds.append([np.min(fertility_rates), np.max(fertility_rates)])

# Add gender ratio bounds
parameter_bounds.append([np.min(gender_ratios), np.max(gender_ratios)])

# Define problem for sensitivity analysis
problem = {
	'num_vars': len(parameter_bounds),
	'names': [f'survival_rate_{i}' for i in range(len(age_categories) - 1)] +
	         ['fertility_rate', 'gender_ratio'],
	'bounds': parameter_bounds
}


# Population projection model
def project_population(parameters, start_year, projection_years=100, step=5):
	"""Project population for given parameters."""
	# Extract parameters
	survival_rates = parameters[:-2]
	fertility_rate = parameters[-2]
	boy_probability = parameters[-1] / (1 + parameters[-1])  # Convert ratio to probability
	
	# Initialize population arrays
	years = [start_year + i * step for i in range(projection_years // step + 1)]
	male_population = np.zeros((len(years), len(age_categories)))
	female_population = np.zeros((len(years), len(age_categories)))
	
	# Set initial population (from available data)
	if start_year in male_1950.index:
		male_population[0] = male_1950.loc[start_year].values
		female_population[0] = female_1950.loc[start_year].values
	else:
		# Use the latest available data if start_year not available
		latest_year = max([year for year in available_years if year <= start_year])
		male_population[0] = male_1950.loc[latest_year].values
		female_population[0] = female_1950.loc[latest_year].values
	
	# Project population
	for i in range(1, len(years)):
		# Calculate newborns
		women_20_39 = female_population[i - 1, 4:8].sum()  # Indices for 20-39 age groups
		newborns = fertility_rate * women_20_39
		
		# Distribute newborns by gender
		male_newborns = newborns * boy_probability
		female_newborns = newborns * (1 - boy_probability)
		
		# Apply survival rates to all age groups
		for j in range(len(age_categories) - 1):
			male_population[i, j + 1] = male_population[i - 1, j] * survival_rates[j]
			female_population[i, j + 1] = female_population[i - 1, j] * survival_rates[j]
		
		# Add newborns
		male_population[i, 0] = male_newborns
		female_population[i, 0] = female_newborns
	
	total_population = male_population.sum(axis=1) + female_population.sum(axis=1)
	
	return years, total_population, male_population, female_population


# Run sensitivity analysis
def evaluate_parameters(param_values):
	"""Evaluate parameter sets for sensitivity analysis."""
	results = []
	years_list = []
	
	for params in param_values:
		years, total_pop, _, _ = project_population(params, 2005, 100, 5)
		results.extend(total_pop)
		years_list.extend(years)
	
	return np.array(results), np.array(years_list)


# Generate parameter samples
print('Generating parameter samples...')
param_values = saltelli.sample(problem, 50)  # Reduced sample size for faster execution
print('Parameter samples generated.')

# Run evaluation
print('Running population projections...')
Y, X = evaluate_parameters(param_values)
print('Projections completed.')

# Perform sensitivity analysis
print('Performing sensitivity analysis...')
Si = sobol.analyze(problem, Y, print_to_console=False)
print('Sensitivity analysis completed.')

# Print sensitivity indices
print("\nFirst-order sensitivity indices:")
for i, name in enumerate(problem['names']):
	print(f"{name}: {Si['S1'][i]:.4f}")

# Plot results
plt.figure(figsize=(14, 8))

# Plot projected population
plt.subplot(2, 2, 1)
plt.scatter(X, Y, alpha=0.1, label='Projections')
# Plot actual data for comparison
actual_years = [year for year in both_1950.index if year >= 1950]
actual_population = [both_1950.loc[year].sum() for year in actual_years]
plt.plot(actual_years, actual_population, 'r-', label='Actual Data', linewidth=2)
plt.ylabel('Total Population')
plt.xlabel('Year')
plt.title('Population Projection vs Actual Data')
plt.legend()
plt.grid(True, alpha=0.3)

# Plot sensitivity indices
plt.subplot(2, 2, 2)
sensitivity_indices = Si['S1'][:len(age_categories) - 1]  # Survival rates only
plt.bar(range(len(sensitivity_indices)), sensitivity_indices)
plt.xlabel('Age Group')
plt.ylabel('Sensitivity Index')
plt.title('Sensitivity of Survival Rates by Age Group')
plt.xticks(range(len(sensitivity_indices)), [f'{i * 5}-{i * 5 + 4}' for i in range(len(sensitivity_indices))],
           rotation=45)

# Plot fertility and gender ratio trends
plt.subplot(2, 2, 3)
fertility_years = [year for year in available_years if not np.isnan(calculate_fertility_rate(year))]
fertility_values = [calculate_fertility_rate(year) for year in fertility_years]
plt.plot(fertility_years, fertility_values, 'o-', label='Fertility Rate')
plt.xlabel('Year')
plt.ylabel('Fertility Rate')
plt.title('Fertility Rate Trend')
plt.grid(True, alpha=0.3)

plt.subplot(2, 2, 4)
gender_years = [year for year in available_years if not np.isnan(calculate_gender_ratio(year))]
gender_values = [calculate_gender_ratio(year) for year in gender_years]
plt.plot(gender_years, gender_values, 'o-', label='Boy/Girl Ratio', color='orange')
plt.xlabel('Year')
plt.ylabel('Boy/Girl Ratio')
plt.title('Gender Ratio at Birth Trend')
plt.grid(True, alpha=0.3)

plt.tight_layout()
plt.show()

# Run a single projection for visualization
print('\nRunning detailed population projection...')
median_params = np.median(param_values, axis=0)
proj_years, total_pop, male_pop, female_pop = project_population(median_params, 2005, 100, 5)

plt.figure(figsize=(12, 8))
plt.plot(proj_years, total_pop, 'b-', label='Total Population')
plt.plot(proj_years, male_pop.sum(axis=1), 'g--', label='Male Population')
plt.plot(proj_years, female_pop.sum(axis=1), 'r--', label='Female Population')
plt.xlabel('Year')
plt.ylabel('Population')
plt.title('100-Year Population Projection (2005-2105)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()

# Plot age distribution changes
plt.figure(figsize=(12, 8))
plt.subplot(2, 2, 1)
plt.bar(age_categories, male_pop[0] / male_pop[0].sum(), alpha=0.7, label='2005')
plt.bar(age_categories, male_pop[-1] / male_pop[-1].sum(), alpha=0.7, label='2105')
plt.xlabel('Age Group')
plt.ylabel('Proportion')
plt.title('Male Age Distribution')
plt.xticks(rotation=45)
plt.legend()

plt.subplot(2, 2, 2)
plt.bar(age_categories, female_pop[0] / female_pop[0].sum(), alpha=0.7, label='2005')
plt.bar(age_categories, female_pop[-1] / female_pop[-1].sum(), alpha=0.7, label='2105')
plt.xlabel('Age Group')
plt.ylabel('Proportion')
plt.title('Female Age Distribution')
plt.xticks(rotation=45)
plt.legend()

plt.subplot(2, 2, 3)
age_pyramid = np.vstack((male_pop[-1] / male_pop[-1].sum(),
                         -(female_pop[-1] / female_pop[-1].sum())))
plt.barh(age_categories, age_pyramid[0], alpha=0.7, label='Male')
plt.barh(age_categories, age_pyramid[1], alpha=0.7, label='Female')
plt.xlabel('Proportion')
plt.ylabel('Age Group')
plt.title('2105 Population Pyramid')
plt.legend()

plt.tight_layout()
plt.show()