# Bayesian Improved First Name Surname Geocoding (BIFSG)
Documentation of BIFSG method, as used for imputing race/ethnicity in the American Families Cohort (AFC) dataset hosted by Stanford Population Health Sciences.

## Step 1: Produce conditional probability tables using `create_tables.R`
All data can be retrieved from the U.S. Census Bureau using public API calls, with the exception of the dataset on first names by race which comes from (Tzioumis, Konstantinos 2018)[https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/TYJKEZ]. A copy has been provided under `/data`. Otherwise, the R script demonstrates how to use the `censusapi` package to retrieve all other needed datasets, and generates all seven required tables. Completed tables are provided in `/data`. The R script checks for the existence of the tables in directory (`DIR_PATH`) of your choice and either generates or loads them. You can set `USE_CACHED` to FALSE if you'd like to regenerate the tables.

## Step 2: Calculate posterior probabilities
