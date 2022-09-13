# Bayesian Improved First Name Surname Geocoding (BIFSG)
Documentation of BIFSG method, as used for imputing race/ethnicity in the American Families Cohort (AFC) dataset hosted by Stanford Population Health Sciences.

## Step 1: Produce conditional probability tables using `create_tables.R`
All data can be retrieved from the U.S. Census Bureau using public API calls, with the exception of the dataset on first names by race which comes from [Tzioumis, Konstantinos (2018)](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/TYJKEZ). A copy has been provided in this repo. The list of counties in `county_list.tsv` can be generated using the `counties()` function in the `tigris` package, looped over all states, but is provided in this repo for convenience. Otherwise, the R script demonstrates how to use the `censusapi` package to retrieve all other needed datasets, and generates all seven required tables. Completed tables are provided in this repo. The R script checks for the existence of the tables in the directory (`DIR_PATH`) of your choice and either generates or loads them. You can set `USE_CACHED` to FALSE if you'd like to regenerate the tables.

## Step 2: Prepare your data
Your data requires a field for `firstname`, a field for `surname`, and a field for `CBG` (census block group). Individual records can be missing one or more of these fields. If you have address information but not CBG, then the most straightforward method is to geocode the addresses into latitude longitude coordinates and then spatial join the coordinates to CBG polygons from TIGER. Provide the full 12-character GEOID as a character type.

## Step 3: Calculate posterior probabilities using `create_BISG_model.R`
After loading the conditional probability tables into your environment, they are converted into data.table format. Then, `predict_race()` is the key function that takes first name, surname, and CBG from your data and outputs the results, which are posterior probabilities that the individual is each of the six race/ethnicity options: Hispanic/Latino, White, Black or African American, Asian American or Pacific Islander, American Indian or Alaska Native, or Other Race. The probabilities add up to 1. The script can handle any combination of missingness in first name, surname, or CBG. If you lack first name, then the result is BISG. If you only have CBG, then your posterior probabilities will be the race/ethnicity distribution of your CBG. If you have nothing at all, then your posterior probabilities will be the race/ethnicity distribution of the whole U.S. Intermediate geometry cases (e.g., tract, county, ZIP Code, state) are not provided but can be added with the same principles. If you are dealing with a large dataset, then `predict_parallel()` can be used to speed up the process.

This code and data were prepared by Cameron Raymond.
If you have questions, reach out to Derek Ouyang at douyang1@law.stanford.edu. 
