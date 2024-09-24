library(tidyverse)
library(censusapi)
library(readxl)
library(furrr)
Sys.setenv(CENSUS_KEY="151c9bb8923eb37021ddfe74612864f3e8fed5c2")

USE_CACHED <- T
DIR_PATH <- "" # set your path

options(tigris_use_cache = FALSE)
census_year <- 2022

# Surname and Race
if (!file.exists(paste0(DIR_PATH,"p_race_given_surname.rds")) | !file.exists(paste0(DIR_PATH,"p_surname_given_race.rds")) | !USE_CACHED) {
  
  raw_surnames <-
    getCensus(
      name = "surname",
      vintage = 2010,
      vars = c("NAME", "COUNT", "PROP100K", "PCTAIAN", "PCTAPI", "PCTBLACK", "PCTHISPANIC", "PCTWHITE", "PCT2PRACE"),
      RANK = "1:200000"
    )
  
  surnames <- raw_surnames %>%  
    as_tibble() %>% 
    mutate_at(c("COUNT", "PCTAIAN", "PCTAPI", "PCTBLACK", "PCTHISPANIC", "PCTWHITE", "PCT2PRACE"), function(x) {
      x %>% 
        as.character() %>% 
        gsub("\\(S\\)", "0", .) %>% 
        as.numeric() %>% 
        replace_na(0)
    })
  
  race_surname_counts <- surnames %>%
    filter(!is.na(NAME)) %>% 
    transmute(
      name = NAME,
      COUNT = COUNT,
      aian = PCTAIAN * COUNT / 100,
      api = PCTAPI * COUNT / 100,
      black = PCTBLACK * COUNT / 100,
      hispanic = PCTHISPANIC * COUNT / 100,
      white = PCTWHITE * COUNT / 100,
      other = COUNT - (aian + api + black + hispanic + white)
    )
  
  p_race_given_surname <- race_surname_counts %>% 
    transmute(
      name,
      aian = aian / COUNT,
      api = api / COUNT,
      black = black / COUNT,
      hispanic = hispanic / COUNT,
      white = white / COUNT,
      other = other / COUNT
    )
  
  p_surname_given_race <- race_surname_counts %>% 
    transmute (
      name,
      aian = aian / sum(race_surname_counts$aian),
      api = api / sum(race_surname_counts$api),
      black = black / sum(race_surname_counts$black),
      hispanic = hispanic / sum(race_surname_counts$hispanic),
      white = white / sum(race_surname_counts$white),
      other = other / sum(race_surname_counts$other)
    )
  
  write_rds(p_race_given_surname, paste0(DIR_PATH,"p_race_given_surname.rds"))
  write_rds(p_surname_given_race, paste0(DIR_PATH,"p_surname_given_race.rds"))
  
}

# Firstname and Race
if(!file.exists(paste0(DIR_PATH,"p_race_given_firstname.rds")) | !file.exists(paste0(DIR_PATH,"p_firstname_given_race.rds")) | !USE_CACHED) {
  
  first_names <- read_xlsx(paste0(DIR_PATH,"firstnames.xlsx"), sheet="Data") %>% 
    as_tibble() %>% 
    filter(!startsWith(firstname, "ALL OTHER"))
  
  first_name_counts <- first_names %>% transmute (
    name = firstname,
    COUNT = obs,
    aian = pctaian * COUNT / 100,
    api = pctapi * COUNT / 100,
    black = pctblack * COUNT / 100,
    hispanic = pcthispanic * COUNT / 100,
    white = pctwhite * COUNT / 100,
    other = COUNT - (aian + api + black + hispanic + white)
  )
  
  p_race_given_firstname <- first_name_counts %>% 
    transmute (
      name,
      aian = aian / COUNT,
      api = api / COUNT,
      black = black / COUNT,
      hispanic = hispanic / COUNT,
      white = white / COUNT,
      other = other / COUNT
    )
  
  p_firstname_given_race <- first_name_counts %>% 
    transmute (
      name,
      aian = aian / sum(first_name_counts$aian),
      api = api / sum(first_name_counts$api),
      black = black / sum(first_name_counts$black),
      hispanic = hispanic / sum(first_name_counts$hispanic),
      white = white / sum(first_name_counts$white),
      other = other / sum(first_name_counts$other)
    )
  
  write_rds(p_race_given_firstname, paste0(DIR_PATH,"p_race_given_firstname.rds"))
  write_rds(p_firstname_given_race, paste0(DIR_PATH,"p_firstname_given_race.rds"))
  
}

# Race and Location (CBG)
if (!file.exists(paste0(DIR_PATH,"p_race_given_cbg.rds")) | !file.exists(paste0(DIR_PATH,"p_cbg_given_race.rds")) | 
    !file.exists(paste0(DIR_PATH,"p_race.rds")) | !USE_CACHED) {
  
  census_metadata <- 
    listCensusMetadata(
      name = paste0(census_year,"/acs/acs5"),
      type = "variables"
    )
  
  n_cores <- parallel::detectCores()
  max_size = 1*1024^3
  options(future.globals.maxSize= max_size)
  plan(multisession, workers = n_cores / 2)
  
  # this helper function prepares a list of arguments for us to use
  # before calling the census api given the geographic granularity we specify
  prepare_census_query <- function(region, state, var, year){
    
    census_env <- list(
      name = "acs/acs5",
      vintage = year,
      region = paste0(region, ":*"),
      regionin = paste0("state:", state),
      vars = c(var)
    )
    
    if (region %in% c("us", "state", "zip code tabulation area")){
      census_env$regionin <- NULL
    }
    
    return(census_env)
  }
  
  # this function downloads and processes the population data from census
  # region: geographic granularity (tract, county, etc)
  # state: two-digit state FIPS code to narrow the search
  # var: variable name like group(B03002)
  # group: asian or nhpi
  download_census_data <- function(region, state, var, 
                                   year=census_year){
    
    census_env <- prepare_census_query(region, state, var, year)
    
    # using non-standard evaluation here
    census_raw <- tryCatch(rlang::exec(getCensus, !!!census_env), 
                           error = function(e) NULL)
    # get variable name and clean the format
    if(is.null(census_raw)){
      pop_df <- data.frame()
    } else {
      
      pop_df <- 
        census_raw %>% 
        mutate(GEOID = str_remove(GEO_ID, ".*?US")) %>% 
        select(GEOID, starts_with("B") & ends_with("E")) %>%
        pivot_longer(
          -GEOID,
          names_to = "variable",
          values_to = "estimate"
        ) %>% 
        left_join(
          census_metadata %>% 
            select(name, label), 
          by = c("variable" = "name")
        ) %>% 
        mutate(
          raceeth = case_when(
            label == "Estimate!!Total:!!Not Hispanic or Latino:!!White alone" ~ "white",
            label == "Estimate!!Total:!!Not Hispanic or Latino:!!Black or African American alone" ~ "black",
            label == "Estimate!!Total:!!Not Hispanic or Latino:!!American Indian and Alaska Native alone" ~ "aian",
            label == "Estimate!!Total:!!Not Hispanic or Latino:!!Asian alone" ~ "asian",
            label == "Estimate!!Total:!!Not Hispanic or Latino:!!Native Hawaiian and Other Pacific Islander alone" ~ "nhpi",
            label == "Estimate!!Total:!!Not Hispanic or Latino:!!Some other race alone" ~ "other",
            label == "Estimate!!Total:!!Not Hispanic or Latino:!!Two or more races:" ~ "multi",
            label == "Estimate!!Total:!!Hispanic or Latino:" ~ "hispanic"
          )
        ) %>% 
        select(-label,-variable) %>% 
        filter(!is.na(raceeth))
      
      if(nrow(pop_df) > 0){
        pop_df <- pop_df %>% 
          rename(pop = estimate) %>% 
          pivot_wider(
            names_from = "raceeth",
            values_from = "pop"
          ) %>% 
          mutate(
            api = asian + nhpi,
            other = other + multi
          ) %>% 
          select(-asian,-nhpi,-multi) %>% 
          filter(white + black + aian + other + hispanic + api > 0)
      } else {
        pop_df <- data.frame()
      }
      
    }
    
    return(pop_df)
  }
  
  # some states don't have tract data; exclude from loop
  # "United States Virgin Islands"                
  # "Commonwealth of the Northern Mariana Islands"
  # "Guam"                                        
  # "American Samoa"   
  states <- states() %>% 
    select(NAME,STATEFP) %>% 
    filter(!STATEFP %in% c("78", "66", "60", "69"))
  
  cfips <- read_tsv(paste0(DIR_PATH,'county_list.tsv')) %>% 
    mutate(countystr = paste(substr(FIPS,1,2),"+county:",substr(FIPS,3,5),sep="")) %>% 
    pull(countystr)
  
  geo_result_1 <- future_map(cfips, function(statecounty){
    geo_results <- 
      download_census_data(state = statecounty, region = "block group",
                           var = "group(B03002)",
                           year = 2022)
  })
  
  geo_result_1 <- geo_result_1 %>% 
    bind_rows() %>% 
    list()
  
  geo_result_2 <- future_map(c("tract", "county"), function(geo_level){
    
    geo_results_combined <- 
      map_dfr(states$STATEFP, function(state){
        geo_results <- 
          download_census_data(state = state, region = geo_level,
                               var = "group(B03002)",
                               year = 2022)
      })
  })
  
  geo_result_3 <- future_map(c("state", "zip code tabulation area", "us"), 
    function(geo_level){
      
    geo_results <- 
      download_census_data(state = NULL, region = geo_level,
                           var = "group(B03002)",
                           year = 2022)
    
    if (geo_level == "us"){
      geo_results <- geo_results %>% 
        mutate(GEOID = "1")
    }
    
    return(geo_results)
  })
  
  geo_result_counts <- c(geo_result_1, geo_result_2, geo_result_3)
  
  saveRDS(
    geo_result_counts, 
    "geo_race_counts.rds"
  )
  
  normalize_geo <- function(geo_df, geo_level){
    
    p_race_given_geo <- geo_df %>% 
      mutate(count = rowSums(select(., -c(GEOID)))) %>% 
      filter(count > 0) %>% 
      mutate(
        across(
          -c(GEOID),
          ~(./count)
        )
      ) %>% 
      select(-count)
    
    p_geo_given_race <- geo_df %>% 
      filter(GEOID %in% p_race_given_geo$GEOID) %>% 
      mutate(
        across(
          -GEOID,
          ~(./sum(.))
        )
      )
    
    if (geo_level == "zip code tabulation area"){
      geo_level <- "zcta"
    }
    
    geo_race <- paste("p", geo_level, "given_race", sep = "_")
    race_geo <- paste("p_race_given", geo_level, sep = "_")
    
    geo_prior_list <- vector("list", length = 0)
    
    geo_prior_list[[geo_race]] <- p_geo_given_race
    geo_prior_list[[race_geo]] <- p_race_given_geo
    
    return(geo_prior_list)
    
  }
  
  output_cond_tables <- 
    map2(
      geo_result_counts, 
      c("cbg","tract","county","state","zip code tabulation area", "us"),
      normalize_geo
    ) %>% 
    list_flatten()
  
  saveRDS(
    output_cond_tables, 
    paste0(DIR_PATH,"geo_race_table.rds")
  )
  
}