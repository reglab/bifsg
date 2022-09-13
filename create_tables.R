library(tidyverse)
library(censusapi)
library(readxl)
Sys.setenv(CENSUS_KEY="151c9bb8923eb37021ddfe74612864f3e8fed5c2")
LAPLACE <- 0.001
USE_CACHED <- Sys.getenv("USE_CACHED") == "TRUE"
DIR_PATH <- Sys.getenv("DIR_PATH")

print(paste0(DIR_PATH,"p_race_given_surname.rds"))

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
      NAME = NAME,
      COUNT = COUNT,
      AIAN = PCTAIAN * COUNT / 100 + LAPLACE,
      API = PCTAPI * COUNT / 100 + LAPLACE,
      BLACK = PCTBLACK * COUNT / 100 + LAPLACE,
      HISP = PCTHISPANIC * COUNT / 100 + LAPLACE,
      WHITE = PCTWHITE * COUNT / 100 + LAPLACE,
      OTHER = COUNT + 6 * LAPLACE - (AIAN + API + BLACK + HISP + WHITE)
    ) %>% 
    mutate (
      COUNT = COUNT + 6 * LAPLACE
    )
  race_surname_counts %>% head() %>% print()
  
  p_race_given_surname <- race_surname_counts %>% 
    transmute(
      NAME = NAME,
      AIAN = AIAN / COUNT,
      API = API / COUNT,
      BLACK = BLACK / COUNT,
      HISP = HISP / COUNT,
      WHITE = WHITE / COUNT,
      OTHER = OTHER / COUNT
    )
  
  p_surname_given_race <- race_surname_counts %>% 
    transmute (
      NAME = NAME,
      AIAN = AIAN / sum(race_surname_counts$AIAN),
      API = API / sum(race_surname_counts$API),
      BLACK = BLACK / sum(race_surname_counts$BLACK),
      HISP = HISP / sum(race_surname_counts$HISP),
      WHITE = WHITE / sum(race_surname_counts$WHITE),
      OTHER = OTHER / sum(race_surname_counts$OTHER)
    )
  
  write_rds(p_race_given_surname, paste0(DIR_PATH,"p_race_given_surname.rds"))
  write_rds(p_surname_given_race, paste0(DIR_PATH,"p_surname_given_race.rds"))
  
} else {
  
  p_race_given_surname <- read_rds(paste0(DIR_PATH,"p_race_given_surname.rds"))
  p_surname_given_race <- read_rds(paste0(DIR_PATH,"p_surname_given_race.rds"))
  
}

cfips <- read_tsv(paste0(DIR_PATH,'county_list.tsv')) %>% 
  mutate(countystr  = paste("state:",substr(FIPS,1,2),"+county:",substr(FIPS,3,5),sep="")) %>% 
  pull(countystr)

# Race and Location (CBG)
if (!file.exists(paste0(DIR_PATH,"p_race_given_cbg.rds")) | !file.exists(paste0(DIR_PATH,"p_cbg_given_race.rds")) | 
    !file.exists(paste0(DIR_PATH,"p_race.rds")) | !USE_CACHED) {
  
  raw_scc_race_cbg <- NULL
  for (countystr in cfips) {
    
    # show(countystr)
    temp <- tryCatch({
      getCensus(
        name = "acs/acs5",
        vintage = 2018,
        region = "block group:*",
        regionin = countystr,
        vars = "group(B03002)"
      ) %>% return()
    },
    error = function(e) {
      show("BAD CALL")
      show(e)
    })
    
    raw_scc_race_cbg <- rbind(raw_scc_race_cbg,temp)
    
  }
  
  acs_vars_2018_5yr <- listCensusMetadata(
    name = "2018/acs/acs5",
    type = "variables"
  )
  
  scc_race_cbg <- raw_scc_race_cbg %>%   
    mutate(CBG = paste0(state,county,tract,block_group)) %>%
    select(!c(GEO_ID,state,county,tract,block_group,NAME) & 
             !ends_with(c("EA","MA","M"))) %>%
    pivot_longer(
      ends_with("E"),
      names_to = "variable",
      values_to = "estimate"
    ) %>%
    left_join(acs_vars_2018_5yr %>% select(name, label), by = c("variable" = "name")) %>%
    select(-variable) %>%
    separate(
      label,
      into = c(NA,NA,"his.lat","category","subcategory"),
      sep = "!!"
    ) %>%
    mutate(
      race = case_when(
        is.na(his.lat) ~ "TOTAL",
        his.lat == "Hispanic or Latino" & is.na(category) ~ "HISP",
        his.lat == "Hispanic or Latino" & !is.na(category) ~ "remove",
        his.lat == "Not Hispanic or Latino" & 
          category %in% c("Some other race alone", "Two or more races") ~ "remove",
        his.lat == "Not Hispanic or Latino" & !is.na(category) ~ category,
        his.lat == "Not Hispanic or Latino" & is.na(category) ~ "remove",
        TRUE ~ subcategory
      )
    ) %>%
    filter(race != "remove") %>%
    select(CBG, race, estimate) %>% 
    pivot_wider(names_from=race, values_from=estimate) %>% 
    mutate(
      OTHER = TOTAL - rowSums(.[3:8]),
      API = `Asian alone` + `Native Hawaiian and Other Pacific Islander alone`
    ) %>%
    select(
      CBG, 
      TOTAL,
      AIAN = `American Indian and Alaska Native alone`,
      API,
      BLACK = `Black or African American alone`,
      HISP,
      WHITE = `White alone`,
      OTHER
    ) %>% 
    mutate(
      AIAN = AIAN + LAPLACE,
      API = API + LAPLACE,
      BLACK = BLACK + LAPLACE,
      HISP = HISP + LAPLACE,
      WHITE = WHITE + LAPLACE,
      OTHER = OTHER + LAPLACE,
      TOTAL = TOTAL + 6 * LAPLACE
    )
  
  p_race_given_cbg <- scc_race_cbg %>%
    transmute (
      CBG = CBG,
      AIAN = AIAN / TOTAL,
      API = API / TOTAL,
      BLACK = BLACK / TOTAL,
      HISP = HISP / TOTAL,
      WHITE = WHITE / TOTAL,
      OTHER = OTHER / TOTAL
    ) %>% 
    arrange(CBG)
  
  p_cbg <- scc_race_cbg %>% 
    select(CBG, TOTAL) %>% 
    transmute(
      CBG = CBG,
      p = TOTAL / sum(scc_race_cbg$TOTAL)
    ) %>%
    arrange(CBG)
  
  p_race <- 
    sweep(data.matrix(p_race_given_cbg %>% select(-CBG)), 1, p_cbg$p, FUN="*") %>% 
    colSums() %>% 
    as_tibble_row()
  
  p_cbg_given_race <-
    sweep(data.matrix(p_race_given_cbg %>% select(-CBG)), 1, p_cbg$p, FUN="*") %>% 
    sweep(., 2, unlist(p_race), FUN="/") %>% 
    as_tibble() %>% 
    bind_cols(p_cbg %>% select(CBG), .)
  
  write_rds(p_race_given_cbg, paste0(DIR_PATH,"p_race_given_cbg.rds"))
  write_rds(p_cbg_given_race, paste0(DIR_PATH,"p_cbg_given_race.rds"))
  write_rds(p_race, paste0(DIR_PATH,"p_race.rds"))
  
} else {
  
  p_race_given_cbg <- read_rds(paste0(DIR_PATH,"p_race_given_cbg.rds"))
  p_cbg_given_race <- read_rds(paste0(DIR_PATH,"p_cbg_given_race.rds"))
  p_race <- read_rds(paste0(DIR_PATH,"p_race.rds"))
  
}

if(!file.exists(paste0(DIR_PATH,"p_race_given_firstname.rds")) | !file.exists(paste0(DIR_PATH,"p_firstname_given_race.rds")) | !USE_CACHED) {
  
  first_names <- read_xlsx(paste0(DIR_PATH,"firstnames.xlsx"), sheet="Data") %>% 
    as_tibble() %>% 
    filter(!startsWith(firstname, "ALL OTHER"))
  
  first_name_counts <- first_names %>% transmute (
    NAME = firstname,
    COUNT = obs,
    AIAN = pctaian * COUNT / 100 + LAPLACE,
    API = pctapi * COUNT / 100 + LAPLACE,
    BLACK = pctblack * COUNT / 100 + LAPLACE,
    HISP = pcthispanic * COUNT / 100 + LAPLACE,
    WHITE = pctwhite * COUNT / 100 + LAPLACE,
    OTHER = COUNT + 6 * LAPLACE - (AIAN + API + BLACK + HISP + WHITE)
  ) %>% mutate (
    COUNT = COUNT + 6 * LAPLACE
  )
  
  p_race_given_firstname <- first_name_counts %>% 
    transmute (
      NAME = NAME,
      AIAN = AIAN / COUNT,
      API = API / COUNT,
      BLACK = BLACK / COUNT,
      HISP = HISP / COUNT,
      WHITE = WHITE / COUNT,
      OTHER = OTHER / COUNT
    )
  
  p_firstname_given_race <- first_name_counts %>% 
    transmute (
      NAME = NAME,
      AIAN = AIAN / sum(first_name_counts$AIAN),
      API = API / sum(first_name_counts$API),
      BLACK = BLACK / sum(first_name_counts$BLACK),
      HISP = HISP / sum(first_name_counts$HISP),
      WHITE = WHITE / sum(first_name_counts$WHITE),
      OTHER = OTHER / sum(first_name_counts$OTHER),
    )
  
  write_rds(p_race_given_firstname, paste0(DIR_PATH,"p_race_given_firstname.rds"))
  write_rds(p_firstname_given_race, paste0(DIR_PATH,"p_firstname_given_race.rds"))
  
} else {
  
  p_race_given_firstname <- read_rds(paste0(DIR_PATH,"p_race_given_firstname.rds"))
  p_firstname_given_race <- read_rds(paste0(DIR_PATH,"p_firstname_given_race.rds"))
  
}