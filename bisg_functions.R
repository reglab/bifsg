USE_CACHED <- T
DIR_PATH <- "" # set your path

geo_race_table_lst <- readRDS(paste0(DIR_PATH,"geo_race_table.rds"))

p_race_given_surname <- readRDS(paste0(DIR_PATH,"p_race_given_surname.rds"))
p_surname_given_race <- readRDS(paste0(DIR_PATH,"p_surname_given_race.rds"))
p_race_given_firstname <- readRDS(paste0(DIR_PATH,"p_race_given_firstname.rds"))
p_firstname_given_race <- readRDS(paste0(DIR_PATH,"p_firstname_given_race.rds"))

#df is your dataframe. It just needs the fields "id" (any kind of unique identifier), "surname", "firstname" (if you're running BIFSG), and at least one of the following geo fields: "cbg", "tract", "zip", "county" (default), or "state" (specify which geo level you want to use as a parameter in the function call). The function will add columns with posterior probabilities to df.
predict_race <- function(df, firstname=F, geo = "county"){
  
  race_var <- p_race_given_surname %>% select(-name) %>% names()
  
  if(geo == "cbg"){
    p_race_given_geo <- geo_race_table_lst$p_race_given_cbg %>% 
      bind_rows(geo_race_table_lst$p_race_given_us)
    p_geo_given_race <- geo_race_table_lst$p_cbg_given_race %>% 
      bind_rows(geo_race_table_lst$p_us_given_race)
    
    df <- df %>% 
      mutate(
        GEOID = ifelse(
          cbg %in% p_geo_given_race$GEOID,
          cbg,
          "1"
        )
      )
  }
  if(geo == "tract"){
    p_race_given_geo <- geo_race_table_lst$p_race_given_tract %>% 
      bind_rows(geo_race_table_lst$p_race_given_us)
    p_geo_given_race <- geo_race_table_lst$p_tract_given_race %>% 
      bind_rows(geo_race_table_lst$p_us_given_race)
    
    df <- df %>% 
      mutate(
        GEOID = ifelse(
          tract %in% p_geo_given_race$GEOID,
          tract,
          "1"
        )
      )
  }
  if(geo == "zip"){
    p_race_given_geo <- geo_race_table_lst$p_race_given_zcta %>% 
      bind_rows(geo_race_table_lst$p_race_given_us)
    p_geo_given_race <- geo_race_table_lst$p_zcta_given_race %>% 
      bind_rows(geo_race_table_lst$p_us_given_race)
    
    df <- df %>% 
      mutate(
        GEOID = ifelse(
          zip %in% p_geo_given_race$GEOID,
          zip,
          "1"
        )
      )
  }
  if(geo == "county"){
    p_race_given_geo <- geo_race_table_lst$p_race_given_county %>% 
      bind_rows(geo_race_table_lst$p_race_given_us)
    p_geo_given_race <- geo_race_table_lst$p_county_given_race %>% 
      bind_rows(geo_race_table_lst$p_us_given_race)
    
    df <- df %>% 
      mutate(
        GEOID = ifelse(
          county %in% p_geo_given_race$GEOID,
          county,
          "1"
        )
      )
  }
  if(geo == "state"){
    p_race_given_geo <- geo_race_table_lst$p_race_given_state %>% 
      bind_rows(geo_race_table_lst$p_race_given_us)
    p_geo_given_race <- geo_race_table_lst$p_state_given_race %>% 
      bind_rows(geo_race_table_lst$p_us_given_race)
    
    df <- df %>% 
      mutate(
        GEOID = ifelse(
          state %in% p_geo_given_race$GEOID,
          state,
          "1"
        )
      )
  }
  
  df1 <- df %>%
    inner_join(p_race_given_surname, by=c("surname"="name"))
  
  df1_posteriors <- NULL
  
  if (nrow(df1) > 0){
    priors <- df1 %>%
      select(all_of(race_var))
    
    update <- df1 %>% 
      select(GEOID) %>% 
      left_join(p_geo_given_race, by="GEOID") %>%
      select(all_of(race_var))
    post <- priors * update
    
    df1_posteriors <- cbind(
      df1 %>% select(id),
      post
    ) 
  }
  
  # step 2
  df2 <- df %>%
    anti_join(p_race_given_surname, by=c("surname"="name"))
  
  df2_posteriors <- NULL
  if (nrow(df2) > 0){
    priors <- df2 %>%
      select(GEOID) %>% 
      inner_join(p_race_given_geo, by="GEOID") %>%
      select(all_of(race_var))
    
    df2_posteriors <- cbind(
      df2 %>% select(id),
      priors
    )
  }
  
  post_combined <- rbind(df1_posteriors, df2_posteriors) 
  
  if (firstname){
    
    firstname_update <- df %>%
      left_join(p_firstname_given_race,
                by=c("firstname"="name")) %>%
      distinct() %>% 
      inner_join(post_combined %>% select(id), ., 
                 by="id") %>% 
      select(all_of(race_var)) %>%
      mutate(
        across(everything(), ~replace_na(.,1))
      )
    
    total_prior <- select(post_combined, all_of(race_var))
    
    post_firstname <- total_prior * firstname_update
    post_firstname_normalized <- post_firstname / rowSums(post_firstname)
    
    post_combined <- cbind(post_combined %>% select(id),
                           post_firstname_normalized)
  }
  
  df_posteriors <- post_combined %>%
    mutate(sum_to_adjust = rowSums(select(., -c(id)))) %>%
    mutate(across(-c(id,-sum_to_adjust), ~./sum_to_adjust)) %>%
    select(-sum_to_adjust) %>% 
    left_join(df,., by = "id")
  
  return(df_posteriors)
}

# Example
# data <- data.frame(
#   id = c(1:3),
#   firstname = c("JOHN","JOSE","JIANG"),
#   surname = c("WALKER","LOPEZ","LI"),
#   cbg = c("010010201001","010010201002",NA)
# )
# 
# result_bisg <- data %>% 
#   predict_race(geo = "cbg")
# 
# result_bifsg <- data %>% 
#   predict_race(firstname = T, geo = "cbg")