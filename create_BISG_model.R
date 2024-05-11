library(tidyverse)
library(data.table)
library(doParallel)
Sys.setenv(USE_CACHED=TRUE)
Sys.setenv(LAPLACE=0.1)
DIR_PATH <- Sys.getenv("DIR_PATH") # set your path
source(paste0(DIR_PATH,"create_tables.R"))

# Function to convert a data frame to a data table for quicker lookup
to_data_table <- function(df) {
  t <- data.table(df)
  setkeyv(t, cols=c(names(t)[1]))
  return(t)
}

# Create table versions of all probability tables
p_race_given_cbg_table <- to_data_table(p_race_given_cbg)
p_cbg_given_race_table <- to_data_table(p_cbg_given_race)
p_race_given_surname_table <- to_data_table(p_race_given_surname)
p_surname_given_race_table <- to_data_table(p_surname_given_race)
p_race_given_firstname_table <- to_data_table(p_race_given_firstname)
p_firstname_given_race_table <- to_data_table(p_firstname_given_race)

predict_race <- function(firstname, surname, cbg) {
  
  surname <- toupper(surname)
  firstname <- toupper(firstname)
  
  prob_cbg_given_race <- p_cbg_given_race_table[cbg] %>% select(-CBG) %>% unlist(use.names=F)
  prob_race_given_cbg <- p_race_given_cbg_table[cbg] %>% select(-CBG) %>% unlist(use.names=F)
  prob_race_given_surname <- p_race_given_surname_table[surname] %>% select(-NAME) %>% unlist(use.names=F)
  prob_surname_given_race <- p_surname_given_race_table[surname] %>% select(-NAME) %>% unlist(use.names=F)
  prob_first_name_given_race <- p_firstname_given_race_table[firstname] %>% select(-NAME) %>% unlist(use.names=F)
  prob_race_given_first_name <- p_race_given_firstname_table[firstname] %>% select(-NAME) %>% unlist(use.names=F)
  race_probs <- p_race %>% unlist(use.names=F)
  
  # CASE 1: USE ALL 3 INPUTS
  if (!anyNA(prob_cbg_given_race) & 
      !anyNA(prob_race_given_surname) &
      !anyNA(prob_first_name_given_race)) {
    
    probs <- (prob_race_given_surname * prob_cbg_given_race * prob_first_name_given_race) /
      sum(prob_race_given_surname * prob_cbg_given_race * prob_first_name_given_race)
    case = "FIRST,LAST,CBG"
    
    # CASE 2: MISSING CBG
  } else if (!anyNA(prob_race_given_surname) & 
             !anyNA(prob_first_name_given_race)) {
    
    probs <- (prob_race_given_surname * prob_first_name_given_race) /
      sum(prob_race_given_surname * prob_first_name_given_race)
    case = "FIRST,LAST"
    
    # CASE 3: MISSING SURNAME (USE CBG AS PRIOR)
  } else if (!anyNA(prob_race_given_cbg) & 
             !anyNA(prob_first_name_given_race)) {
    
    probs <- (prob_race_given_cbg * prob_first_name_given_race) /
      sum(prob_race_given_cbg * prob_first_name_given_race)
    case = "FIRST,CBG"
    
    # CASE 4: MISSING FIRST NAME
  } else if (anyNA(prob_first_name_given_race) & 
             !anyNA(prob_cbg_given_race) &
             !anyNA(prob_race_given_surname)) {
    
    probs <- (prob_race_given_surname * prob_cbg_given_race)  /
      sum(prob_race_given_surname * prob_cbg_given_race)
    case = "LAST,CBG"
    
    # CASE 5: ONLY CBG
  } else if (!anyNA(prob_race_given_cbg)) {
    
    probs <- prob_race_given_cbg
    case = "CBG"
    
    # CASE 6: ONLY SURNAME (USE STATE DISTRIB. AS PRIOR)
  } else if (!anyNA(prob_race_given_surname)) {
    
    probs <- (prob_surname_given_race * race_probs) /
      sum(prob_surname_given_race * race_probs)
    case="LAST"
    
    # CASE 7: ONLY FIRST NAME (USE STATE DISTRIB. AS PRIOR)
  } else if (!anyNA(prob_race_given_first_name)) {
    
    probs <- (prob_first_name_given_race * race_probs) /
      sum(prob_first_name_given_race * race_probs)
    case="FIRST"
    
    # CASE 8: NO INPUT INFORMATION
  } else {
    
    probs <- race_probs
    case="NONE"
    
  }

  probs <- as.list(probs)
  names(probs) <- names(p_race)
  probs$CASE=case
  as_tibble_row(probs)
  
}

# Parallelize any prediction function that operates on a single data point
# to operate on a full data frame with columns FIRST_NAME, SURNAME, CBG.
# Prediction function should accept (FIRST_NAME, SURNAME, CBG) as arguments.
# Prediction function should return a list / vector of probabilities corresponding
# to the predicted probability of each race/ethnicity: AIAN, API, BLACK, HISP, WHITE, OTHER.
predict_parallel <- function(df, fn) {
  
  registerDoParallel(detectCores())
  
  preds <- foreach (i = 1:nrow(df), .combine=rbind) %dopar% {
    fn(df[i,]$FIRST_NAME, df[i,]$SURNAME, df[i,]$CBG)
  }
  
  preds <- preds %>%
    add_column(names(p_race)[max.col(select(preds, AIAN, API, BLACK, HISP, WHITE, OTHER), ties.method='first')])
  
  names(preds)[length(names(preds))] <- "PREDICTION"
  
  preds %>%
    select(PREDICTION, everything()) %>%
    bind_cols(df, .)
  
}
