library(tidyverse)
library(httr2)
library(jsonlite)

player_id_name <- read_csv("data/player_id_name_dupr.csv")

base_url <- "https://api.dupr.gg"
email <- "mac@mattherman.info"
password <- "Wmc904!#@6b6b"

# This gets your access token
auth_resp <- request(base_url) %>%
  req_url_path("/auth/v1.0/login") %>%
  req_body_json(list(email = email, password = password)) %>%
  req_perform()

token <- resp_body_json(auth_resp)$result$accessToken

get_dupr <- function(dupr_id) {
  Sys.sleep(runif(1, 3, 6))
  message("getting dupr for player id ", dupr_id)

  rating_resp <- request(base_url) |> 
    req_url_path(paste0("player/v1.0/", dupr_id)) |> 
    req_headers(Authorization = paste("Bearer", token)) |> 
    req_perform()
  
  resp_parsed <- resp_body_json(rating_resp)
  
  rating <- suppressWarnings(as.numeric(resp_parsed$result$ratings$doubles))
  reliability <- as.integer(resp_parsed$result$ratings$doublesReliabilityScore)

  if (is.na(rating)) {
    reliability <- NA_integer_
  }
  
  if (length(reliability) == 0) {
    reliability <- 0L
  }
  
  tibble(dupr_id, rating, reliability)

}


get_dupr_safely <- safely(get_dupr)

dupr_to_get <- player_id_name |> 
  filter(!is.na(dupr_id)) |> 
  distinct(dupr_id) |> 
  pull(dupr_id)

dupr_return <- map(dupr_to_get, get_dupr_safely)

dupr_return_df <- dupr_return |> 
  list_transpose() |>
  pluck("result") |> 
  list_rbind()
  

player_dupr_lookup <- dupr_return_df |> 
  mutate(rating = as.numeric(rating), dupr_id) |>
  left_join(player_id_name, join_by(dupr_id)) |> 
  mutate(player_id = as.character(player_id)) |> 
  select(-n, -player_name)

write_rds(player_dupr_lookup, "data/player_id_dupr_lookup.rds")
