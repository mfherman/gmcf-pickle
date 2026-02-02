library(tidyverse)
library(httr2)
library(jsonlite)

enroll <- read_csv("data/gmcf_1770041442_enrollments.csv", col_names = FALSE)

player_id_player_name_semi <- enroll |> 
  select(player_id = X1, player_name = X2) |> 
  filter(player_id != "player_id") |> 
  mutate(player_id = as.numeric(player_id))

# likely dupes - need to fix these!
player_id_player_name |> 
  group_by(player_name) |> 
  arrange(player_name) |> 
  filter(n() > 1)

player_id_dupes_to_fix <- read_csv("data/player_id_dupes.csv")


dupe_to_remove <- player_id_dupes_to_fix |> 
  filter(dupe) |> 
  pull(player_id)

player_id_player_name <- player_id_player_name_semi |> 
  filter(!player_id %in% dupe_to_remove)


session_player_long <- as_tibble(t(enroll)) |>
  mutate(
    date_time = ymd_hm(V2),
    level = str_extract(V4, "^[^:]+"),
    level = str_replace(level, " to ", "\u2013"),
    spots = as.numeric(str_extract(V4, "\\d+$"))
    ) |> 
  select(
    session_id = V1,
    date_time,
    level,
    spots,
    starts_with("V")
  ) |> 
  mutate(across(starts_with("V"), \(x) if_else(x == "In", first(x), NA))) |>
  filter(!is.na(spots)) |>
  pivot_longer(starts_with("V"), values_to = "player_id") |>
  mutate(
    player_id = as.numeric(player_id),
    weekday = wday(date_time, label = TRUE),
    day_part = case_when(
      hour(date_time) < 12 ~ "Morning",
      between(hour(date_time), 12, 15) ~ "Afternoon",
      TRUE ~ "Evening"
      ),
    day_part = fct_relevel(day_part, "Morning", "Afternoon", "Evening")
    ) |>
  select(-name) |> 
  distinct() |> 
  left_join(player_id_dupes_to_fix, join_by(player_id)) |> 
  mutate(player_id = coalesce(player_id_corrected, player_id)) |> 
  select(-player_name, -player_id_corrected, -dupe)


session_id_played <- session_player_long |> 
  filter(!is.na(player_id)) |> 
  distinct(player_id) |> 
  pull()

player_id_dupr <- read_csv("data/player_id_name_dupr.csv")

player_id_to_lookup_dupr_id <- player_id_player_name |> 
  filter(player_id %in% session_id_played) |> 
  anti_join(filter(player_id_dupr, !is.na(dupr_id)), join_by("player_id"))

# write out player ids that have no dupr to look up manually
write_csv(player_id_to_lookup_dupr_id, paste0("data/player_id_to_lookup_dupr_id_", today(), ".csv"))

new_dupr_id_found <- read_csv("data/player_id_to_lookup_dupr_id_2026-02-02.csv") |> 
  filter(!is.na(dupr_id)) |> 
  select(player_id, dupr_id)

dupr_id_to_get <- player_id_dupr |> 
  select(player_id, dupr_id) |> 
  bind_rows(new_dupr_id_found) |> 
  filter(!is.na(dupr_id))
  
base_url <- "https://api.dupr.gg"
email <- "mac@mattherman.info"
password <- "Wmc904!#@6b6b"

# This gets your access token
auth_resp <- request(base_url) |> 
  req_url_path("/auth/v1.0/login") |> 
  req_body_json(list(email = email, password = password)) |> 
  req_perform()

token <- resp_body_json(auth_resp)$result$accessToken

get_dupr <- function(dupr_id) {
  Sys.sleep(runif(1, 3, 6))
  message("getting dupr for dupr id ", dupr_id)
  # browser()
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

dupr_return <- map(dupr_id_to_get$dupr_id, get_dupr_safely)

dupr_return_df <- dupr_return |> 
  list_transpose() |>
  pluck("result") |> 
  inner_join(dupr_id_to_get, join_by("dupr_id")) |> 
  distinct()

player_dupr_id_name_lookup <- player_id_player_name |> 
  left_join(dupr_return_df, join_by("player_id"))

by_session <- session_player_long |> 
  left_join(player_dupr_id_name_lookup, join_by("player_id")) |> 
  group_by(session_id, date_time, level, spots, weekday, day_part) |>
  summarize(
    registered = n_distinct(player_id, na.rm = TRUE),
    mean_dupr = mean(rating, na.rm = TRUE)
    ) |> 
  ungroup() |> 
  mutate(
    registered = if_else(registered < 4, 0, registered),
    registered = if_else(registered > spots, spots, registered),
    session_run = registered >= 4,
    mean_dupr = if_else(!session_run, NA_real_, mean_dupr),
    pct_registered = registered / spots
    )


write_rds(by_session, "data/by_session.rds")
write_rds(session_player_long, "data/session_player_long.rds")
write_rds(player_dupr_id_name_lookup, "data/player_dupr_id_name_lookup.rds")

# session_player_long |> 
#   count(player_id, sort = TRUE) |> 
#   left_join(player_id_name_lookup) |> 
#   filter(!is.na(player_id)) |> 
#   write_csv("data/player_id_name.csv")

# 
# player_dupr_lookup <- dupr_return_df |> 
#   mutate(rating = as.numeric(rating), dupr_id) |>
#   left_join(player_id_name, join_by(dupr_id)) |> 
#   mutate(player_id = as.character(player_id)) |> 
#   select(-n, -player_name)
# 
# 
# write_rds(player_dupr_lookup, "data/player_id_dupr_lookup.rds")




# request(base_url) |> 
#   req_url_path(paste0("player/v1.0/", "2936665")) |> 
#   req_headers(Authorization = paste("Bearer", token))
# 
# get_dupr("2936665")


