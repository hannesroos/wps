rm(list = ls())

## Load packages

library(tidyverse)
library(lubridate)
library(sf)
library(odbc)
library(countrycode)
library(zoo)

## Connect to HCSS Data Warehouse

con <- dbConnect(odbc::odbc(), "dwh")

## Function for point to polygon

geo_inside <- function(lon, lat, map, variable) {
  
  variable <- enquo(variable)
  # slow if lots of lons and lats or big sf - needs improvement
  pt <-
    tibble(
      x = lon,
      y = lat) %>%
    st_as_sf(coords = c("x", "y"), crs = st_crs(map))
  
  pt %>% 
    st_join(map) %>% 
    pull(!!variable)
  
}

## Load polygons data

polygons <- st_read("/srv/r-common/shapefiles/gdam/gadm36_levels_shp/gadm36_2.shp", stringsAsFactors = FALSE)
polygons_df <- polygons

st_geometry(polygons_df) <- NULL

## Load ACLED data

acled <- dbGetQuery(con, "select * from ev.acled") %>% 
  mutate_if(bit64::is.integer64, as.integer) %>% 
  mutate(
    adm2_code = geo_inside(longitude, latitude, polygons, GID_2)
  )

## Filter ACLED data based on task, aggregate up to ADM2-month level

acled_filtered <- acled %>% 
  filter(
    event_type %in% c("Battles", "Explosions/Remote violence", "Explosions/Remote violence", "Violence against civilians", "Riots") | sub_event_type == "Excessive force against protesters",
    !is.na(adm2_code)
  ) %>% 
  mutate(
    month_conflict = ymd(paste(year(event_date), month(event_date), 1))
  ) %>% 
  group_by(iso3c, adm2_code, month_conflict) %>% 
  summarise(
    fatalities = sum(fatalities),
    count = n()
  ) %>% 
  ungroup()

## Load data on when ACLED data starts per country

acled_start <- read_csv("data/acled_start_month_apr19_version.csv") %>% 
  mutate(
    Country = ifelse(Country == "Eswatini", "Swaziland", Country),
    iso3c = countrycode(Country, "country.name", "iso3c"),
    month_start = dmy(paste(1, `Start Date`)),
    iso3c = ifelse(Country == "Kosovo", "XKX", iso3c)
  ) %>% 
  select(iso3c, month_start)

## Create monthly rack

months = tibble(
  month_conflict = seq(ymd('1997-01-01'), ymd('2019-04-01'), by = 'month')
)

## Create rack of ADM2 regions

gid2s <- polygons_df %>% 
  select(GID_0, GID_2) %>% 
  rename(
    iso3c = GID_0,
    adm2_code = GID_2)

## Combine data on ACLED start dates per country and ADM2 regions of the countries

rack <- merge(acled_start, months) %>% 
  filter(month_conflict >= month_start) %>% 
  left_join(gid2s)

## Join rack with ACLED data, create a binary indicator of conflict with fatalities in ADM2, calculate 12 month variables

acled_full <- left_join(rack, acled_filtered) %>% 
  mutate(
    fatalities = ifelse(is.na(fatalities), 0, fatalities),
    count = ifelse(is.na(count), 0, count),
    binary = ifelse(fatalities > 0, 1, 0)
  ) %>% 
  arrange(adm2_code, month_conflict) %>% 
  group_by(adm2_code) %>% 
  mutate(
    fatalities_12m = rollsum(fatalities, 12, align = "left", fill = NA),
    count_12m = rollsum(count, 12, align = "left", fill = NA),
    binary_12m = rollmax(binary, 12, align = "left", fill = NA)
  ) %>% 
  ungroup()
