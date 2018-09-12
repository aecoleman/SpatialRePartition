library(acs)
library(stringr)
library(tigris)
library(sp)
library(sf)
library(tmap)
library(tmaptools)
library(data.table)
library(units)

# -----

year <- 2016

# Note: 5e-2 gives a square with about 5km per side
cellsize <- 5e-2

dirpath <- 'D:/Shapefiles/TIGRIS'

oconus <- c('AK', 'HI', 'PR', 'GU', 'MP', 'VI', 'AS')

roads.to.keep <- c('S1100', 'S1200', 'S1400', 'S1640')

# Get State data from TIGER ----

fips <- tigris::counties(year = year, 
                         class = 'sf')

# Processing ----

oconus.fips <- sprintf('%02d', fips.state$STATE[fips.state$STUSAB %in% oconus])

state <- fips[! fips$STATEFP %in% oconus.fips,]

crs.state <- st_crs(state)

grd <- 
  st_make_grid(state, 
               cellsize = c(cellsize, cellsize)) %>% 
  as.data.frame() %>%
  st_as_sf() 

st_crs(grd) <- crs.state

grd <- grd[state,]

grd <- data.table(grd)

grd[, `:=`( grid.id = .I,
            LEN = as_units(0, 'm') ) ]

setkeyv(grd, cols = 'grid.id')

# ----

file.nm <- dir(dirpath, pattern = '^TL_2016_[A-Z][A-Z]_ROADS.gpkg')

state.nm <- str_extract(file.nm, '(?<=2016_)[A-Z][A-Z](?=_)')

file.nm <- file.nm[!state.nm %in% oconus]

# fips <- tigris::counties(state = 'IN', class = 'sf')

# puma <- st_read( file.path(dirpath, 'TL_2016_IN_PUMA.gpkg'))

# awtr <- st_read( file.path(dirpath, 'TL_2016_IN_AWATER.gpkg'))

# tpb <- txtProgressBar(min = 0, max = length(file.nm), style = 3)

for( i in file.nm ){

  road <- st_read( file.path(dirpath, i), 
                   stringsAsFactors = FALSE)

  road <- road[road$MTFCC %in% roads.to.keep,]
  
  road <- st_transform(road, crs.state)
  
  # st_crs(road) <- crs.state

  grd.rd <- st_intersection(road, st_as_sf(grd) ) %>% 
    as.data.frame() %>%
    st_as_sf() %>%
    data.table()

  grd[ grd.rd[, .(grid.id, LEN = st_length(geom) )
              ][, .(LEN = sum(LEN)), 
                  keyby = .(grid.id)],
      LEN := x.LEN + i.LEN,
      on = .(grid.id)]
  
}

rm(i, grd.rd)

grd[, LOG.LEN := log10(as.numeric(LEN)) ]

t.s <- Sys.time()

file.out <- paste0(format(t.s, format = '%Y%m%d.%H%M'), '-', year, '-road_raster_5km.RDS')

saveRDS(st_as_sf(grd), file = file.out )

# --------

tmap_mode(mode = 'plot')

t.0 <- Sys.time()

tmap_save(
  tm_shape(st_as_sf(grd)) +
    tm_fill(col = 'LEN', 
            n = 20, 
            palette = viridisLite::inferno(n = 20)) +
  tm_layout(legend.show = FALSE,
            bg.color = 'black'),
  filename = paste0(format(t.s, format = '%Y%m%d.%H%M'), 
                    '-', year, '-US_Road_Density_Map_5km_Resolution.png') )

t.0 <- difftime(Sys.time(), t.0, units = 'h')