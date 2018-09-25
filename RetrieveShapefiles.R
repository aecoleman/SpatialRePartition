# Get Shapes
# 
# Author: Andrew Coleman

# Attach Required Packages ----

library(data.table)
library(sf)
library(tigris)
library(parallel)
library(pbapply)
library(acs)

# Define parameters ----

dirpath <- 'D:/Shapefiles/TIGRIS'

year <- 2016

# Define functions ----

ReadyCluster <- function(n){
  
  cl <- makeCluster( min(n, detectCores() - 1L) )
  
  clusterEvalQ(cl, library(data.table))
  clusterEvalQ(cl, library(tigris))
  clusterEvalQ(cl, library(sf))
  clusterExport(cl, c('year', 'fips.state', 'fips.county') )

  return( cl )
  
}

SaveShapeCBSA <- function(save.dir, year){
  
  shp.cbsa.us <- tigris::core_based_statistical_areas(year = year, 
                                              class = 'sf')
  
  st_write( shp.cbsa.us, 
            file.path(save.dir, 
                      sprintf('TL_%04.0f_US_CBSA.gpkg', 
                              year) ) )
  
}

SaveShapeCongressionalDistrict <- function(save.dir, year){
  
  shp.codi.us <- tigris::congressional_districts(year = year, class = 'sf')
  
  st_write( shp.codi.us, 
            file.path(save.dir, 
                      sprintf('TL_%04.0f_US_CONGRESSIONAL_DISTRICTS.gpkg', 
                              year) ) )
  
  
}

SaveShapePUMA <- function(save.dir, year, cores){
  
  if(missing(cores)){ cores <- detectCores() - 1L }
  
  stopifnot(dir.exists(save.dir))
  
  cl <- ReadyCluster(n = cores)
  
  shp.puma.list <- pblapply(seq(nrow(fips.state)), 
                            FUN = function(x){ 
                            
                              out <- try( 
                                  st_cast(
                                    tigris::pumas( state = fips.state$STATE[x],
                                           year = year,
                                           class = 'sf' ),
                                    'MULTIPOLYGON'),
                                silent = TRUE) 
                              
                              if( identical( class(out), c('sf', 'data.frame') ) ){
                                
                                file.nm.puma <- file.path(save.dir, 
                                                          fips.state$STUSAB[x], 
                                                          sprintf('TL_%04.0f_%s_PUMA.gpkg', 
                                                                  year, 
                                                                  fips.state$STUSAB[x]) )
                                
                                if( !dir.exists( dirname( file.nm.puma ) ) ){
                                  dir.create(dirname( file.nm.puma ) ) 
                                }
                                
                                st_write( out, file.nm.puma )
                                
                                return(file.nm.puma)
                                
                              } else {
                                
                                return(NULL)
                                
                              }
                              
                            },
                            cl = cl)
  
  stopCluster(cl)

  return(shp.puma.list)
  
}

.SaveShapeForStateGet <- function(what, save.dir, year, cores, file.nm, fun.nm, fips.county, geom.type){
 
  if(missing(cores)){ cores <- detectCores() - 1L }
  
  stopifnot(dir.exists(save.dir))
  
  cl <- ReadyCluster(n = cores)
  
  shp.county.list <- 
    parLapplyLB(cl = cl,
                 X = seq(nrow(fips.county)), 
               fun = function(x){ 
                   
                   dt <- try( 
                     st_cast(
                       do.call( fun.nm, 
                                args = list('state' = fips.county$State.ANSI[x],
                                            'county' = fips.county$County.ANSI[x],
                                            'cb' = TRUE,
                                            'year' = year,
                                            'class' = 'sf') ),
                       geom.type),
                     silent = TRUE) 
                   
                   return(dt)
                     
                 }
    )
  
  stopCluster(cl)
  
  if( length( shp.county.list ) > 1 ){
    
    shp.state.dt <- do.call('rbind', shp.county.list)
    
  } else {
    
    shp.state.dt <- shp.county.list[[1]]
    
  }
  
  rm(shp.county.list)
  
  
  if( 'geometry' %in% names( shp.state.dt) & !file.exists(file.nm) ){
    
    st_write( shp.state.dt, file.nm )
    
    return(file.nm)
    
  } else {
    
    return(NULL)
  }

}

.SaveShapeForState <- function(what, save.dir, year, cores, state.fips){
  
  if( is.character( state.fips ) ){
    
    if( grepl('^[0-9]+$', state.fips) ){
      
      state.fips <- as.numeric( state.fips )
      
    } else {
      
      fips.county <- acs::fips.county[fips.county$State == state.fips,]
      
    }
    
  }
  
  if( !is.character( state.fips ) ){
    fips.county <- acs::fips.county[fips.county$State.ANSI == state.fips,]
  }
  

  
  file.nm <- 
    file.path( 
      save.dir,
      fips.county$State[1],
      sprintf('TL_%04.0f_%s_%s.gpkg', 
              year, 
              fips.county$State[1],
              what) )
  
  if( !dir.exists( dirname(file.nm) ) ){
    dir.create(  dirname(file.nm) )
  }
  
  if( what == 'AWATER' ){
    fun.nm <- 'area_water'
    geom.type <- 'MULTIPOLYGON'
  } else if ( what == 'ROADS' ){
    fun.nm <- 'roads'
    geom.type <- 'MULTILINESTRING'
  } else if ( what == 'TRACTS' ){
    fun.nm <- 'tracts'
    geom.type <- 'MULTIPOLYGON'
  }
  
  if( !file.exists(file.nm) ){
    
    return( .SaveShapeForStateGet(what, save.dir, year, cores, file.nm, fun.nm, fips.county, geom.type) )
    
  } else {
  
    return( NULL )
    
  }
}

SaveShapeByState <- function(what, save.dir, year, cores){
  
  if( missing(cores) ){
    cores <- detectCores() - 1L 
  }
  
  file.nm.list <- 
    pblapply(fips.state$STATE,
             FUN = function(x){
               
               .SaveShapeForState(what = what,
                                  save.dir = save.dir, 
                                  year = year, 
                                  cores = cores, 
                                  state.fips = x)
               
             } )
  
  return( file.nm.list )
  
}

# Get Shapefiles ----

pumas <- SaveShapePUMA(save.dir = dirpath, year = year)

# roads <- SaveShapeByState(what = 'ROADS', save.dir = dirpath, year = year)

awater <- SaveShapeByState(what = 'AWATER', save.dir = dirpath, year = year)

tracts <- SaveShapeByState(what = 'TRACTS', save.dir = dirpath, year = year)

cbsa <- SaveShapeCBSA(save.dir = dirpath, year = year)

