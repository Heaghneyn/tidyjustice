#### CREATE DATA ENVIRONMENT ####

##initialize data folder
#' @description
#' This function will create or connect to a folder to store justice data pull through the Socrata platform 
#' as well as store API credentials
#' @param folder str folder location to be created or initialized
init_justice_data <- function(justice_folder) {
  
  #check if specified justice folder exists, if it doesn't give user option to create
  if (dir.exists(justice_folder)) {
    cli::cli_alert_success("Justice folder found!")
  } else {
    val <- request_input(
      request = paste0("Confirm creation of Justice folder at '", justice_folder),
      vals = c("Y", "N"))
    if (val == "Y") {
      dir.create(path = justice_folder)
      cli::cli_alert_success(paste0("- Justice folder created at'", justice_folder, "'"))
    } else {
      stop(paste0("Please run function again with updated folder location"))
    }
  }
  Sys.setenv(JUSTICE_FOLDER = justice_folder)
  
  #ensure that data cache is present
  cli::cli_alert_info("Checking Justice folder structure")
  
  #check for/create raw data cache
  if (dir.exists(paste0(justice_folder, "/data"))) {
    cli::cli_alert_success("Data folder identified")
  } else {
    cli::cli_alert_success("Creating 'data' folder")
    dir.create(paste0(justice_folder, "/data"))
  }
  
  Sys.setenv(JUSTICE_DATA_CACHE = paste0(justice_folder, "/data"))
  
  #check for/create cleaned data cache
  if (dir.exists(paste0(Sys.getenv("JUSTICE_DATA_CACHE"), "/clean_data"))) {
    cli::cli_alert_success("Clean data sub-folder identified")
  } else {
    cli::cli_alert_success("Creating 'clean_data' sub-folder")
    dir.create(paste0(Sys.getenv("JUSTICE_DATA_CACHE"), "/clean_data"))
  }
  
  Sys.setenv(JUSTICE_CLEAN_DATA = paste0(Sys.getenv("JUSTICE_DATA_CACHE"), "/clean_data"))
  
  #check for/create misc folder, this is where the charge key crosswalk will live
  #this will also serve as a home for any other crosswalks or non-odyssey datasets to be pulled from
  if (dir.exists(paste0(Sys.getenv("JUSTICE_FOLDER"), "/misc"))) {
    cli::cli_alert_success("Miscellaneous folder identified")
  } else {
    cli::cli_alert_success("Creating miscellaneous folder")
    dir.create(paste0(Sys.getenv("JUSTICE_FOLDER"), "/misc"))
  }
  
  #check if API credentials exist, if not ask for and store API credentials
  yaml_cred_file <- file.path(justice_folder, "creds.yaml")
  yaml_cred_file_exists <- file.exists(yaml_cred_file)
  cred_file <- file.path(justice_folder, "creds.rds")
  cred_file_exists <- file.exists(cred_file)
  
  if (yaml_cred_file_exists & !cred_file_exists) {
    yaml::read_yaml(yaml_cred_file) |>
      readr::write_rds(cred_file)
    cli::cli_alert_info("Cred file updated to RDS from YAML")
    
    cred_file <- file.path(justice_folder, "creds.rds")
    cred_file_exists <- file.exists(cred_file)
  }
  
  if (cred_file_exists) {
    cli::cli_alert_success("Justice API secret key found")
    
    invisible(capture.output(
      socrata_api_key <- readRDS(cred_file)$socrata_api_key))
    
    invisible(capture.output(
      socrata_id <- readRDS(cred_file)$socrata_id
    ))
    
      Sys.setenv(SOCRATA_API_KEY = socrata_api_key)
      Sys.setenv(SOCRATA_ID = socrata_id)
      
      invalid_key <- F
    } else {
      invalid_key <- T
    }
  
  if (invalid_key) {
    cli::cli_alert_warning("Socrata API Key and ID are not found - enter a new key and ID? (yes/no)")
    while (TRUE) {
      response <- stringr::str_to_lower(stringr::str_trim(readline("Response: ")))
      if (!response %in% c("yes", "no")) {
        cli::cli_alert_warning("Invalid response. Please try again.")
      } else if (response == "no") {
        break
      } else if (response == "yes") {
        cli::cli_alert_warning("Please enter new key and ID")
          
          key <- readline(prompt = "Please enter your Socrata API secret key (without any quotation marks or wrappers): ")
          id <- readline(prompt = "Please enter your Socrata ID (without any quotation marks or wrappers): ")
            
            list(
              "socrata_api_key" = key,
              "socrata_id" = id
            ) |> yaml::write_yaml(yaml_cred_file)
              
            Sys.setenv(SOCRATA_API_KEY = key)
            Sys.setenv(SOCRATA_ID = id)
            
            break
            
      }
    }
  }
  
  cli::cli_alert_success("Justice folder successfully initialized!")
  
}

#### DATA ACCESS FUNCTIONS ####

## pull socrata table
#' @description
#' a function to pull a singular table through the Socrata API
#' @param api_key str the user's Socrata API secret key
#' @param id str the user's Socrata ID
#' @param table str the web address of the Socrata table being downloaded
#' @param table_name str the name the table will be saved under
download_table <- function(api_key = Sys.getenv("SOCRATA_API_KEY"), 
                           id = Sys.getenv("SOCRATA_ID"), 
                           table, 
                           table_name) {
  
  df <- RSocrata::read.socrata(
    url = table,
    email = id,
    password = api_key
  )
  
  timestamp <- lubridate::as_date(Sys.time())
  
  write_rds(df, file = paste0(Sys.getenv("JUSTICE_DATA_CACHE"), "/", table_name, timestamp, ".rds"))
  
  gc()
  
}

##download all 'justice' tables
download_all_tables <- function(x) {
  
}


##load data table
#' @description
#' A function to load raw data tables from the user's data cache
#' @param data_cache str folder location of the justice data cache
#' @return tibble df object that the user selects
load_raw_data <- function(data_cache = Sys.getenv("JUSTICE_DATA_CACHE")) {
  
  #list data in data cache and remove clean_data folder
  data.list <- list.files(data_cache)
  data.list <- data.list[!grepl("clean_data", data.list)]
    
  #prompt user with list and have them enter selection number
  cat("Available files in your data cache:\n")
  for (i in seq_along(data.list)) {
    cat(i, ":", data.list[i], "\n")
  }
  
  cat("\nPlease select a file by entering its number: ")
  selection <- as.integer(readLines(con = stdin(), n = 1))
  
  #validate the user's selection
  if (!is.na(selection) && selection >= 1 & selection  <= length(data.list)) {
    selected_file <- data.list[selection]
    cat("\nYou selected:", selected_file, ", loading...\n")
  } else {
    cat("\nInvalid selection. Please try again.\n")
  }
  
  df <- readRDS(paste0(data_cache, "/", selected_file))
  
  gc()
  return(df)
}


#clear data cache
#' @description
#' A function to clear all datasets out of the data cache
#' @param data_cache str folder location of justice data cache
clear_cache <- function(data_cache = Sys.getenv("JUSTICE_DATA_CACHE")) {
  
  #list data files (match pattern to .rds to keep folders untouched) in the cache and remove 
  data.list <- list.files(data_cache)
  data.list <- data.list[grepl(".rds", data.list)]
  
  for (i in data.list) {
    
    file.remove(paste0(data_cache, "/", i))
    
  }
}

## freeze copy of data in data cache
freeze_data <- function (x) {
  
}


#### CLEANING FUNCTIONS ####

## clean booking data ##
#' @description
#' a function to clean and standardize booking data
#' @param df tibble a booking df object to be cleaned
#' @param remove_missing boolean T or F, remove records with missing bookingnbr or not
#' @return tibble with cleaned booking data
clean_booking <- function(df, 
                          remove_missing = T) {
  
  #identify duplicates
  df.dupes <- df |>
    dplyr::filter(!is.na(bookingnbr)) |>
    dplyr::group_by(bookingnbr) |>
    dplyr::mutate(n = dplyr::n()) |>
    dplyr::filter(n > 1) |>
    dplyr::arrange(desc(score), .by_group = T) |>
    dplyr::slice(1) |>
    dplyr::ungroup() |>
    dplyr::select(-n)
  
  #deduplicate and rename variables 
  df01 <- df |>
    dplyr::filter(!bookingnbr %in% df.dupes$bookingnbr) |>
    rbind(df.dupes) |>
    dplyr::rename(jailing.id = jailingid, 
                  booking.num = bookingnbr, 
                  party.id = partyid, 
                  sid.num = sidnum, 
                  arrest.id = arrestid, 
                  gender = genderky, 
                  race = racedesc, 
                  delivery.agency = deliveryagency, 
                  booking.date = bookingdate, 
                  chrg.off.desc = chgoffdesc, 
                  prev.booking.num = prbkg,
                  prev.booking.date = prdate, 
                  days.diff = daysdiff, 
                  any.highneed = any_highneed, 
                  release.date = releasedate, 
                  release.desc = releasedesc, 
                  high.need = highneed, 
                  screenings.id = screeningsid, 
                  cmhs.completed.datetime = cmhsdatetimecompleted, 
                  gains.date.completed = gainsdatecompleted, 
                  length.of.stay = length_of_stay) |>
    dplyr::mutate_at(.vars = c("jailing.id", "booking.num", "party.id", "arrest.id", "age", "length.of.stay", "prev.booking.num", 
                       "days.diff", "score"), ~ suppressWarnings(as.numeric(.)))
  
  #choice to remove records where booking.num is missing
  if (remove_missing == T) {
    
  df02 <- df01 |>
    dplyr::filter(!is.na(booking.num)) |>
    dplyr::mutate(booking.date = as.Date(booking.date), 
                  prev.booking.date = as.Date(prev.booking.date), 
                  release.date = as.Date(release.date))
  
  } else {
    
    df02 <- df01 |>
      dplyr::mutate(booking.date = as.Date(booking.date), 
                    prev.booking.date = as.Date(prev.booking.date), 
                    release.date = as.Date(release.date))
    
  }
  
  #do final duplicate checking and output message if there are still dupes
  dupe.check <- df02 |>
    dplyr::group_by(booking.num) |>
    dplyr::mutate(n = dplyr::n()) |>
    dplyr::filter(n > 1) |>
    dplyr::ungroup() |>
    dplyr::pull(booking.num)
  
  if (length(dupe.check) > 0) {
    cli::cli_alert_warning("De-duplication of booking numbers failed, please investigate these booking numbers: ")
    
    for (i in 1:length(dupe.check)) {
      print(dupe.check[i])
    }
  }
  
  gc()
  return(df02)
}

## clean charge data ##
#' @description
#' a function to clean charge based datasets
#' @param charge_table tibble a charge based df object to be cleaned
#' @return tibble with cleaned charge data
clean_charges <- function(charge_table) {
  
  #remove records with missing bookingnbr and standardize dates
  #rename variables to match style
  df01 <- charge_table |>
    dplyr::filter(!is.na(bookingnbr)) |>
    dplyr::mutate(arrestdate = as.Date(arrestdate),
                  chargeoffensedescription = toupper(chargeoffensedescription)) |>
    dplyr::rename(booking.num = bookingnbr, 
                  arrest.id = arrestid,
                  arrest.seq.num = arrestsequencenum,
                  jailing.id = jailingid, 
                  party.id = partyid, 
                  arrest.date = arrestdate, 
                  charge.id = chargeid, 
                  arrest.off.hist.id = arrestoffensehistid, 
                  chrg.off.desc = chargeoffensedescription, 
                  took.screening = took_screening, 
                  familiar.face = familiar_face, 
                  hold.reason.key = holdreasonkey, 
                  hold.reason = holdreason, 
                  high.need = highneed, 
                  bond.amount = bondamount) |>
    dplyr::mutate_at(.vars = c("jailing.id", "booking.num", "party.id", "arrest.id", "charge.id", "arrest.seq.num", 
                               "arrest.off.hist.id"),  ~ suppressWarnings(as.numeric(.)))
  
  #remove extra spaces in charge description string
  df01$chrg.off.desc <- gsub("\\s+", " ", df01$chrg.off.desc)
  
  gc() 
  return(df01)
}


## clean warrant data ##
#' @description
#' a function to clean warrant data
#' @param warrant_data tibble a dataframe of warrant information (want to use warrant status table here)
#' @return tibble a cleaned df of warrant info
clean_warrants <- function(warrant_data) {
  
  #de dupe warrant by warrant number and partyid, keep only max(statusdate) record
  df01 <- warrant_data |>
    dplyr::group_by(warrantnumber, partyid) |>
    dplyr::mutate(n = dplyr::n(), 
                  maxstatusdate = max(statusdate)) |>
    dplyr::filter(statusdate == maxstatusdate) |>
    dplyr::arrange(activewarrant, .by_group = T) |>
    dplyr::slice(1) |>
    dplyr::ungroup() |>
    dplyr::select(-c("n", "maxstatusdate", "warrantid", "id", "statussequencenum")) |>
    unique()
  
  #check deduping
  dupe.check <- df01 |>
    dplyr::group_by(warrantnumber, partyid) |>
    dplyr::mutate(n = n()) |>
    dplyr::ungroup() |>
    dplyr::filter(n > 1) |>
    dplyr::pull(warrantnumber)
  
  if (length(dupe.check) > 0) {
    cli::cli_alert_warning("There are still duplicate warrant records in this dataset, please investigate these warrant numbers: ")
    
    for (i in 1:length(dupe.check)) {
      print(dupe.check[i])
    }
  }
  
  #rename and change var types to match style 
  df02 <- df01 |>
    dplyr::rename(warrant.number = warrantnumber, 
                  warrant.type.code = warranttypecd, 
                  warrant.type = warranttype, 
                  warrant.status.code = warrantstatuscd, 
                  warrant.status = warrantstatus, 
                  status.date = statusdate, 
                  warrant.status.desc = warrantstatusdesc, 
                  active.warrant = activewarrant, 
                  party.id = partyid, 
                  name.first = namefirst, 
                  name.last = namelast, 
                  name.mid = namemid, 
                  race = racedesc, 
                  gender = genderdesc, 
                  case.id = caseid, 
                  case.num = casenbr, 
                  node.id = nodeid, 
                  dob = dtdob, 
                  sex.off.warrant = sexoffenderwarrant) |>
    dplyr::mutate_at(c("party.id", "case.id", "node.id"), ~ suppressWarnings(as.numeric(.))) |>
    dplyr::mutate_at(c("warrant.type", "warrant.status.code", "warrant.status", "warrant.status.desc"), ~ toupper(.))
    
  gc()
  return(df02)
}

#### ANALYSIS FUNCTIONS ####


## extract partyID from booking number
#' @description
#' a function to attach partyID to a list of booking numbers
#' @param booking_numbers vector a list of booking numbers
#' @param booking_data tibble a df of booking data that has been cleaned with the clean_booking function
get_booking_info <- function(booking_numbers,
                             booking_data) {
 
  #ensure booking.num var is named and classed correctly
  if (!"booking.num" %in% names(booking_numbers)) {
    cli::cli_alert_warning("Please rename your booking number variable to 'booking.num'")
  }
  
  if (class(booking_numbers$booking.num) != "numeric") {
    cli::cli_alert_warning("Please convert booking.num to a numeric type variable")
  }
  
  #join booking numbers to booking data 
  df <- booking_numbers |>
    dplyr::left_join(booking_data |> dplyr::select(booking.num, party.id, booking.date, delivery.agency, gender, race, 
                                                   chrg.off.desc, release.date, release.desc, score), 
                     by = "booking.num")
  
  #identify and output booking.nums that couldn't be matched
  missed.numbers <- df |>
    dplyr::filter(is.na(party.id)) |>
    dplyr::pull(booking.num)
  
  if (length(missed.numbers) > 0) {
    cli::cli_alert_warning(paste0(length(missed.numbers), " records were not able to be matched to booking data, unmatched booking numbers:"))
    
    for (i in 1:length(missed.numbers)) {
      print(missed.numbers[i])
    }
  }
  
  gc()
  return(df)
}


##connect hearings to cases
get_hearings <- function(x) {
  
}

## get warrants for a party
#' @description
#' a function to pull warrants related to a partyID
#' 
get_warrants <- function(x) {
  
  
  
}

## connect booking data to charge standardization ##
#' @description
#' a function to crosswalk ISLG's jail_charge_key with charge data
#' @param df tibble charge based df to connect the jail charge key to
#' @return charge based tibble with ISLG charge key connected
connect_charge_key <- function(df) {
  
  #ensure charge key is present in misc folder
  file.list <- list.files(paste0(Sys.getenv("JUSTICE_FOLDER"), "/misc"))
  
  if (!"atlanta_jail_charge_key_2024.csv" %in% file.list) {
    print("Please ensure that you have the charge key from ISLG in your 'misc' folder and it is named 'atlanta_jail_charge_key_2024.csv'")
  }
  
  charge.key <- read.csv(file = paste0(Sys.getenv("JUSTICE_FOLDER"), "/misc/atlanta_jail_charge_key_2024.csv")) |>
    dplyr::select(-site)
  
  #join odyssey charge data to the charge key by statute first, create flag to subset joined rows before joining by description
  df1 <- dplyr::left_join(df, charge.key, by = c("statute" = "chrg_code", "chargeoffensedescription" = "chrg_desc")) |>
    dplyr::mutate(connected = dplyr::if_else(is.na(chrg_type_violent), 0, 1))
  
  df2 <- df1 |> 
    dplyr::filter(connected == 0) |>
    dplyr::select(-names(charge.key |> dplyr::select(-c("chrg_code", "chrg_desc"))))
  
  return()
}


#### MISC FUNCTIONS ####
#' Modified readlines function to simplify user interface
#'
#' @description Manage input
#' @param request str:
#' @param error_resp str:
#' @param vals array:
#' @param max_i int:
#' @returns val from `vals` or stops output
request_input <- function(request,
                          vals,
                          error_resp = "Valid input not chosen",
                          max_i = 3) {
  message(request)
  
  i <- 1
  
  vals_str <- paste0(
    "Please choose one of the following [",
    paste0(as.character(vals), collapse = "/"),
    "]:  \n"
  )
  
  val <- readline(prompt = vals_str)
  
  while (!val %in% vals) {
    val <- readline(prompt = vals_str)
    i <- i + 1
    if (i == max_i) {
      stop(error_resp)
    }
  }
  
  return(val)
}
