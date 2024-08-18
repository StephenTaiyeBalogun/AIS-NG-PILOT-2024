
# load packages and import files --------------------------------------------------------------------------

if (!requireNamespace("pacman")) install.packages("pacman", dependencies = TRUE)

pacman::p_load(
  tidyverse,
  here,
  janitor,
  fs,
  hms,
  readxl
)


# Define folder paths
hh_folder <- dir_ls(here("data", "household"))

ind_folder <- dir_ls(here("data", "individual"))

lab_folder <- dir_ls(here("data", "laboratory"))


# Function to read and clean data. The cleaning involves removing the metadata variables and the `unique id` variable derived from the individual cluster, household and person identifier as appropriate.

read_and_clean <- function(path) {
  read_csv(path, na = c("", "NA", "NULL", "null")) |>
    clean_names() |>
    select(!c(id, created_at, updated_at, file_stored_date, unique_id))
}


# household dataset -----------------------------------------------------------------------------------------------

## Load and clean household data

# the folder_path contains the following datasets

# 1. hh_identification,
# 2. hh_characteristics
# 3. hh_death,
# 4. hh_members and
# 5. hh_schedule tables

hh_data <- hh_folder |>
  map(read_and_clean)


# Merge household datasets

## The hh_deaths table has a few households that had more than one deaths, so we transpose to give one record per household with the information about deaths added as new columns before merging with the other datasets

hh_dataset <- hh_data[1:2] |>
  reduce(full_join, by = join_by(qhclust, qhnumber)) |>
  full_join(
    hh_data[[3]] |>
      pivot_wider(
        id_cols = c(qhclust, qhnumber, qh40, qh41, qh42, qh43, qh44, qh45, qh46, qh47, qh48, line_num2),
        names_from = qhmdline,
        values_from = c(qh39d:qh39y, dead_dk, qh39, diedname:diedgend),
        names_vary = "slowest"
      ),
    by = join_by(qhclust, qhnumber)
  ) |>
  full_join(
    list(
      hh_data[[4]],
      hh_data[[5]] |> rename(qhline = qh01)  ## table 4 has the same variable named as 'qhline' so renaming for consistency
      ) |>
      reduce(full_join, by = join_by(qhclust, qhnumber, qhline)),
    by = join_by(qhclust, qhnumber)
  ) |>
  mutate(
    country = "Nigeria",
    year = 2024,
    centroidid = str_c("NG00", qhclust),   ## standardize centroid id to 8 character length
    qhnumber = str_pad(qhnumber, 6, pad = "0"), ## standardize qhnumber to six character length
    qhline = str_pad(qhline, 2, pad = "0"),   ## standardize qhline to 2 character length
    householdid = str_c(centroidid, qhnumber), ## housholdid has 14 character length (8 + 6)
    personid = str_c(householdid, qhline)      ## personsid as 16 character length(14 + 2)
    ) |>
  relocate(country, year, centroidid, householdid, personid, .before = qhnumber) |>
  select(!c(qhclust, qhnumber, qhline, qhaddress, ghlatitude, ghlatitude_1, ghlongitude, ghlongitude_1))


hh_dataset |>
  write_csv(
    here("output", "merged_datasets", "2024-08-16 AIS pilot household dataset.csv"),
    na = ""
  )




# individual dataset ----------------------------------------------------------------------------------------------

# Load and clean individual data

### the folder_path contains the following tables:
# 1. individual identification,
# 2. adult questions,
# 3. respondent background,
# 4. laboratorian respondent background,
# 5. pretest questionnaire,
# 6. laboratory determine,
# 7. laboratory unigold,
# 8. laboratory stat-pak,
# 9. posttest questionnaire,
# 10. linkage to care
# 11. appendix 5 and 6
# 12. mother pregnant care HIV screening, and
# 13. child roster for mother

ind_data <- ind_folder |>
  map(read_and_clean)

# Special handling for child roster to remove observations that were not part of the pilot data
child_roster <- ind_data[[13]] |>
  filter(is.na(childlisted), qnumber != 9907)


## merge all the individual tables

ind_dataset <- ind_data[1:12] |>
  reduce(full_join, by = join_by(qcluster, qnumber, qline)) |>
  full_join(child_roster, by = join_by(qcluster, qnumber, qline, childnamex)) |>
  mutate(
    country = "Nigeria",
    year = 2024,
    centroidid = str_c("NG00", qcluster), ## standardize centroidid to 8 character length
    qnumber = str_pad(qnumber, 6, pad = "0"), ## standardize qnumber to 6 character length
    qline = str_pad(qline, 2, pad = "0"),  ## standardize qline to 2 character length
    householdid = str_c(centroidid, qnumber),
    personid = str_c(householdid, qline)
  ) |>
  rename(ptid = ptid1) |>
  select(!c(qcluster, qnumber, qline, ptid_link)) |>
  relocate(country, year, centroidid, householdid, personid, .before = qviolen)


ind_dataset |>
  write_csv(
    here("output", "merged_datasets", "2024-08-16 AIS pilot individual dataset.csv"),
    na = ""
  )




# satellite lab data -----------------------------------------------------------------------------------------------

## the folder contains the LDMS TREM and GEENIUS datasets. However, the LDMS TREM has both the QA and the GEENIUS results so we are using only the TREM data

lab_data <- lab_folder |>
  map(\(df) read_csv(df, na = c("", "NA", "NULL", "null"))) |>
  map(clean_names)


## extract each table and format/create the date columns as appropriate
ldms_trem <- lab_data[[1]] |>
  select(id1:vid, testnm:result, finres, qadisc) |>
  pivot_wider(
    id_cols = c(id1:vid, finres, qadisc),
    names_from = testnm,
    values_from = c(rsltid, result),
    names_glue = "{testnm}_{.value}",
    names_vary = "slowest"
  ) |>
  clean_names() |>
  select(!geenius_result) |>
  relocate(unigold_rsltid, unigold_result, .before = stat_pak_rsltid) |>
  relocate(finres, qadisc, .after = geenius_rsltid)



# merge household, individual and ldms_trem datasets --------------------------------------------------------------

## joining the
# 1. household to individual using the country, year, centroidid, householdid and personid variables. There are 13 records in the individual dataset not in the household dataset, accounting for the increase from 437 to 450 when both are joined.

# 2. there are 19 records in the LDMS data that are not in the CAPI data, accounting for a further increase from 450 to 469 in the final join of CAPI and LDMS data.


capi_ldms_dataset <- hh_dataset |>
  full_join(ind_dataset, by = join_by(country, year, centroidid, householdid, personid)) |>
  full_join(ldms_trem, by = join_by(ptid == id1))


capi_ldms_dataset |>
  write_csv(
    here("output", "merged_datasets", "2024-08-16 AIS pilot CAPI and LDMS dataset.csv"),
    na = ""
  )

