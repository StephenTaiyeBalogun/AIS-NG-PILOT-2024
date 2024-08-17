
# install required packages ---------------------------------------------------------------------------------------

if (!requireNamespace("pacman")) install.packages("pacman", dependencies = TRUE)

pacman::p_load(
  tidyverse,
  here,
  janitor,
  fs,
  hms,
  readxl
)


# household interview and roster ---------------------------------------------------------------------------------

folder_path <- dir_ls(here("data", "household"))

### the folder_path contains the following datasets

# 1. hh_identification,
# 2. hh_characteristics
# 3. hh_death,
# 4. hh_members and
# 5. hh_schedule tables


## import the 5 tables and remove the 'id', 'created_at', 'updated_at' and 'file_stored_date' metadata from the tables as they are not from CAPI. Also dropping the 'unique_id' since it's created from the cluster id (qhclust) and id of household (qhnumber).

hh_tables <- map(
  folder_path, \(df) read_csv(df, na = c("", "NA", "NULL", "null"))
) |>
  map(clean_names) |>
  map(\(df) select(df, !c(id, created_at, updated_at, file_stored_date, unique_id)))


## join the three tables from the household questionnaires, then the two tables from the roster separately, then merge the two together.
## The hh_deaths table has a few households that had more than one deaths, so we transpose to give one record per household with the information about deaths added as new columns

hh_dataset <- hh_tables[1:2] |>
  reduce(full_join, by = join_by(qhclust, qhnumber)) |>
  full_join(
    hh_tables[[3]] |>
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
      hh_tables[[4]],
      hh_tables[[5]] |> rename(qhline = qh01)  ## table 4 has the same variable named as 'qhline' so renaming for consistency
      ) |>
      reduce(full_join, by = join_by(qhclust, qhnumber, qhline)),
    by = join_by(qhclust, qhnumber),
    relationship = "many-to-many"
  ) |>
  mutate(
    country = "Nigeria",
    year = 2024,
    centroidid = paste0("NG00", qhclust), ## standardize centroidid to 8 character length as recommended by PHIA
    qhnumber = if_else(
      nchar(qhnumber) == 2,
      paste0("0000", qhnumber), ## standardize household number to 6 character length to generate householdid of 14 character length as recommended
      paste0("00000", qhnumber)
      ),
    qhline = if_else(
      nchar(qhline) < 2,
      paste0("0", qhline),
      as.character(qhline)
    ),
    householdid = paste0(centroidid, qhnumber),
    personid = paste0(householdid, qhline)
    ) |>
  relocate(country, year, centroidid, householdid, personid, .before = qhnumber) |>
  select(!c(qhclust, qhnumber, qhline, qhaddress, ghlatitude, ghlatitude_1, ghlongitude, ghlongitude_1))


hh_dataset |>
  write_csv(
    here("output", "merged_datasets", "2024-08-16 AIS pilot household dataset.csv"),
    na = ""
  )




# individual interview --------------------------------------------------------------------------------------------

folder_path2 <- dir_ls(here("data", "individual"))

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


ind_tables <- map(
  folder_path2, \(df) read_csv(df, na = c("", "NA", "NULL", "null"))
) |>
  map(clean_names)


## remove metadata from the datasets. Also dropping the unique_id here because it already concatenates the cluster id, household id and the person id (albeit not in the standard format).  The child roster for mother needs additional cleaning so it is handled separately.

all_but_roster <- map(ind_tables[1:12], \(df) select(df, !c(id, created_at, updated_at, file_stored_date, unique_id)))


child_roster <- ind_tables[[13]] |>
  select(!c(id, created_at, updated_at, file_stored_date, unique_id)) |>
  filter(is.na(childlisted), qnumber != 9907)


## merge all the individual interview datasets together

ind_dataset <- all_but_roster |>
  reduce(full_join, by = join_by(qcluster, qnumber, qline)) |>
  full_join(child_roster, by = join_by(qcluster, qnumber, qline, childnamex)) |>
  mutate(
    country = "Nigeria",
    year = 2024,
    centroidid = paste0("NG00", qcluster), ## standardize centroidid to 8 character length as recommended by PHIA
    qnumber = if_else(
      nchar(qnumber) == 2,
      paste0("0000", qnumber), ## standardize household number to 6 character length to generate householdid of 14 character length as recommended
      paste0("00000", qnumber)
    ),
    qline = if_else(
      nchar(qline) < 2,
      paste0("0", qline),
      as.character(qline)
    ),
    householdid = paste0(centroidid, qnumber),
    personid = paste0(householdid, qline)
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

folder_path3 <- dir_ls(
  here("data", "laboratory")
)

## the folder contains the LDMS TREM and GEENIUS datasets. However, the LDMS TREM has both the QA and the GEENIUS results so we are using only the TREM data

lab_tables <- map(
  folder_path3, \(df) read_csv(df, na = c("", "NA", "NULL", "null"))
) |>
  map(clean_names)


## extract each table and format/create the date columns as appropriate
ldms_trem <- lab_tables[[1]] |>
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

