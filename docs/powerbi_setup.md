# Power BI dashboard setup (ICU monitoring)

The dashboard reads the CSV exports written by the Gold job to `workspace/exports/<table>/part-....csv`.
Spark replaces these files on every pipeline run (the file name changes), so the queries below always read the newest `.csv` of each folder.

Tool: Power BI Desktop (Windows). The report file is in `dashboard/`.

## 0. One-time settings
* **File → Options and settings → Options → Current File → Data Load → Time intelligence:** turn **Auto date/time** off, so date columns do not turn into Year/Quarter/Month/Day hierarchies.
* **View → Themes → Customize current theme → Text → Font family:** one font for the whole report (for example Segoe UI).

## 1. Parameter and loader function

1. **Home → Transform data → Manage Parameters → New Parameter**
   * Name `ExportsPath`, Type Text, Current value the full path of the exports folder, for example `D:\DE_internship\week11\workspace\exports` (no trailing backslash). On another machine, change only this value.
2. **New Source → Blank Query → Advanced Editor**, paste, and name the query `fnLoadGold`:

```m
(tableName as text) =>
let
    Source   = Folder.Files(ExportsPath & "\" & tableName),
    CsvOnly  = Table.SelectRows(Source, each [Extension] = ".csv"),
    Latest   = Table.Max(CsvOnly, "Date modified"),
    Bytes    = Binary.Buffer(Latest[Content]),
    Data     = Csv.Document(Bytes, [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Data, [PromoteAllScalars = true])
in
    Promoted
```
`Binary.Buffer` loads the file into memory immediately, which shortens the window in which a pipeline run can replace the file ("Could not find file" error). If a refresh still fails, wait for the DAG run to finish and refresh again.

## 2. The four table queries

Create four blank queries (Advanced Editor) with exactly these names.

**gold_patient_current_status**
```m
let
    Source = fnLoadGold("gold_patient_current_status"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"patient_id", type text}, {"device_id", type text},
        {"heart_rate", type number}, {"spo2", type number}, {"temperature_c", type number},
        {"total_score", Int64.Type}, {"patient_status", type text}, {"vitals_missing", Int64.Type},
        {"minutes_since_last_reading", type number}, {"is_stale", type logical}}, "en-US"),
    Times  = Table.TransformColumns(Typed, {
        {"event_ts", each DateTimeZone.RemoveZone(DateTimeZone.FromText(_)), type datetime},
        {"gold_processed_at", each DateTimeZone.RemoveZone(DateTimeZone.FromText(_)), type datetime}})
in
    Times
```

**gold_patient_vitals_scored**
```m
let
    Source = fnLoadGold("gold_patient_vitals_scored"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"patient_id", type text}, {"device_id", type text}, {"event_date", type date},
        {"heart_rate", type number}, {"spo2", type number}, {"temperature_c", type number},
        {"hr_out_of_range", type logical}, {"spo2_out_of_range", type logical},
        {"temp_out_of_range", type logical}, {"temp_unit_missing", type logical},
        {"hr_score", Int64.Type}, {"spo2_score", Int64.Type}, {"temp_score", Int64.Type},
        {"total_score", Int64.Type}, {"max_component_score", Int64.Type},
        {"vitals_missing", Int64.Type}, {"patient_status", type text}}, "en-US"),
    Times  = Table.TransformColumns(Typed, {
        {"event_ts", each DateTimeZone.RemoveZone(DateTimeZone.FromText(_)), type datetime},
        {"gold_processed_at", each DateTimeZone.RemoveZone(DateTimeZone.FromText(_)), type datetime}})
in
    Times
```

**gold_daily_patient_kpi**
```m
let
    Source = fnLoadGold("gold_daily_patient_kpi"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"patient_id", type text}, {"event_date", type date}, {"readings_count", Int64.Type},
        {"avg_heart_rate", type number}, {"max_heart_rate", type number},
        {"avg_spo2", type number}, {"min_spo2", type number},
        {"avg_temperature_c", type number}, {"max_temperature_c", type number},
        {"avg_score", type number}, {"max_score", Int64.Type},
        {"critical_readings", Int64.Type}, {"warning_readings", Int64.Type}, {"stable_readings", Int64.Type},
        {"out_of_range_readings", Int64.Type}, {"readings_with_missing_vitals", Int64.Type},
        {"pct_readings_critical", type number}}, "en-US")
in
    Typed
```

**gold_critical_status_summary**
```m
let
    Source = fnLoadGold("gold_critical_status_summary"),
    Typed  = Table.TransformColumnTypes(Source, {
        {"event_date", type date}, {"patient_status", type text},
        {"readings_count", Int64.Type}, {"patients_count", Int64.Type}}, "en-US")
in
    Typed
```

Click **Close & Apply**. If a type step fails, a column name differs from your export: open the CSV, compare the header and fix the name in the query.

What each table holds: `vitals_scored` has one row per reading (trends); `current_status` has one row per patient with the latest reading (ward overview); `daily_patient_kpi` has one row per patient per day; `critical_status_summary` has one row per day and status.

## 3. Model

**Model view**, two relationships (one-to-many, single direction, active):
* `gold_patient_current_status[patient_id]` → `gold_patient_vitals_scored[patient_id]`
* `gold_patient_current_status[patient_id]` → `gold_daily_patient_kpi[patient_id]`

`current_status` has one row per patient, so it acts as the patient dimension. Delete any other automatic relationship (for example between `scored` and `daily_kpi`). `gold_critical_status_summary` has no relationships.

## 4. Measures

1. **Home → Enter data**, name the table `_Measures`, **Load**; hide its empty column (right-click → Hide in report view).
2. Select `_Measures` and create each measure with **New measure** (or paste them all in **DAX query view**). The full list is in [`powerbi_measures.dax`](powerbi_measures.dax), keep that file as a backup.
3. Set **% Critical** to the Percentage format, and the count measures to a whole number with a thousands separator.

## 5. Report pages

Use the same slicer, card and table styling on all pages (set one visual up, then Format painter). Rename the fields in each visual to readable labels, and turn off Sum on numeric columns in tables (**Don't summarize**).

**Page 1 — Ward overview** (nurse and doctor: who needs attention now)
* Cards: Monitored Patients, Critical Patients, Warning Patients, Silent Devices, % Critical, Data As Of.
* Donut: patients by `patient_status` (colors: Critical `#d64545`, Warning `#f0a30a`, Stable `#2f9e44`, Unknown gray).
* Table from `current_status`: patient, status, device, HR, SpO₂, score, temperature, minutes since last reading; sorted by score descending; status cell colored by the `Status Color` measure (Cells → Background color → fx → Field value).
* Slicer on `patient_status`.

**Page 2 — Patient trends** (one patient over time)
* Slicer on `patient_id` from `gold_patient_current_status`.
* Cards: Selected Status, Selected Score, Selected HR, Selected SpO2.
* Four line charts from `gold_patient_vitals_scored`: X axis `event_ts` (Continuous), Y axis `heart_rate`, `spo2`, `temperature_c` and `total_score` (Average). On the SpO₂ chart add a constant line at 92 (Analytics pane).
* Table of the latest readings (`event_ts`, HR, SpO₂, temperature, score, status), newest first, with a **Top N** filter of 10 on `event_ts` by the `Latest Reading Time` measure; status colored by `Row Status Color`.

**Page 3 — Daily KPIs** (ward manager)
* Cards: Total Readings (Display units: None), Out Of Range Readings, Readings With Missing Vitals.
* Stacked column: `event_date` against `critical_readings`, `warning_readings` and `stable_readings` (Categorical axis).
* Matrix from `gold_critical_status_summary`: rows `event_date`, columns `patient_status`, values `patients_count`. A patient can appear under several statuses on the same day, so the columns do not add up to the number of patients.
* Table of the top 10 patients by `critical_readings` (Top N filter on `patient_id`), from `gold_daily_patient_kpi`.

Add on every page the note: *Simplified early warning score (adapted from NEWS2) — decision support only, not a diagnosis.* Times are in UTC.

## 6. Refresh

After each pipeline run (the Gold task rewrites the CSVs), click **Home → Refresh**. Refresh once the DAG run is green; refreshing while the Gold task is writing can fail. Scheduled refresh in the Power BI Service would need a gateway to reach local files; for the capstone, manual refresh is enough.
