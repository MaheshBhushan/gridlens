# Power BI report specification

This is a build sheet for the GridLens Power BI report. No `.pbix` is shipped: the report
has not been built, because Power BI Desktop (Windows) and a Databricks workspace were not
available when this repository was written. Everything below refers to objects that the
`gridlens-pipeline` job creates (see `databricks.yml` and `src/gridlens/sql/views.sql`).

## 1. Data source

| Setting | Value |
|---|---|
| Connector | Get Data > Azure Databricks (or "Databricks" connector) |
| Server hostname | SQL warehouse > Connection details > Server hostname |
| HTTP path | SQL warehouse > Connection details > HTTP path |
| Authentication | Personal access token (Free Edition) or Microsoft Entra ID |
| Catalog / schema | `workspace` / `gridlens` (bundle variables `catalog`, `schema`) |
| Storage mode | Import. The gold views are small (tens of thousands of rows); refresh after each job run. DirectQuery also works because every visual reads a single view. |

Tables to load (all views):

| Power BI table | Source view | Grain |
|---|---|---|
| `LoadProfile` | `v_load_profile` | tariff x season x day_type x half-hour slot |
| `PeakDemand` | `v_peak_demand` | one row per complete day (48 intervals) |
| `Forecast` | `v_consumption_forecast` | one row per half hour (train, test, future) |
| `ForecastAccuracy` | `v_forecast_accuracy` | model x split |
| `DataQuality` | `v_data_quality` | one row per data-quality rule plus totals |

## 2. Model

Add a calculated date table and relate it to the day-grained views. The views are
otherwise independent; `LoadProfile` and `ForecastAccuracy` stand alone.

```DAX
Date =
ADDCOLUMNS (
    CALENDAR ( MIN ( PeakDemand[reading_date] ), MAX ( Forecast[forecast_date] ) ),
    "Year", YEAR ( [Date] ),
    "Month", FORMAT ( [Date], "yyyy-mm" ),
    "Weekday", FORMAT ( [Date], "ddd" ),
    "Is Weekend", WEEKDAY ( [Date], 2 ) > 5
)
```

Relationships (single direction, Date filters facts):

- `Date[Date]` 1 -> * `PeakDemand[reading_date]`
- `Date[Date]` 1 -> * `Forecast[forecast_date]`

Mark `Date` as the date table. Set `LoadProfile[slot_label]` to sort by `LoadProfile[slot]`.

## 3. Measures

```DAX
-- Peak demand
Peak kW per Household = MAX ( PeakDemand[peak_kw_per_household] )
Avg Daily kWh per Household = AVERAGE ( PeakDemand[kwh_per_household] )
Avg Load Factor = AVERAGE ( PeakDemand[load_factor] )
Peak Aggregate kW = MAX ( PeakDemand[peak_aggregate_kw] )
Days in Selection = COUNTROWS ( PeakDemand )

-- Load profile
Mean kW per Household = AVERAGE ( LoadProfile[mean_kw_per_household] )
P90 kWh per Half Hour = AVERAGE ( LoadProfile[p90_kwh] )
Evening Peak Ratio =
DIVIDE (
    CALCULATE ( [Mean kW per Household], LoadProfile[slot] >= 34 && LoadProfile[slot] <= 41 ),
    [Mean kW per Household]
)

-- Forecast
Actual kWh = SUM ( Forecast[actual_kwh] )
Forecast kWh = SUM ( Forecast[predicted_kwh] )
Baseline kWh = SUM ( Forecast[baseline_kwh] )
Forecast Abs Error = SUM ( Forecast[abs_error_kwh] )
Forecast WAPE % =
DIVIDE (
    CALCULATE ( [Forecast Abs Error], Forecast[split] = "test" ),
    CALCULATE ( [Actual kWh], Forecast[split] = "test" )
) * 100
Next 7 Days Forecast kWh per Household =
CALCULATE ( [Forecast kWh], Forecast[split] = "future" )

-- Accuracy table (already aggregated by the pipeline)
Held-out MAE =
CALCULATE ( MAX ( ForecastAccuracy[mae] ), ForecastAccuracy[split] = "test" )
Held-out WAPE % =
CALCULATE ( MAX ( ForecastAccuracy[wape_pct] ), ForecastAccuracy[split] = "test" )

-- Data quality
Rows Checked = CALCULATE ( SUM ( DataQuality[rows] ), DataQuality[rule] = "TOTAL_INPUT" )
Rows Passed = CALCULATE ( SUM ( DataQuality[rows] ), DataQuality[rule] = "TOTAL_PASSED" )
Rows Rejected = [Rows Checked] - [Rows Passed]
Reject Rate % = DIVIDE ( [Rows Rejected], [Rows Checked] ) * 100
```

`Forecast WAPE %` recomputed in DAX should equal `wape_pct` for the model on the test
split in `ForecastAccuracy`; use that as a refresh sanity check.

## 4. Pages

**Page 1 - Load profiles**
- Slicers: `LoadProfile[tariff]`, `LoadProfile[season]`, `LoadProfile[day_type]`.
- Line chart: X `slot_label`, Y `Mean kW per Household`, legend `season`.
- Line chart: X `slot_label`, Y `mean_kwh` and `p90_kwh` (spread of household demand).
- Card: `Evening Peak Ratio` (16:00-21:00 vs whole-day mean).

**Page 2 - Peak demand**
- Slicer: `Date[Month]`.
- Column chart: X `Date[Date]`, Y `Peak kW per Household`; line overlay `Avg Load Factor`.
- Table: top 10 days by `peak_rank_overall` with `peak_slot_label`, `households`,
  `peak_kw_per_household`, `load_factor`.
- Matrix: rows `Date[Month]`, columns `Date[Weekday]`, values `Peak kW per Household`
  (heatmap via conditional formatting).
- Cards: `Peak kW per Household`, `Avg Daily kWh per Household`, `Days in Selection`.

**Page 3 - Forecast**
- Line chart: X `Forecast[ts]`, Y `Actual kWh`, `Forecast kWh`, `Baseline kWh`; filter to
  the last 35 days so the held-out 28 days and the 7 future days are visible.
- Table: `ForecastAccuracy` (model, split, n, mae, rmse, wape_pct).
- Cards: `Held-out MAE`, `Held-out WAPE %`, `Next 7 Days Forecast kWh per Household`.

**Page 4 - Data quality**
- Bar chart: `DataQuality[rule]` (excluding the two TOTAL rows) by `rows`.
- Cards: `Rows Checked`, `Rows Passed`, `Reject Rate %`.

## 5. Refresh

Schedule the `gridlens-pipeline` job, then a dataset refresh after it in the Power BI
service (gateway not required for Databricks cloud sources).
