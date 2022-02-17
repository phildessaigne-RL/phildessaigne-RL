#CCH Corporate Actions Integration
Developed for SCP by Matt Coleman and Alex Firestone - Ridgeline Professional Services

Last update: Jan 28, 2022

**Description:**
- CCH Corporate Actions Integration extracts CUSIP information from active holdings in the Ridgeline platform and formats the data into a CSV file with a single column of CUSIPs. Active holdings include both supervised and unsupervised portfolio sleeves.

**Program Outline:**
- Fetch All Watchlists & All Securities CUSIP/InstrumentID from tenant
- For Watchlist specified in yaml file - extract all instruments in watchlist by instrumentID
- Search for CUSIP by instrumentID for all instruments in watchlist
- Clean up and export to CSV file

**ToDo:**


Requires:
* sz_config.yaml with the below fields
```yaml
SeizertCchOut:
    watch_list_definition: "All Active Holdings"
    override_run_date: "2021-10-15"
    output_file_name: "output/MMDD_CCH_Out.csv"
```
- override_run_date: Defaults to yesterdays date if left blank (empty string)
- output_file_name: Required
 
 **Ridgeline APIs Used:**
 ```
LaunchHoldingsReport
FetchExportDataSetResponse
FetchPortfolioSleevesV2
```
 ****