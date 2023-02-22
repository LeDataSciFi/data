# Wondering what the variables are? 

For the files with "ccm" in the name, variable descriptions are in `ccm_variable_descriptions.csv`. This covers _most_ of the variables in the datasets.

The patent variables in `two_pat_vars.csv`:
- Patent stock: $stock_t = (1-d)*stock_{t-1} + flow_t$ where $flow_t$ is the number of patents the firm received that year and $d_t$ is 15%.
- `frac_PatsThatCiteSelf`: The fraction of a firm's patents in a given year that cited its own previous patents.

The patent variables in `firmyear_patstats.csv`:
- patent_app_count: How many patents the firm applied for in that year
- RETech_avg: The average RETech of the patents the firm applied for in that year
- [RETech is described here](https://bowen.finance/bfh_data/)

