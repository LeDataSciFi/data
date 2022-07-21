# Wondering what the variables are? 

Variable descriptions are in `ccm_variable_descriptions.csv`. This covers _most_ of the variables in the datasets.

The patent variables:
- Patent stock: $stock_t = (1-d)*stock_{t-1} + flow_t$ where $flow_t$ is the number of patents the firm received that year and $d_t$ is 15%.
- `frac_PatsThatCiteSelf`: The fraction of a firm's patents in a given year that cited its own previous patents.
