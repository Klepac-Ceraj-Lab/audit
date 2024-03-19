# VKC Sequencing Audit

- Requires [VKCJuliaRegistry](https://github.com/Klepac-Ceraj-Lab/VKCJuliaRegistry) to resolve packages
- On first setup on new machine / user, set local preferences:
    - If on a lab machine, run `VKCComputing.set_default_preferences!()`
        - otherwise, set `mgx_analysis_dir` and `mgx_raw_dir`
    - run `VKCComputing.set_airtable_dir!()` to set location of local airtable db
    - run `VKCComputing.set_readwrite_pat!()` to add a personal access token for airtable database.

