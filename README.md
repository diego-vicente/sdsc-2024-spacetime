# _Where & When_: A Space-Time Analysis of Vehicle Collisions in London

> Join us to explore a comprehensive use case, analyzing data with CARTO's Analytic Toolbox function through both the Workflows interface and the SQL console. We'll delve into space-time functions, working with geolocated time series data, and extracting insights through methods like space-time Getis-Ord, hotspot analysis, and time series clustering.

This repository contains all the material used in the workshop, organized in different formats for ease of use. Here you can find:
- A [complete transcript](./transcript.md) with supporting material like code, maps or images;
- A folder containing [all of the SQL queries](./sql) used in the workshop and the maps;
- Links to the other relevant materials like [the slides]() used.

## Notes on the code

All the code uses the Google BigQuery project ``sdsc-london-2024.spacetime`` to store the tables and preview functions. There are some caveats for this:
- It is a public project, so every table mentioned in the code is accesible for any user using the BigQuery console.
- It is read-only, so every time the code attempts to write a table in it, will fail. Please edit the code with a project and dataset that you are allowed to write to.
- All of those tables do exist, so you can explore the output of each query without the need of actually running it.
